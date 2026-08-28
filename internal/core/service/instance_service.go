package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

var _ port.InstanceService = (*InstanceService)(nil)

// compiledPlanCacheTTL is a provisional number (matching this LLD's own
// treatment of MAX_CLIENT_CONN/MAX_TENANT_QUEUES_PER_WORKER elsewhere) — a
// cache miss always falls through to a direct GetCompiledWorkflow call, so
// an over- or under-generous TTL only affects hit rate, never correctness.
const compiledPlanCacheTTL = time.Hour

func compiledPlanCacheKey(tenantID, versionID uuid.UUID) string {
	return "compiled_plan:" + tenantID.String() + ":" + versionID.String()
}

type InstanceService struct {
	Instances   port.InstanceRepository
	Tasks       port.TaskRepository
	Assignments port.TaskAssignmentRepository
	Outbox      port.OutboxRepository
	Transactor  port.Transactor
	Temporal    port.TemporalClient
	Definitions port.DefinitionServiceClient
	Eligibility port.EligibilityChecker
	Validator   port.EventValidator
	// Cache is nil-safe:
	// Start's compiled-plan read no-ops straight to Definitions when nil.
	Cache port.CacheStore
	Log   port.Logger
}

func (s *InstanceService) logger() port.Logger {
	if s.Log != nil {
		return s.Log
	}
	return noopLogger{}
}

func (s *InstanceService) fetchCompiledWorkflow(ctx context.Context, tenantID, versionID uuid.UUID) (*port.CompiledWorkflow, error) {
	if s.Cache != nil {
		raw, err := s.Cache.Get(ctx, compiledPlanCacheKey(tenantID, versionID))
		if err != nil {
			s.logger().Warn("compiled-plan cache read failed, falling back to Definitions", map[string]any{"error": err.Error()})
		} else if raw != "" {
			var compiled port.CompiledWorkflow
			if err := json.Unmarshal([]byte(raw), &compiled); err == nil {
				return &compiled, nil
			}
			s.logger().Warn("compiled-plan cache hit failed to unmarshal, falling back to Definitions", nil)
		}
	}

	compiled, err := s.Definitions.GetCompiledWorkflow(ctx, tenantID, versionID)
	if err != nil {
		return nil, err
	}
	// Write-through on a miss: with no other populator left in this codebase
	// (the former TemplateCachePrewarmer was the only one), skipping this
	// would leave the cache permanently cold — every Start call paying the
	// Definitions hop forever, not just the first one after a publish.
	if s.Cache != nil {
		if raw, err := json.Marshal(compiled); err != nil {
			s.logger().Warn("compiled-plan marshal for cache write-through failed", map[string]any{"error": err.Error()})
		} else if err := s.Cache.Set(ctx, compiledPlanCacheKey(tenantID, versionID), string(raw), compiledPlanCacheTTL); err != nil {
			s.logger().Warn("compiled-plan cache write-through failed", map[string]any{"error": err.Error()})
		}
	}
	return compiled, nil
}

func (s *InstanceService) Start(ctx context.Context, in port.StartInstanceInput) (*port.Instance, error) {
	compiled, err := s.fetchCompiledWorkflow(ctx, in.TenantID, in.WorkflowVersionID)
	if err != nil {
		return nil, err
	}
	if compiled.Status != "PUBLISHED" {
		return nil, port.ErrVersionNotPublished
	}
	if !compiled.IsValid {
		return nil, port.ErrVersionInvalid
	}

	var collab dsl.CompiledCollaboration
	if err := json.Unmarshal([]byte(compiled.CompiledPlanJSON), &collab); err != nil {
		return nil, fmt.Errorf("unmarshal compiled plan: %w", err)
	}
	mainPlan := findPlan(&collab, collab.MainPlan)
	if mainPlan == nil {
		return nil, fmt.Errorf("main plan %q not found in compiled collaboration", collab.MainPlan)
	}

	if err := validateOverrideMap(mainPlan, in.OverrideMap); err != nil {
		return nil, err
	}

	if err := s.validateAssigneeEligibility(ctx, mainPlan, in.OverrideMap); err != nil {
		return nil, err
	}

	overrideMapJSON, err := json.Marshal(overrideMapToStrings(in.OverrideMap))
	if err != nil {
		return nil, fmt.Errorf("marshal override_map: %w", err)
	}

	instanceID := uuid.New()
	temporalWorkflowID := in.TenantID.String() + ":" + in.BusinessKey
	inst := &domain.Instance{
		ID:                 instanceID,
		TenantID:           in.TenantID,
		WorkflowID:         compiled.WorkflowID,
		WorkflowVersionID:  in.WorkflowVersionID,
		BusinessKey:        in.BusinessKey,
		TemporalWorkflowID: temporalWorkflowID,
		Status:             domain.InstanceStatusRunning,
		CurrentNodeKeys:    []string{},
		ContextJSON:        in.ContextJSON,
		OverrideMap:        overrideMapJSON,
		TaskQueue:          mainPlan.TaskQueue,
		StartedByUserID:    in.StartedByUserID,
	}

	txErr := s.Transactor.RunInTx(withTenantGUC(ctx, in.TenantID), func(ctx context.Context) error {
		if err := s.Instances.Create(ctx, inst); err != nil {
			return wrapInstanceErr(err)
		}
		sink := instanceEventSink{Outbox: s.Outbox, Validator: s.Validator}
		return sink.enqueueInstanceEvent(ctx, in.TenantID.String(), domain.EventWorkflowInstanceStarted, inst.ID.String(),
			in.StartedByUserID.String(), domain.NewWorkflowInstanceStartedPayload(instanceCore(inst), in.StartedByUserID))
	})
	if txErr != nil {
		return nil, txErr
	}

	startErr := s.startWorkflowWithRetry(ctx, port.StartWorkflowInput{
		TemporalWorkflowID: temporalWorkflowID,
		TaskQueue:          mainPlan.TaskQueue,
		TenantID:           in.TenantID,
		InstanceID:         instanceID,
		WorkflowVersionID:  in.WorkflowVersionID,
		ContextJSON:        string(in.ContextJSON),
		OverrideMap:        overrideMapToStrings(in.OverrideMap),
	})
	if startErr != nil {
		s.logger().Error("StartWorkflow failed after workflow_instance was committed", map[string]any{
			"instance_id": instanceID, "tenant_id": in.TenantID, "business_key": in.BusinessKey, "error": startErr.Error(),
		})
		return nil, fmt.Errorf("start workflow: %w", startErr)
	}

	return toPortInstance(inst), nil
}

const startWorkflowMaxAttempts = 3

func (s *InstanceService) startWorkflowWithRetry(ctx context.Context, in port.StartWorkflowInput) error {
	var lastErr error
	for attempt := 1; attempt <= startWorkflowMaxAttempts; attempt++ {
		if _, err := s.Temporal.StartWorkflow(ctx, in); err != nil {
			lastErr = err
			if attempt < startWorkflowMaxAttempts {
				select {
				case <-ctx.Done():
					return fmt.Errorf("start workflow retry: %w", ctx.Err())
				case <-time.After(time.Duration(attempt) * 100 * time.Millisecond):
				}
			}
			continue
		}
		return nil
	}
	return lastErr
}
