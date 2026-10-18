package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var _ port.InstanceService = (*InstanceService)(nil)

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
	Queues      port.ActiveTaskQueueRepository
	Gate        port.TenantGate
	Validator   port.EventValidator
	Cache       port.CacheStore
	Log         port.Logger
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
	return s.readCompiledWorkflow(ctx, tenantID, versionID)
}

func (s *InstanceService) readCompiledWorkflow(ctx context.Context, tenantID, versionID uuid.UUID) (*port.CompiledWorkflow, error) {
	compiled, err := s.Definitions.GetCompiledWorkflow(ctx, tenantID, versionID)
	if err != nil {
		s.logger().Warn("compiled-plan read from Definitions failed", map[string]any{"error": err.Error()})
		return nil, err
	}
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
	if s.Gate != nil {
		if err := s.Gate.Allow(ctx, in.TenantID); err != nil {
			return nil, err
		}
	}
	if existing, err := s.matchingStart(ctx, in); err != nil {
		return nil, err
	} else if existing != nil {
		return s.resumeStart(ctx, existing)
	}
	compiled, err := s.readCompiledWorkflow(ctx, in.TenantID, in.WorkflowVersionID)
	if err != nil {
		return nil, err
	}
	if compiled.Status != "PUBLISHED" {
		return nil, port.ErrVersionNotPublished
	}
	if !compiled.IsValid {
		return nil, port.ErrVersionInvalid
	}

	collab, err := decodeCollaboration(compiled.CompiledPlanJSON)
	if err != nil {
		return nil, err
	}
	mainPlan := findPlan(collab, collab.MainPlan)
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

	taskQueue, err := s.resolveTaskQueue(ctx, in.TenantID, mainPlan.TaskQueue)
	if err != nil {
		return nil, err
	}

	instanceID := uuid.New()
	temporalWorkflowID := in.TenantID.String() + ":" + in.SubjectID + ":" + in.BusinessKey
	inst := &domain.Instance{
		ID:                 instanceID,
		TenantID:           in.TenantID,
		WorkflowID:         compiled.WorkflowID,
		WorkflowVersionID:  in.WorkflowVersionID,
		SubjectID:          in.SubjectID,
		BusinessKey:        in.BusinessKey,
		TemporalWorkflowID: temporalWorkflowID,
		Status:             domain.InstanceStatusRunning,
		CurrentNodeKeys:    []string{},
		ContextJSON:        in.ContextJSON,
		OverrideMap:        overrideMapJSON,
		TaskQueue:          taskQueue,
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
		if errors.Is(txErr, port.ErrDuplicateBusinessKey) {
			existing, err := s.matchingStart(ctx, in)
			if err != nil {
				return nil, err
			}
			if existing == nil {
				return nil, port.ErrDuplicateBusinessKey
			}
			return s.resumeStart(ctx, existing)
		}
		return nil, txErr
	}

	return s.launchWorkflow(ctx, inst)
}

func (s *InstanceService) matchingStart(ctx context.Context, in port.StartInstanceInput) (*domain.Instance, error) {
	existing, err := s.Instances.GetLiveByBusinessKey(ctx, in.TenantID, in.SubjectID, in.BusinessKey)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, nil
		}
		return nil, wrapInstanceErr(err)
	}
	if existing.Status != domain.InstanceStatusRunning ||
		existing.WorkflowVersionID != in.WorkflowVersionID ||
		existing.StartedByUserID != in.StartedByUserID ||
		!sameContext(existing.ContextJSON, in.ContextJSON) ||
		!sameOverrideMap(existing.OverrideMap, in.OverrideMap) {
		return nil, nil
	}
	return existing, nil
}

func (s *InstanceService) resumeStart(ctx context.Context, existing *domain.Instance) (*port.Instance, error) {
	if existing.TemporalRunID != "" {
		return toPortInstance(existing), nil
	}
	return s.launchWorkflow(ctx, existing)
}

func sameContext(stored, requested []byte) bool {
	a, okA := decodeContext(stored)
	b, okB := decodeContext(requested)
	return okA && okB && reflect.DeepEqual(a, b)
}

func decodeContext(raw []byte) (any, bool) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return map[string]any{}, true
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, false
	}
	if v == nil {
		return map[string]any{}, true
	}
	return v, true
}

func sameOverrideMap(stored []byte, requested map[string]uuid.UUID) bool {
	var have map[string]string
	if len(stored) > 0 {
		if err := json.Unmarshal(stored, &have); err != nil {
			return false
		}
	}
	want := overrideMapToStrings(requested)
	if len(have) != len(want) {
		return false
	}
	for k, v := range want {
		if have[k] != v {
			return false
		}
	}
	return true
}

func (s *InstanceService) launchWorkflow(ctx context.Context, inst *domain.Instance) (*port.Instance, error) {
	var overrides map[string]string
	if len(inst.OverrideMap) > 0 {
		if err := json.Unmarshal(inst.OverrideMap, &overrides); err != nil {
			return nil, fmt.Errorf("decode override_map: %w", err)
		}
	}
	out, err := s.startWorkflowWithRetry(ctx, port.StartWorkflowInput{
		TemporalWorkflowID: inst.TemporalWorkflowID,
		TaskQueue:          inst.TaskQueue,
		TenantID:           inst.TenantID,
		InstanceID:         inst.ID,
		WorkflowVersionID:  inst.WorkflowVersionID,
		ContextJSON:        string(inst.ContextJSON),
		OverrideMap:        overrides,
	})
	if err != nil {
		s.logger().Error("StartWorkflow failed after workflow_instance was committed", map[string]any{
			"instance_id": inst.ID, "tenant_id": inst.TenantID, "business_key": inst.BusinessKey, "error": err.Error(),
		})
		return nil, fmt.Errorf("start workflow: %w", err)
	}
	if out.TemporalRunID != "" {
		inst.TemporalRunID = out.TemporalRunID
		if err := s.Instances.RecordRunID(ctx, inst.TenantID, inst.ID, out.TemporalRunID); err != nil {
			s.logger().Warn("could not record the run id of a workflow that started", map[string]any{
				"instance_id": inst.ID, "tenant_id": inst.TenantID, "error": err.Error(),
			})
		}
	}
	return s.afterWorkflowStarted(ctx, inst.TenantID, inst), nil
}

func (s *InstanceService) afterWorkflowStarted(ctx context.Context, tenantID uuid.UUID, inst *domain.Instance) *port.Instance {
	current, err := s.Instances.GetByID(ctx, tenantID, inst.ID)
	if err != nil {
		s.logger().Warn("could not re-read an instance whose workflow just started", map[string]any{
			"instance_id": inst.ID, "tenant_id": tenantID, "error": err.Error(),
		})
		return toPortInstance(inst)
	}
	if current.Status == domain.InstanceStatusTerminated {
		if err := s.Temporal.TerminateWorkflow(ctx, inst.TemporalWorkflowID, inst.TemporalRunID, "terminated while starting"); err != nil {
			s.logger().Error("TerminateWorkflow failed for an instance terminated while its workflow was starting", map[string]any{
				"instance_id": inst.ID, "tenant_id": tenantID, "error": err.Error(),
			})
		}
	}
	return toPortInstance(current)
}

const startWorkflowMaxAttempts = 3

func (s *InstanceService) startWorkflowWithRetry(ctx context.Context, in port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
	var lastErr error
	for attempt := 1; attempt <= startWorkflowMaxAttempts; attempt++ {
		out, err := s.Temporal.StartWorkflow(ctx, in)
		if err == nil {
			return out, nil
		}
		lastErr = err
		if attempt < startWorkflowMaxAttempts {
			select {
			case <-ctx.Done():
				return port.StartWorkflowOutput{}, fmt.Errorf("start workflow retry: %w", ctx.Err())
			case <-time.After(time.Duration(attempt) * 100 * time.Millisecond):
			}
		}
	}
	return port.StartWorkflowOutput{}, lastErr
}

func (s *InstanceService) resolveTaskQueue(ctx context.Context, tenantID uuid.UUID, planQueue string) (string, error) {
	if s.Queues == nil {
		return "", errors.New("the tenant task queue registry is not configured")
	}
	name := domain.TenantTaskQueueName(tenantID)
	queue, err := s.Queues.GetByQueueName(ctx, name)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return planQueue, nil
		}
		return "", fmt.Errorf("look up the tenant task queue: %w", err)
	}
	if !queue.Routable {
		return planQueue, nil
	}
	return name, nil
}
