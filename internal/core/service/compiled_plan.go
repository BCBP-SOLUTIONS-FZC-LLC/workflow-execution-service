package service

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

const scopeTender = "tender"

func scopeMatches(ctx context.Context, log port.Logger, plans *compiledPlanCache, tenantID uuid.UUID, c delegationCandidate, scope string, scopeID *string) bool {
	switch scope {
	case "all":
		return true
	case "department":
		if scopeID == nil {
			return false
		}
		plan, err := plans.mainPlan(ctx, tenantID, c.instance.WorkflowVersionID)
		if err != nil {
			log.Warn("scope match: skipping candidate with unreadable compiled plan", map[string]any{"task_id": c.task.ID})
			return false
		}
		taskDeptID, _ := deptAndSuffix(c.task.NodeKey)
		for _, dept := range plan.Departments {
			if dept.ID != taskDeptID {
				continue
			}
			return dept.IAMDepartmentID == *scopeID && deptUUID(&dept) == c.task.DepartmentID
		}
		return false
	case scopeTender:
		return scopeID != nil && c.instance.SubjectID == *scopeID
	default:
		if observability.DelegationScopeUnhandledTotal != nil {
			observability.DelegationScopeUnhandledTotal.WithLabelValues(scope).Inc()
		}
		log.Warn("scope match: delegation scope is unknown to this service, matching nothing", map[string]any{
			"scope": scope, "task_id": c.task.ID,
		})
		return false
	}
}

type compiledPlanCache struct {
	definitions port.DefinitionServiceClient
	store       port.CacheStore
	log         port.Logger
	byVersion   map[uuid.UUID]*dsl.CompiledPlan
}

func newCompiledPlanCache(definitions port.DefinitionServiceClient, log port.Logger) *compiledPlanCache {
	return &compiledPlanCache{definitions: definitions, log: log, byVersion: make(map[uuid.UUID]*dsl.CompiledPlan)}
}

func (c *compiledPlanCache) backedBy(store port.CacheStore) *compiledPlanCache {
	c.store = store
	return c
}

func cachedCompiledWorkflow(
	ctx context.Context, store port.CacheStore, definitions port.DefinitionServiceClient, log port.Logger, tenantID, versionID uuid.UUID,
) (*port.CompiledWorkflow, error) {
	if store != nil {
		raw, err := store.Get(ctx, compiledPlanCacheKey(tenantID, versionID))
		if err != nil {
			log.Warn("compiled-plan cache read failed, falling back to Definitions", map[string]any{"error": err.Error()})
		} else if raw != "" {
			var compiled port.CompiledWorkflow
			if err := json.Unmarshal([]byte(raw), &compiled); err == nil {
				return &compiled, nil
			}
			log.Warn("compiled-plan cache hit failed to unmarshal, falling back to Definitions", nil)
		}
	}
	return liveCompiledWorkflow(ctx, store, definitions, log, tenantID, versionID)
}

func liveCompiledWorkflow(
	ctx context.Context, store port.CacheStore, definitions port.DefinitionServiceClient, log port.Logger, tenantID, versionID uuid.UUID,
) (*port.CompiledWorkflow, error) {
	compiled, err := definitions.GetCompiledWorkflow(ctx, tenantID, versionID)
	if err != nil {
		return nil, err
	}
	if store != nil {
		if raw, err := json.Marshal(compiled); err != nil {
			log.Warn("compiled-plan marshal for cache write-through failed", map[string]any{"error": err.Error()})
		} else if err := store.Set(ctx, compiledPlanCacheKey(tenantID, versionID), string(raw), compiledPlanCacheTTL); err != nil {
			log.Warn("compiled-plan cache write-through failed", map[string]any{"error": err.Error()})
		}
	}
	return compiled, nil
}

func (c *compiledPlanCache) mainPlan(ctx context.Context, tenantID, versionID uuid.UUID) (*dsl.CompiledPlan, error) {
	plan, err := c.readMainPlan(ctx, tenantID, versionID)
	if err != nil {
		c.log.Warn("compiled plan unreadable", map[string]any{"version_id": versionID, "error": err.Error()})
	}
	return plan, err
}

func (c *compiledPlanCache) readMainPlan(ctx context.Context, tenantID, versionID uuid.UUID) (*dsl.CompiledPlan, error) {
	if plan, ok := c.byVersion[versionID]; ok {
		observability.IncCounter(observability.WFCacheHitsTotal)
		return plan, nil
	}
	observability.IncCounter(observability.WFCacheMissesTotal)
	compiled, err := cachedCompiledWorkflow(ctx, c.store, c.definitions, c.log, tenantID, versionID)
	if err != nil {
		return nil, err
	}
	collab, err := decodeCollaboration(compiled.CompiledPlanJSON)
	if err != nil {
		return nil, err
	}
	plan := findPlan(collab, collab.MainPlan)
	if plan == nil {
		return nil, fmt.Errorf("main plan %q not found in compiled collaboration", collab.MainPlan)
	}
	c.byVersion[versionID] = plan
	return plan, nil
}

func requiredLevelForTask(plan *dsl.CompiledPlan, task *domain.Task) (string, bool) {
	for _, dept := range plan.Departments {
		if deptUUID(&dept) != task.DepartmentID {
			continue
		}
		for i := range dept.Stages {
			stage := &dept.Stages[i]
			if dsl.NodeKey(dept.ID, stage) == task.NodeKey {
				return stage.Role, true
			}
		}
	}
	return "", false
}

func decodeCollaboration(planJSON string) (*dsl.CompiledCollaboration, error) {
	var collab dsl.CompiledCollaboration
	if err := json.Unmarshal([]byte(planJSON), &collab); err != nil {
		return nil, fmt.Errorf("unmarshal compiled plan: %w", err)
	}
	if collab.SchemaVersion != dsl.CurrentSchemaVersion {
		return &collab, nil
	}
	expanded, err := dsl.ExpandCalls(&collab, domain.MaxPlanStages)
	if err != nil {
		return nil, fmt.Errorf("%w: expand compiled plan: %w", port.ErrVersionInvalid, err)
	}
	return expanded, nil
}
