package service

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
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
	default:
		if scope != scopeTender {
			if observability.DelegationScopeUnhandledTotal != nil {
				observability.DelegationScopeUnhandledTotal.WithLabelValues(scope).Inc()
			}
			log.Warn("scope match: delegation scope is unknown to this service, matching on subject", map[string]any{
				"scope": scope, "task_id": c.task.ID,
			})
		}
		return scopeID != nil && c.instance.SubjectID == *scopeID
	}
}

type compiledPlanCache struct {
	definitions port.DefinitionServiceClient
	log         port.Logger
	byVersion   map[uuid.UUID]*dsl.CompiledPlan
}

func newCompiledPlanCache(definitions port.DefinitionServiceClient, log port.Logger) *compiledPlanCache {
	return &compiledPlanCache{definitions: definitions, log: log, byVersion: make(map[uuid.UUID]*dsl.CompiledPlan)}
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
	compiled, err := c.definitions.GetCompiledWorkflow(ctx, tenantID, versionID)
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
	expanded, err := dsl.ExpandCalls(&collab, 0)
	if err != nil {
		return nil, fmt.Errorf("%w: expand compiled plan: %w", port.ErrVersionInvalid, err)
	}
	return expanded, nil
}
