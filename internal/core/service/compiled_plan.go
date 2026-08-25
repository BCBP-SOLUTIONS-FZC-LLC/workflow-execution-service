package service

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

func scopeMatches(ctx context.Context, plans *compiledPlanCache, tenantID uuid.UUID, c delegationCandidate, scope string, scopeID *string) bool {
	switch scope {
	case "all":
		return true
	case "department":
		if scopeID == nil {
			return false
		}
		plan, err := plans.mainPlan(ctx, tenantID, c.instance.WorkflowVersionID)
		if err != nil {
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
		return scopeID != nil && c.instance.BusinessKey == *scopeID
	}
}

type compiledPlanCache struct {
	definitions port.DefinitionServiceClient
	byVersion   map[uuid.UUID]*dsl.CompiledPlan
}

func newCompiledPlanCache(definitions port.DefinitionServiceClient) *compiledPlanCache {
	return &compiledPlanCache{definitions: definitions, byVersion: make(map[uuid.UUID]*dsl.CompiledPlan)}
}

func (c *compiledPlanCache) mainPlan(ctx context.Context, tenantID, versionID uuid.UUID) (*dsl.CompiledPlan, error) {
	if plan, ok := c.byVersion[versionID]; ok {
		return plan, nil
	}
	compiled, err := c.definitions.GetCompiledWorkflow(ctx, tenantID, versionID)
	if err != nil {
		return nil, err
	}
	var collab dsl.CompiledCollaboration
	if err := json.Unmarshal([]byte(compiled.CompiledPlanJSON), &collab); err != nil {
		return nil, fmt.Errorf("unmarshal compiled plan: %w", err)
	}
	plan := findPlan(&collab, collab.MainPlan)
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
			if stageNodeKey(dept.ID, stage) == task.NodeKey {
				return stage.Role, true
			}
		}
	}
	return "", false
}
