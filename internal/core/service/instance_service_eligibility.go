package service

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

func deptUUID(dept *dsl.DepartmentDef) uuid.UUID {
	id, _ := uuid.Parse(dept.IAMDepartmentID)
	return id
}

func stageNodeKey(deptID string, stage *dsl.StageDef) string {
	if stage.NodeID != "" {
		return deptID + "/" + stage.NodeID
	}
	return deptID + "/" + stage.Type
}

func findPlan(collab *dsl.CompiledCollaboration, name string) *dsl.CompiledPlan {
	for _, p := range collab.Plans {
		if p.Name == name {
			return p
		}
	}
	return nil
}

func validateOverrideMap(plan *dsl.CompiledPlan, overrideMap map[string]uuid.UUID) error {
	if len(overrideMap) == 0 {
		return nil
	}
	valid := make(map[string]struct{})
	for _, dept := range plan.Departments {
		for i := range dept.Stages {
			valid[stageNodeKey(dept.ID, &dept.Stages[i])] = struct{}{}
		}
	}
	for nodeKey := range overrideMap {
		if _, ok := valid[nodeKey]; !ok {
			return fmt.Errorf("%w: node key %q", port.ErrOverrideMapInvalid, nodeKey)
		}
	}
	return nil
}

func (s *InstanceService) validateAssigneeEligibility(ctx context.Context, plan *dsl.CompiledPlan, overrideMap map[string]uuid.UUID) error {
	type check struct {
		nodeKey string
		req     port.EligibilityCheckRequest
	}
	var checks []check

	for _, dept := range plan.Departments {
		if dept.Ignore {
			continue
		}
		for i := range dept.Stages {
			stage := &dept.Stages[i]
			if stage.ConnectorType != "" {
				continue
			}
			nodeKey := stageNodeKey(dept.ID, stage)

			var userIDs []uuid.UUID
			if override, ok := overrideMap[nodeKey]; ok {
				userIDs = []uuid.UUID{override}
			} else {
				for _, raw := range stage.DefaultAssignees {
					id, err := uuid.Parse(raw)
					if err != nil {
						continue // not this method's job to validate DSL well-formedness
					}
					userIDs = append(userIDs, id)
				}
			}

			for _, userID := range userIDs {
				checks = append(checks, check{
					nodeKey: nodeKey,
					req: port.EligibilityCheckRequest{
						NewUserID: userID, DepartmentID: deptUUID(&dept), RequiredLevel: stage.Role,
					},
				})
			}
		}
	}

	if len(checks) == 0 {
		return nil
	}

	requests := make([]port.EligibilityCheckRequest, len(checks))
	for i, c := range checks {
		requests[i] = c.req
	}
	results, err := s.Eligibility.CheckEligibilityBatch(ctx, requests, uuid.Nil)
	if err != nil {
		return err
	}

	var ineligibleNodes []string
	for i, eligible := range results {
		if !eligible {
			ineligibleNodes = append(ineligibleNodes, checks[i].nodeKey)
		}
	}
	if len(ineligibleNodes) > 0 {
		return &port.AssigneeIneligibleError{Nodes: ineligibleNodes}
	}
	return nil
}

func overrideMapToStrings(m map[string]uuid.UUID) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v.String()
	}
	return out
}
