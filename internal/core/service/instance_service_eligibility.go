package service

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/enums"
)

func createsHumanTask(stage *dsl.StageDef) bool {
	if stage.ConnectorType != "" {
		return false
	}
	switch stage.Type {
	case string(enums.StageTypeSendTask), string(enums.StageTypeReceiveTask):
		return false
	default:
		return true
	}
}

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

type eligibilityCheck struct {
	nodeKey string
	req     port.EligibilityCheckRequest
}

// resolveStageAssignees returns the user IDs a stage would be assigned to. An
// override replaces the stage's defaults entirely. An unparseable default is
// dropped, so a stage whose every default is malformed resolves to nobody and
// the caller refuses the instantiation. That is deliberate: the task
// interpreter parses the same values when it creates the task and fails the
// activity there instead, which strands an instance mid-run rather than
// refusing it up front.
func resolveStageAssignees(stage *dsl.StageDef, nodeKey string, overrideMap map[string]uuid.UUID) []uuid.UUID {
	if override, ok := overrideMap[nodeKey]; ok {
		return []uuid.UUID{override}
	}
	userIDs := make([]uuid.UUID, 0, len(stage.DefaultAssignees))
	for _, raw := range stage.DefaultAssignees {
		id, err := uuid.Parse(raw)
		if err != nil {
			continue
		}
		userIDs = append(userIDs, id)
	}
	return userIDs
}

// collectAssigneeChecks walks the plan once, returning the eligibility checks to
// run and the node keys that resolved to nobody. A node naming no one it can
// resolve is ineligible without asking IAM: Execution would create a task with
// no assignment rows, which its claim path refuses, so the instance would never
// advance (LLD §5.5).
func collectAssigneeChecks(
	plan *dsl.CompiledPlan,
	overrideMap map[string]uuid.UUID,
) (checks []eligibilityCheck, unresolvedNodes []string) {
	for _, dept := range plan.Departments {
		if dept.Ignore {
			continue
		}
		for i := range dept.Stages {
			stage := &dept.Stages[i]
			if !createsHumanTask(stage) {
				continue
			}
			nodeKey := stageNodeKey(dept.ID, stage)
			userIDs := resolveStageAssignees(stage, nodeKey, overrideMap)
			if len(userIDs) == 0 {
				unresolvedNodes = append(unresolvedNodes, nodeKey)
				continue
			}
			for _, userID := range userIDs {
				checks = append(checks, eligibilityCheck{
					nodeKey: nodeKey,
					req: port.EligibilityCheckRequest{
						NewUserID: userID, DepartmentID: deptUUID(&dept), RequiredLevel: stage.Role,
					},
				})
			}
		}
	}
	return checks, unresolvedNodes
}

func (s *InstanceService) validateAssigneeEligibility(ctx context.Context, plan *dsl.CompiledPlan, overrideMap map[string]uuid.UUID) error {
	checks, ineligibleNodes := collectAssigneeChecks(plan, overrideMap)

	if len(checks) > 0 {
		requests := make([]port.EligibilityCheckRequest, len(checks))
		for i, c := range checks {
			requests[i] = c.req
		}
		results, err := s.Eligibility.CheckEligibilityBatch(ctx, requests, uuid.Nil)
		if err != nil {
			return err
		}
		for i, result := range results {
			if result.Err != nil || !result.Eligible {
				ineligibleNodes = append(ineligibleNodes, checks[i].nodeKey)
			}
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
