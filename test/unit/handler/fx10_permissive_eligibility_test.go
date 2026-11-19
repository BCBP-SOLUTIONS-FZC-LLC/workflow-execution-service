package handler_test

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

type fx10AllowAll struct{ port.EligibilityChecker }

func (fx10AllowAll) CheckEligibility(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string) (bool, error) {
	return true, nil
}

type fx10PlanOnly struct{ plan string }

func (d fx10PlanOnly) GetCompiledWorkflow(_ context.Context, _, versionID uuid.UUID) (*port.CompiledWorkflow, error) {
	return &port.CompiledWorkflow{VersionID: versionID, VersionNumber: 1, Status: "PUBLISHED", IsValid: true, CompiledPlanJSON: d.plan}, nil
}

// fx10AllowEligibility wires a task service so every user is eligible for the stage of "finance/review" in the
// given department, the node the task fakes of these tests sit at.
func fx10AllowEligibility(svc *service.TaskService, deptID uuid.UUID) {
	raw, err := json.Marshal(dsl.CompiledCollaboration{
		SchemaVersion: dsl.CurrentSchemaVersion, MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main", TaskQueue: "wf-queue-default",
			Departments: []dsl.DepartmentDef{{
				ID: "finance", IAMDepartmentID: deptID.String(),
				Stages: []dsl.StageDef{{Type: "userTask", NodeID: "review", Role: "approver"}},
			}},
		}},
	})
	if err != nil {
		panic(err)
	}
	svc.Eligibility = fx10AllowAll{}
	svc.Definitions = fx10PlanOnly{plan: string(raw)}
}
