package service_test

import (
	"encoding/json"
	"strings"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

// fx10IsPlanKey tells the cache key of a compiled plan, which the eligibility check writes next to the keys
// a cache fake of these tests watches.
func fx10IsPlanKey(key string) bool { return strings.HasPrefix(key, "compiled_plan:") }

// fx10ReviewNode is the node the task fakes of these tests sit at.
const fx10ReviewNode = "finance/review"

// fx10Collaborators returns an eligibility checker that approves every user and a definitions client serving
// a plan whose only stage is fx10ReviewNode in the given department.
func fx10Collaborators(deptID uuid.UUID) (port.EligibilityChecker, port.DefinitionServiceClient) {
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
	return &fakeEligibilityChecker{}, &fakeDefinitionClient{resp: publishedCompiledWorkflow(uuid.New(), uuid.New(), string(raw))}
}

// fx10AllowTaskEligibility wires a task service so every user is eligible for fx10ReviewNode in the department.
func fx10AllowTaskEligibility(svc *service.TaskService, deptID uuid.UUID) {
	svc.Eligibility, svc.Definitions = fx10Collaborators(deptID)
}

// fx10AllowClientEligibility wires a delegate client so every user is eligible for fx10ReviewNode in the department.
func fx10AllowClientEligibility(svc *service.WorkflowClient, deptID uuid.UUID) {
	svc.Eligibility, svc.Definitions = fx10Collaborators(deptID)
}
