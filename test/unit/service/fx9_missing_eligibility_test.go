package service_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

// fx9AllowEligibility wires a service so every user is eligible for the stage of "finance/review" in the
// given department, the node the task fakes of these tests sit at.
func fx9AllowEligibility(t *testing.T, svc *service.TaskService, deptID uuid.UUID) {
	t.Helper()
	plan := compiledPlanJSONWithDept(t, dsl.DepartmentDef{
		ID: "finance", IAMDepartmentID: deptID.String(), Stages: []dsl.StageDef{{Type: "userTask", NodeID: "review", Role: "approver"}},
	})
	svc.Eligibility = &fakeEligibilityChecker{}
	svc.Definitions = &fakeDefinitionClient{resp: publishedCompiledWorkflow(uuid.New(), uuid.New(), plan)}
}

// fx9Wiring says which of the two collaborators the eligibility check needs a service is built without.
type fx9Wiring struct {
	name            string
	withEligibility bool
	withDefinitions bool
}

func fx9IncompleteWirings() []fx9Wiring {
	return []fx9Wiring{
		{name: "no eligibility checker", withDefinitions: true},
		{name: "no definitions client", withEligibility: true},
		{name: "neither"},
	}
}

func TestTaskService_Reassign_BuiltWithoutWhatTheEligibilityCheckNeeds_RefusesAndSendsNoSignal(t *testing.T) {
	for _, w := range fx9IncompleteWirings() {
		t.Run(w.name, func(t *testing.T) {
			h := csdNewTaskHarness(t, "single", csdReviewStage())
			if !w.withEligibility {
				h.svc.Eligibility = nil
			}
			if !w.withDefinitions {
				h.svc.Definitions = nil
			}

			got, err := h.svc.Reassign(context.Background(), h.tenantID, h.task.ID, uuid.New(), uuid.New(), csdTaskVersion)

			require.Error(t, err)
			assert.Nil(t, got)
			assert.Empty(t, h.temporal.signals, "an unchecked user must not be given the task")
		})
	}
}

func TestTaskService_OverrideAssignee_BuiltWithoutWhatTheEligibilityCheckNeeds_RefusesAndStoresNothing(t *testing.T) {
	for _, w := range fx9IncompleteWirings() {
		t.Run(w.name, func(t *testing.T) {
			svc, instances, tasks, assignments, overrides, _ := newTaskServiceHarness()
			outbox := &fakeOutbox{}
			svc.Signals = outbox
			if w.withEligibility {
				svc.Eligibility = &csdEligibility{}
			}
			tenantID, instanceID := uuid.New(), uuid.New()
			instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, TemporalWorkflowID: "tenant:biz", Status: domain.InstanceStatusRunning}
			task := newReadyTask(tenantID, instanceID)
			task.RecordVersion = 4
			tasks.byID[task.ID] = task
			assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: uuid.New(), IsActive: true}

			got, err := svc.OverrideAssignee(context.Background(), port.AssigneeOverrideInput{
				TenantID: tenantID, InstanceID: instanceID, NodeKey: task.NodeKey,
				NewUserID: uuid.New(), Reason: "on leave", ActorUserID: uuid.New(), RecordVersion: 4,
			})

			require.Error(t, err)
			assert.Nil(t, got)
			assert.Empty(t, overrides.created, "no audit row for an unchecked override")
			assert.Empty(t, outbox.signals, "no reassign signal for an unchecked override")
			assert.EqualValues(t, 4, task.RecordVersion, "the task version does not move")
		})
	}
}
