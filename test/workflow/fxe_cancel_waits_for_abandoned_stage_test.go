package workflow_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

func wflBoundaryIntoReceiveCollab() *dsl.CompiledCollaboration {
	return &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main",
			Departments: []dsl.DepartmentDef{
				{ID: "intake", Stages: wflStages("Intake")},
				{ID: "ops", Stages: []dsl.StageDef{
					{Type: "prep", NodeID: "Prepare", BoundaryTimer: &dsl.BoundaryTimer{Duration: "1h", Interrupting: true, TargetDept: "esc"}},
				}},
				{ID: "esc", Stages: []dsl.StageDef{{Type: "receive_task", NodeID: "Wait", Extras: map[string]string{"message": "never"}}}},
			},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"intake", "ops"}}}},
		}},
	}
}

// An interrupting boundary hands over to a department that waits for a message
// that never comes. A cancel ends the run anyway: the waiting path is not an
// abandoned stage the run has to wait for.
func TestExecute_CancelWhileABoundaryPathWaitsForAMessageEndsTheRun(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, wflBoundaryIntoReceiveCollab())
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("intake", "Intake", 1))
	wflCancelAt(env, time.Hour+time.Minute)

	status, err := wflStart(t, env, fakes, "")

	wflAssertCancelEndedTheInstance(t, fakes, status, err)
}

// A force-back while a boundary path waits for a message that never comes
// completes the route: the earlier department runs again.
func TestExecute_ForceBackWhileABoundaryPathWaitsForAMessageCompletesTheRoute(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, wflBoundaryIntoReceiveCollab())
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("intake", "Intake", 1))
	wflSignalAt(env, 2*time.Hour, "instance-force-back", adminSignalWire{AdminUserID: "admin-7", RecordVersion: 9})
	wflCancelAt(env, 3*time.Hour)

	status, err := wflStart(t, env, fakes, "")

	wflAssertCancelEndedTheInstance(t, fakes, status, err)
	audits := wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute)
	require.Len(t, audits, 1)
	assert.Equal(t, wflTenant, audits[0].TenantID)
	assert.Equal(t, domain.ForceRouteDirectionBack, audits[0].Direction)
	assert.Equal(t, "intake/Intake", audits[0].TargetNodeID)
	assert.Equal(t, []domain.NodeKey{"intake/Intake", "ops/Prepare", "intake/Intake"}, wflNodesCreated(fakes), "the earlier department ran again")
}

// A branch of a parallel step has an interrupting boundary whose supersede
// write is slow. An instance-cancel that lands while that write is in flight
// ends the run only after the write (repeated on a context that outlives the
// cancel) has run, so the branch's task is not left open.
func TestExecute_CancelInAParallelStepWaitsForABranchBoundarySupersede(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := wflForkOnly()
	plan := collab.Plans[0]
	plan.Departments[2].Stages = []dsl.StageDef{
		{Type: "prep", NodeID: "Charge", BoundaryTimer: &dsl.BoundaryTimer{Duration: "1h", Interrupting: true, TargetDept: "esc"}},
	}
	plan.Departments = append(plan.Departments, dsl.DepartmentDef{ID: "esc", Stages: wflStages("Escalate")})
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, collab)
	record := func(_ context.Context, in port.UpdateTaskStatusInput) error {
		return fakes.begin(port.ActivityUpdateTaskStatus, in)
	}
	env.OnActivity(port.ActivityUpdateTaskStatus, mock.Anything, mock.MatchedBy(func(in port.UpdateTaskStatusInput) bool {
		return in.Status == domain.TaskStatusSuperseded
	})).After(3 * time.Second).Return(record).Once()
	env.OnActivity(port.ActivityUpdateTaskStatus, mock.Anything, mock.Anything).Return(record)
	wflCancelAt(env, time.Hour+time.Second)

	status, err := wflStart(t, env, fakes, "")

	wflAssertCancelEndedTheInstance(t, fakes, status, err)
	var superseded int
	for _, u := range wflInputs[port.UpdateTaskStatusInput](fakes, port.ActivityUpdateTaskStatus) {
		if u.TaskID == "billing/Charge" && u.Status == domain.TaskStatusSuperseded {
			require.Equal(t, wflTenant, u.TenantID)
			superseded++
		}
	}
	assert.Equal(t, 1, superseded, "the branch task was superseded before the run ended")
}
