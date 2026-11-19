package workflow_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// w23aPlanReadFails is a fake set whose plan read is rejected for good;
// every other activity succeeds.
func w23aPlanReadFails() *wflFakes {
	fakes := wflNewFakes()
	fakes.failWith = func(name string, _ int) error {
		if name == port.ActivityGetCompiledPlan {
			return temporal.NewNonRetryableApplicationError("version is not published", "DefinitionServiceClientError", nil)
		}
		return nil
	}
	return fakes
}

// A plan read the Definition Service rejects fails the start: the instance is
// written FAILED once, for this tenant and instance and without a completion
// time, no task is created, and the workflow's error is the wrapped plan error.
func TestExecute_APlanReadRejectedAtStartWritesFailedAndReturnsTheWrappedError(t *testing.T) {
	for _, name := range []string{"the status write succeeds"} {
		t.Run(name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			fakes := w23aPlanReadFails()
			wflRegisterAll(env, fakes, wflOneTaskCollab())

			_, err := wflStart(t, env, fakes, "")

			require.Error(t, err)
			assert.Contains(t, err.Error(), "workflow: GetCompiledPlanActivity: ")
			assert.Contains(t, err.Error(), "version is not published")
			assert.Equal(t, 1, fakes.attemptsOf(port.ActivityGetCompiledPlan), "a rejection is read once")
			writes := wflInputs[port.UpdateInstanceStatusInput](fakes, port.ActivityUpdateInstanceStatus)
			require.Len(t, writes, 1)
			assert.Equal(t, port.UpdateInstanceStatusInput{
				InstanceID: wflInstance, TenantID: wflTenant, Status: domain.InstanceStatusFailed,
			}, writes[0], "FAILED, no completion time, no failed branches")
			assert.Nil(t, writes[0].CompletedAt)
			assert.Empty(t, wflNodesCreated(fakes), "no task is created")
		})
	}
}

// A gateway branch whose condition is outside the grammar fails the instance
// with the invalid-expression message; no branch is taken and the department it
// pointed at never starts.
func TestExecute_ExclusiveGatewayWithAnInvalidConditionFailsTheInstanceNamingTheExpression(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := wflGatewayAfterA(
		dsl.ExclusiveBranch{ConditionExpression: `amount > 1`, Target: "B"},
		dsl.ExclusiveBranch{Target: "B"},
	)
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, collab)
	wflSignalAt(env, 5*time.Second, "stage-transition", wflDoneAWith(`{"amount":2}`))

	_, err := wflStart(t, env, fakes, "")

	require.Error(t, err)
	assert.Contains(t, err.Error(), `workflow: dsl: invalid condition expression "amount > 1"`)
	assert.Equal(t, []domain.NodeKey{"A/StepA"}, wflNodesCreated(fakes), "the else branch was not taken either")
	assert.Equal(t, []domain.InstanceStatus{domain.InstanceStatusFailed}, wflStatuses(fakes))
	failed := wflInputs[port.UpdateInstanceStatusInput](fakes, port.ActivityUpdateInstanceStatus)
	assert.Equal(t, wflTenant, failed[0].TenantID)
	assert.Equal(t, wflInstance, failed[0].InstanceID)
}

func w23aParallelCollab() *dsl.CompiledCollaboration {
	return &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main",
			Departments: []dsl.DepartmentDef{
				{ID: "a", Stages: wflStages("A1")},
				{ID: "b", Stages: wflStages("B1")},
				{ID: "tail", Stages: wflStages("Tail")},
			},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{
				{Parallel: []dsl.ParallelBranch{
					{DeptID: "a", Steps: []dsl.ExecutionStep{{Sequential: []string{"a"}}}},
					{DeptID: "b", Steps: []dsl.ExecutionStep{{Sequential: []string{"b"}}}},
				}},
				{Sequential: []string{"tail"}},
			}},
		}},
	}
}

// A cancel that arrives while the branches of a parallel step run and nothing
// is parked cancels the instance once, ends the fork as terminated, never starts
// the next step, and writes no completion: the run ends TERMINATED.
func TestExecute_CancelWhileAParallelStepRunsCancelsOnceAndEndsTerminatedWithoutACompletionWrite(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, w23aParallelCollab())
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("a", "A1", 1))
	wflCancelAt(env, 6*time.Second)

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusTerminated, status)
	cancels := wflInputs[port.CancelInstanceInput](fakes, port.ActivityCancelInstance)
	require.Len(t, cancels, 1, "CancelInstance runs once")
	assert.Equal(t, wflTenant, cancels[0].TenantID)
	assert.Equal(t, wflInstance, cancels[0].InstanceID)
	assert.Equal(t, "admin-9", cancels[0].AdminUserID)
	assert.EqualValues(t, 4, cancels[0].RecordVersion)
	assert.Empty(t, wflStatuses(fakes), "a terminated run writes no instance status of its own")
	assert.ElementsMatch(t, []domain.NodeKey{"a/A1", "b/B1"}, wflNodesCreated(fakes), "the step after the fork never starts")
}

// w23aBoundaryHost is a department whose only stage carries a non-interrupting
// timer boundary with the given path, plus the departments the path may run.
func w23aBoundaryHost(timer *dsl.BoundaryTimer) *dsl.CompiledCollaboration {
	return &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main",
			Departments: []dsl.DepartmentDef{
				{ID: "ops", Stages: []dsl.StageDef{{Type: "prep", NodeID: "Host", BoundaryTimer: timer}}},
				{ID: "side", Stages: wflStages("Side")},
				{ID: "x", Stages: wflStages("X1")},
			},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"ops"}}}},
		}},
	}
}

// A non-interrupting boundary path that fails (its first task cannot be created)
// does not fail the host: the host keeps waiting, then completes, and the
// instance ends COMPLETED with no FAILED write.
func TestExecute_ANonInterruptingBoundaryPathThatFailsDoesNotFailTheHost(t *testing.T) {
	cases := []struct {
		name  string
		timer *dsl.BoundaryTimer
	}{
		{name: "a target department", timer: &dsl.BoundaryTimer{Duration: "1h", Interrupting: false, TargetDept: "side"}},
		{name: "a list of steps", timer: &dsl.BoundaryTimer{Duration: "1h", Interrupting: false, Steps: []dsl.ExecutionStep{{Sequential: []string{"side"}}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			fakes := wflNewFakes()
			wflRefuseTaskFor(fakes, "side/Side")
			wflRegisterAll(env, fakes, w23aBoundaryHost(tc.timer))
			wflSignalAt(env, 2*time.Hour, "stage-transition", wflDone("ops", "Host", 1))

			status, err := wflStart(t, env, fakes, "")

			require.NoError(t, err)
			assert.Equal(t, domain.InstanceStatusCompleted, status)
			assert.Equal(t, []domain.InstanceStatus{domain.InstanceStatusCompleted}, wflStatuses(fakes), "no FAILED or other instance write")
			assert.Equal(t, []domain.NodeKey{"ops/Host", "side/Side"}, wflNodesCreated(fakes), "the path was tried once and failed")
			taskWrites := wflInputs[port.UpdateTaskStatusInput](fakes, port.ActivityUpdateTaskStatus)
			require.Len(t, taskWrites, 1, "only the host's own completion writes a task status")
			assert.Equal(t, "ops/Host", taskWrites[0].TaskID)
			assert.Equal(t, domain.TaskStatusCompleted, taskWrites[0].Status)
			assert.Equal(t, wflTenant, taskWrites[0].TenantID)
		})
	}
}

// An instance cancel that reaches a spawned boundary path ends the run: the
// path reports it to the base run, which finishes TERMINATED and writes no
// completion, while the host task keeps waiting until then.
func TestExecute_ACancelInsideASpawnedBoundaryPathEndsTheRunTerminated(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	timer := &dsl.BoundaryTimer{Duration: "1h", Interrupting: false, Steps: []dsl.ExecutionStep{{Parallel: []dsl.ParallelBranch{
		{DeptID: "x", Steps: []dsl.ExecutionStep{{Sequential: []string{"x"}}}},
	}}}}
	wflRegisterAll(env, fakes, w23aBoundaryHost(timer))
	wflCancelAt(env, 2*time.Hour)

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusTerminated, status)
	cancels := wflInputs[port.CancelInstanceInput](fakes, port.ActivityCancelInstance)
	require.Len(t, cancels, 1)
	assert.Equal(t, wflInstance, cancels[0].InstanceID)
	assert.Equal(t, wflTenant, cancels[0].TenantID)
	assert.Contains(t, wflNodesCreated(fakes), domain.NodeKey("x/X1"), "the path was running when the cancel came")
	assert.Empty(t, wflStatuses(fakes), "a terminated run writes no instance status of its own")
}
