package workflow_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

func wflCancelAt(env *testsuite.TestWorkflowEnvironment, delay time.Duration) {
	wflSignalAt(env, delay, "instance-cancel", adminSignalWire{AdminUserID: "admin-9", RecordVersion: 4})
}

func wflRefuseTaskFor(fakes *wflFakes, node domain.NodeKey) {
	fakes.failFor = func(_ string, in any) error {
		if v, ok := in.(port.CreateTaskInput); ok && v.NodeKey == node {
			return wflRefuse("create " + string(node))
		}
		return nil
	}
}

func wflAssertCancelEndedTheInstance(t *testing.T, fakes *wflFakes, status domain.InstanceStatus, err error) {
	t.Helper()
	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusTerminated, status)
	cancels := wflInputs[port.CancelInstanceInput](fakes, port.ActivityCancelInstance)
	require.Len(t, cancels, 1)
	assert.Equal(t, wflTenant, cancels[0].TenantID)
	assert.Equal(t, wflInstance, cancels[0].InstanceID)
	for _, s := range wflStatuses(fakes) {
		assert.NotEqual(t, domain.InstanceStatusCompleted, s, "the instance was not completed over the cancel")
	}
}

// An instance-cancel that lands in the park of a fork nested inside a branch
// of another fork ends the whole instance: the outer fork does not count the
// branch as finished, the step after it never starts, and the instance is not
// written COMPLETED.
func TestExecute_CancelInANestedParkEndsTheWholeInstance(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := nestedParallelCollaboration()
	plan := collab.Plans[0]
	plan.Departments = append(plan.Departments, dsl.DepartmentDef{ID: "tail", Stages: wflStages("Tail")})
	plan.Execution.Steps = append(plan.Execution.Steps, dsl.ExecutionStep{Sequential: []string{"tail"}})
	fakes := wflNewFakes()
	wflRefuseTaskFor(fakes, "b2/prep")
	wflRegisterAll(env, fakes, collab)
	wflSignalsAt(env, 2*time.Second,
		wflSignal{"stage-transition", wflDoneStage("a", "prep", 1)},
		wflSignal{"stage-transition", wflDoneStage("b1", "prep", 1)},
	)
	wflCancelAt(env, 6*time.Second)

	status, err := wflStart(t, env, fakes, "")

	wflAssertCancelEndedTheInstance(t, fakes, status, err)
	assert.NotContains(t, wflNodesCreated(fakes), domain.NodeKey("tail/Tail"), "no step ran after the cancel")
}

// An instance-cancel that lands in the park of a fork inside an interrupting
// boundary path ends the whole instance instead of ending only that path.
func TestExecute_CancelInAParkInsideAFiredBoundaryPathEndsTheWholeInstance(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main",
			Departments: []dsl.DepartmentDef{
				{ID: "ops", Stages: []dsl.StageDef{
					{Type: "prep", NodeID: "Prepare", BoundaryTimer: &dsl.BoundaryTimer{
						Duration: "1h", Interrupting: true,
						Steps: []dsl.ExecutionStep{{Parallel: []dsl.ParallelBranch{
							{DeptID: "x", Steps: []dsl.ExecutionStep{{Sequential: []string{"x"}}}},
							{DeptID: "y", Steps: []dsl.ExecutionStep{{Sequential: []string{"y"}}}},
						}}},
					}},
					{Type: "prep", NodeID: "Hold"},
				}},
				{ID: "x", Stages: wflStages("X1")},
				{ID: "y", Stages: wflStages("Y1")},
			},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"ops"}}}},
		}},
	}
	fakes := wflNewFakes()
	wflRefuseTaskFor(fakes, "x/X1")
	wflRegisterAll(env, fakes, collab)
	wflSignalAt(env, 2*time.Hour, "stage-transition", wflDone("y", "Y1", 1))
	wflCancelAt(env, 3*time.Hour)

	status, err := wflStart(t, env, fakes, "")

	wflAssertCancelEndedTheInstance(t, fakes, status, err)
}

// An instance-cancel that lands in the park of a fork inside a call step ends
// the whole instance: the step after the call never starts.
func TestExecute_CancelInAParkInsideACallEndsTheWholeInstance(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main",
			Departments: []dsl.DepartmentDef{
				{ID: "x", Stages: wflStages("X1")},
				{ID: "y", Stages: wflStages("Y1")},
				{ID: "after", Stages: wflStages("Finish")},
			},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{
				{SubWorkflow: &dsl.SubWorkflowStep{
					NodeID: "Call", Name: "call",
					Plan: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Parallel: []dsl.ParallelBranch{
						{DeptID: "x", Steps: []dsl.ExecutionStep{{Sequential: []string{"x"}}}},
						{DeptID: "y", Steps: []dsl.ExecutionStep{{Sequential: []string{"y"}}}},
					}}}},
				}},
				{Sequential: []string{"after"}},
			}},
		}},
	}
	fakes := wflNewFakes()
	wflRefuseTaskFor(fakes, "x/X1")
	wflRegisterAll(env, fakes, collab)
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("y", "Y1", 1))
	wflCancelAt(env, 6*time.Second)

	status, err := wflStart(t, env, fakes, "")

	wflAssertCancelEndedTheInstance(t, fakes, status, err)
	assert.NotContains(t, wflNodesCreated(fakes), domain.NodeKey("after/Finish"), "no step ran after the cancel")
}

// An instance-cancel that lands in the park of a fork inside a non-interrupting
// boundary path ends the whole instance: the host task that is still open does
// not carry the run on to COMPLETED.
func TestExecute_CancelInAParkInsideANonInterruptingBoundaryPathEndsTheWholeInstance(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main",
			Departments: []dsl.DepartmentDef{
				{ID: "ops", Stages: []dsl.StageDef{
					{Type: "prep", NodeID: "Prepare", BoundaryTimer: &dsl.BoundaryTimer{
						Duration: "30m", Interrupting: false,
						Steps: []dsl.ExecutionStep{{Parallel: []dsl.ParallelBranch{
							{DeptID: "x", Steps: []dsl.ExecutionStep{{Sequential: []string{"x"}}}},
							{DeptID: "y", Steps: []dsl.ExecutionStep{{Sequential: []string{"y"}}}},
						}}},
					}},
				}},
				{ID: "x", Stages: wflStages("X1")},
				{ID: "y", Stages: wflStages("Y1")},
			},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"ops"}}}},
		}},
	}
	fakes := wflNewFakes()
	wflRefuseTaskFor(fakes, "x/X1")
	wflRegisterAll(env, fakes, collab)
	wflSignalAt(env, time.Hour, "stage-transition", wflDone("y", "Y1", 1))
	wflCancelAt(env, 2*time.Hour)
	wflSignalAt(env, 3*time.Hour, "stage-transition", wflDone("ops", "Prepare", 1))

	status, err := wflStart(t, env, fakes, "")

	wflAssertCancelEndedTheInstance(t, fakes, status, err)
	for _, c := range wflInputs[port.CompleteAssignmentInput](fakes, port.ActivityCompleteAssignment) {
		assert.NotEqual(t, "ops/Prepare", c.TaskID, "the host task was never completed")
	}
}

// The same for a non-interrupting boundary path that names a department.
func TestExecute_CancelInAParkInsideANonInterruptingTargetDepartmentEndsTheWholeInstance(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main",
			Departments: []dsl.DepartmentDef{
				{ID: "ops", Stages: []dsl.StageDef{
					{Type: "prep", NodeID: "Prepare", BoundaryTimer: &dsl.BoundaryTimer{Duration: "30m", Interrupting: false, TargetDept: "side"}},
				}},
				{ID: "side", Stages: []dsl.StageDef{{
					Type: "prep", NodeID: "Look",
					BoundaryTimer: &dsl.BoundaryTimer{
						Duration: "10m", Interrupting: true,
						Steps: []dsl.ExecutionStep{{Parallel: []dsl.ParallelBranch{
							{DeptID: "x", Steps: []dsl.ExecutionStep{{Sequential: []string{"x"}}}},
							{DeptID: "y", Steps: []dsl.ExecutionStep{{Sequential: []string{"y"}}}},
						}}},
					},
				}}},
				{ID: "x", Stages: wflStages("X1")},
				{ID: "y", Stages: wflStages("Y1")},
			},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"ops"}}}},
		}},
	}
	fakes := wflNewFakes()
	wflRefuseTaskFor(fakes, "x/X1")
	wflRegisterAll(env, fakes, collab)
	wflSignalAt(env, 2*time.Hour, "stage-transition", wflDone("y", "Y1", 1))
	wflCancelAt(env, 3*time.Hour)
	wflSignalAt(env, 4*time.Hour, "stage-transition", wflDone("ops", "Prepare", 1))

	status, err := wflStart(t, env, fakes, "")

	wflAssertCancelEndedTheInstance(t, fakes, status, err)
	for _, c := range wflInputs[port.CompleteAssignmentInput](fakes, port.ActivityCompleteAssignment) {
		assert.NotEqual(t, "ops/Prepare", c.TaskID, "the host task was never completed")
	}
}
