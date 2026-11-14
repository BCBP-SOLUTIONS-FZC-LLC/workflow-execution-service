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

// wflCallOf is a plan of one call step over department review (one task),
// carrying the given boundary paths, followed by department after.
func wflCallOf(timers []dsl.TimerPath, messages []dsl.MessagePath) *dsl.CompiledCollaboration {
	stage := func(node string) []dsl.StageDef { return []dsl.StageDef{{Type: "prep", NodeID: node}} }
	return &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main",
			Departments: []dsl.DepartmentDef{
				{ID: "review", Stages: stage("Review")},
				{ID: "esc", Stages: stage("Escalate")},
				{ID: "after", Stages: stage("Finish")},
			},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{
				{SubWorkflow: &dsl.SubWorkflowStep{
					NodeID: "Call", Name: "call",
					Plan:         dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"review"}}}},
					TimerPaths:   timers,
					MessagePaths: messages,
				}},
				{Sequential: []string{"after"}},
			}},
		}},
	}
}

func wflTaskStatusesOf(f *wflFakes, taskID string) []domain.TaskStatus {
	var out []domain.TaskStatus
	for _, in := range wflInputs[port.UpdateTaskStatusInput](f, port.ActivityUpdateTaskStatus) {
		if in.TaskID == taskID {
			out = append(out, in.Status)
		}
	}
	return out
}

// A non-interrupting boundary that fires on a call starts its side path and
// leaves the call running: the call's own task is not superseded, and once it
// resolves the run goes on to the step after the call.
func TestExecute_NonInterruptingBoundaryOnACallStartsASidePathAndTheCallRunsOn(t *testing.T) {
	cases := []struct {
		name  string
		build func() *dsl.CompiledCollaboration
		fire  func(env *testsuite.TestWorkflowEnvironment)
	}{
		{
			name: "timer",
			build: func() *dsl.CompiledCollaboration {
				return wflCallOf([]dsl.TimerPath{{Duration: "1h", Interrupting: false, TargetDept: "esc"}}, nil)
			},
			fire: func(*testsuite.TestWorkflowEnvironment) {},
		},
		{
			name: "message fired by hand",
			build: func() *dsl.CompiledCollaboration {
				return wflCallOf(nil, []dsl.MessagePath{{MessageName: "note", BoundaryID: "Boundary_Note", Interrupting: false, TargetDept: "esc"}})
			},
			fire: func(env *testsuite.TestWorkflowEnvironment) {
				wflSignalAt(env, 10*time.Minute, port.SignalInstanceExternalAnswer, wflAnswerWire{Key: "Call#Boundary_Note"})
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			fakes := wflNewFakes()
			wflRegisterAll(env, fakes, tc.build())
			tc.fire(env)
			var createdWhileCallRuns []domain.NodeKey
			env.RegisterDelayedCallback(func() { createdWhileCallRuns = wflNodesCreated(fakes) }, 90*time.Minute)
			wflSignalAt(env, 2*time.Hour, "stage-transition", wflDone("review", "Review", 1))
			wflSignalAt(env, 3*time.Hour, "stage-transition", wflDone("after", "Finish", 1))

			status, err := wflStart(t, env, fakes, "")

			require.NoError(t, err)
			assert.Equal(t, domain.InstanceStatusCompleted, status)
			assert.ElementsMatch(t, []domain.NodeKey{"review/Review", "esc/Escalate"}, createdWhileCallRuns,
				"the side path started while the call's task was still open")
			assert.Equal(t, []domain.NodeKey{"review/Review", "esc/Escalate", "after/Finish"}, wflNodesCreated(fakes),
				"the step after the call ran once the call resolved")
			assert.NotContains(t, wflTaskStatusesOf(fakes, "review/Review"), domain.TaskStatusSuperseded)
			assert.Equal(t, []domain.TaskStatus{domain.TaskStatusCompleted}, wflTaskStatusesOf(fakes, "review/Review"))
		})
	}
}

// A cancel that arrives while a call runs at the top level ends the run as
// TERMINATED: the cancel write is made once, no boundary path of the call
// starts, and the run is not also written COMPLETED or FAILED.
func TestExecute_CancelWhileACallRunsTerminatesWithoutStartingABoundaryPath(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, wflCallOf([]dsl.TimerPath{{Duration: "1h", Interrupting: true, TargetDept: "esc"}}, nil))
	wflSignalAt(env, 10*time.Second, "instance-cancel", adminSignalWire{AdminUserID: "admin-2", Reason: "withdrawn", RecordVersion: 6})

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusTerminated, status)
	assert.Equal(t, []domain.NodeKey{"review/Review"}, wflNodesCreated(fakes), "neither the boundary path nor the next step started")
	cancels := wflInputs[port.CancelInstanceInput](fakes, port.ActivityCancelInstance)
	require.Len(t, cancels, 1)
	assert.Equal(t, wflTenant, cancels[0].TenantID)
	assert.Equal(t, wflInstance, cancels[0].InstanceID)
	assert.Equal(t, "admin-2", cancels[0].AdminUserID)
	assert.EqualValues(t, 6, cancels[0].RecordVersion)
	assert.Empty(t, wflStatuses(fakes), "the cancel activity owns the final status")
}

// A cancel that arrives during a parallel step with no failed branch reaches
// the step's gateway, which ends the run as TERMINATED and runs the cancel
// write once.
func TestExecute_CancelDuringAParallelStepWithNoFailedBranchTerminates(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, parallelCollaboration())
	wflSignalAt(env, 5*time.Second, "instance-cancel", adminSignalWire{AdminUserID: "admin-4", Reason: "duplicate", RecordVersion: 2})

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusTerminated, status)
	cancels := wflInputs[port.CancelInstanceInput](fakes, port.ActivityCancelInstance)
	require.Len(t, cancels, 1)
	assert.Equal(t, wflTenant, cancels[0].TenantID)
	assert.Equal(t, wflInstance, cancels[0].InstanceID)
	assert.Equal(t, "admin-4", cancels[0].AdminUserID)
	require.NotNil(t, cancels[0].Reason)
	assert.Equal(t, "duplicate", *cancels[0].Reason)
	assert.ElementsMatch(t, []domain.NodeKey{"warehouse/prep", "billing/prep"}, wflNodesCreated(fakes))
	assert.Empty(t, wflStatuses(fakes))
}

// A cancel with an empty reason reaches the cancel write as no reason (null),
// not as an empty string; a reason that is given arrives as written.
func TestExecute_CancelWithAnEmptyReasonCarriesNoReason(t *testing.T) {
	cases := []struct {
		name   string
		reason string
		want   *string
	}{
		{name: "empty reason", reason: "", want: nil},
		{name: "given reason", reason: "wrong tender", want: func() *string { s := "wrong tender"; return &s }()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			fakes := wflNewFakes()
			wflRegisterAll(env, fakes, wflOneTaskCollab())
			wflSignalAt(env, 5*time.Second, "instance-cancel", adminSignalWire{AdminUserID: "admin-1", Reason: tc.reason, RecordVersion: 1})

			status, err := wflStart(t, env, fakes, "")

			require.NoError(t, err)
			assert.Equal(t, domain.InstanceStatusTerminated, status)
			cancels := wflInputs[port.CancelInstanceInput](fakes, port.ActivityCancelInstance)
			require.Len(t, cancels, 1)
			assert.Equal(t, tc.want, cancels[0].Reason)
			assert.Equal(t, wflInstance, cancels[0].InstanceID)
		})
	}
}

// A cancel and the completion of the last step, sent a second apart, are
// decided by which comes first: a cancel first terminates the run (one cancel
// write, no final status write); a completion first completes it and the later
// cancel is never read.
func TestExecute_CancelAndLastStepCompletionAreDecidedByWhichComesFirst(t *testing.T) {
	cases := []struct {
		name         string
		cancelAt     time.Duration
		finishAt     time.Duration
		wantStatus   domain.InstanceStatus
		wantCancels  int
		wantStatuses []domain.InstanceStatus
	}{
		{name: "cancel first", cancelAt: 5 * time.Second, finishAt: 6 * time.Second, wantStatus: domain.InstanceStatusTerminated, wantCancels: 1},
		{
			name: "completion first", cancelAt: 6 * time.Second, finishAt: 5 * time.Second, wantStatus: domain.InstanceStatusCompleted,
			wantStatuses: []domain.InstanceStatus{domain.InstanceStatusCompleted},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			fakes := wflNewFakes()
			wflRegisterAll(env, fakes, wflOneTaskCollab())
			wflSignalAt(env, tc.cancelAt, "instance-cancel", adminSignalWire{AdminUserID: "admin-1", RecordVersion: 1})
			wflSignalAt(env, tc.finishAt, "stage-transition", wflDone("ops", "Step", 1))

			status, err := wflStart(t, env, fakes, "")

			require.NoError(t, err)
			assert.Equal(t, tc.wantStatus, status)
			assert.Len(t, wflInputs[port.CancelInstanceInput](fakes, port.ActivityCancelInstance), tc.wantCancels)
			assert.Equal(t, tc.wantStatuses, wflStatuses(fakes))
		})
	}
}
