package workflow_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

const (
	w9bDeptXIAM = "018e1f2a-0000-7000-8000-0000000000a2"
	w9bDeptYIAM = "018e1f2a-0000-7000-8000-0000000000b2"
)

// w9bTwoForksOneInsideACall is an outer fork of two branches. Branch x is an
// inner fork of x1 and x2. Branch y is a call whose body is an inner fork of y1
// and y2 and whose timer path interrupts the call after three seconds into esc.
func w9bTwoForksOneInsideACall() *dsl.CompiledCollaboration {
	seq := func(dept string) []dsl.ExecutionStep { return []dsl.ExecutionStep{{Sequential: []string{dept}}} }
	inner := func(left, right string) dsl.ExecutionStep {
		return dsl.ExecutionStep{Parallel: []dsl.ParallelBranch{
			{DeptID: left, Steps: seq(left)},
			{DeptID: right, Steps: seq(right)},
		}}
	}
	return &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main",
			Departments: []dsl.DepartmentDef{
				{ID: "x1", Stages: wflStages("X1")},
				{ID: "x2", IAMDepartmentID: w9bDeptXIAM, Stages: wflStages("X2")},
				{ID: "y1", Stages: wflStages("Y1")},
				{ID: "y2", IAMDepartmentID: w9bDeptYIAM, Stages: wflStages("Y2")},
				{ID: "esc", Stages: wflStages("E1")},
			},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Parallel: []dsl.ParallelBranch{
				{DeptID: "x", Steps: []dsl.ExecutionStep{inner("x1", "x2")}},
				{DeptID: "y", Steps: []dsl.ExecutionStep{{SubWorkflow: &dsl.SubWorkflowStep{
					NodeID: "Call", Name: "call",
					Plan:       dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{inner("y1", "y2")}},
					TimerPaths: []dsl.TimerPath{{Duration: "3s", Interrupting: true, TargetDept: "esc"}},
				}}}},
			}}}},
		}},
	}
}

func w9bRefuseCreates(fakes *wflFakes, nodes ...domain.NodeKey) {
	fakes.failFor = func(_ string, in any) error {
		if v, ok := in.(port.CreateTaskInput); ok {
			for _, n := range nodes {
				if v.NodeKey == n {
					return wflRefuse("create " + string(n))
				}
			}
		}
		return nil
	}
}

func w9bDegradedWrites(fakes *wflFakes) []port.UpdateInstanceStatusInput {
	var out []port.UpdateInstanceStatusInput
	for _, in := range wflInputs[port.UpdateInstanceStatusInput](fakes, port.ActivityUpdateInstanceStatus) {
		if in.Status == domain.InstanceStatusDegraded {
			out = append(out, in)
		}
	}
	return out
}

func w9bDepartmentsOf(in port.UpdateInstanceStatusInput) []uuid.UUID {
	var out []uuid.UUID
	for _, b := range in.FailedBranches {
		out = append(out, b.DepartmentID)
	}
	return out
}

// When one of two open parks is abandoned (its call was interrupted), the
// instance is written DEGRADED again and lists only the branches of the park
// that is still open; RUNNING is written only once that park closes.
func TestExecute_ParkAbandonedWhileAnotherParkIsOpenWritesDegradedForTheRemainingParkOnly(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	w9bRefuseCreates(fakes, "x2/X2", "y2/Y2")
	wflRegisterAll(env, fakes, w9bTwoForksOneInsideACall())
	// The two parks open one after the other: signals at the same instant are delivered in no
	// fixed order, which made the second degraded write list one park or two.
	wflSignalAt(env, 1*time.Second, "stage-transition", wflDone("x1", "X1", 1))
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("y1", "Y1", 1))
	wflSignalAt(env, 5*time.Second, "stage-transition", wflDone("esc", "E1", 1))
	wflSignalAt(env, 8*time.Second, "instance-force-forward", adminSignalWire{AdminUserID: "admin-7", TargetNodeKey: "x2/X2", RecordVersion: 9})

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	writes := w9bDegradedWrites(fakes)
	require.Len(t, writes, 3, "one write per park opened, and one when a park was abandoned")
	for _, w := range writes {
		assert.Equal(t, wflTenant, w.TenantID)
		assert.Equal(t, wflInstance, w.InstanceID)
	}
	assert.ElementsMatch(t, []uuid.UUID{uuid.MustParse(w9bDeptXIAM), uuid.MustParse(w9bDeptYIAM)}, w9bDepartmentsOf(writes[1]),
		"both parks were open before the abandon")
	assert.Equal(t, []uuid.UUID{uuid.MustParse(w9bDeptXIAM)}, w9bDepartmentsOf(writes[2]),
		"after the abandon only the open park's branch is listed")
	statuses := wflStatuses(fakes)
	require.GreaterOrEqual(t, len(statuses), 2)
	assert.Equal(t, []domain.InstanceStatus{domain.InstanceStatusRunning, domain.InstanceStatusCompleted}, statuses[len(statuses)-2:],
		"RUNNING comes only when the remaining park closes")
}

// When the only open park is abandoned while an admin pause is held, RUNNING is
// never written: the instance goes back to PAUSED through the pause activity,
// and a refused pause is only logged.
func TestExecute_ParkAbandonedWhileAPauseIsHeldGoesBackToPausedInsteadOfRunning(t *testing.T) {
	cases := []struct {
		name        string
		refusePause bool
	}{
		{name: "pause accepted", refusePause: false},
		{name: "pause refused is only logged", refusePause: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			fakes := wflNewFakes()
			created := false
			fakes.failFor = func(name string, in any) error {
				if v, ok := in.(port.CreateTaskInput); ok && v.NodeKey == "p1/P1" && !created {
					created = true
					return wflRefuse(name)
				}
				if tc.refusePause && name == port.ActivityPauseInstance {
					return wflRefuse(name)
				}
				return nil
			}
			wflRegisterAll(env, fakes, dfkCallWithFork())
			wflSignalAt(env, 1*time.Second, "stage-transition", wflDone("p2", "P2", 1))
			wflSignalAt(env, 2*time.Second, "instance-pause", wflAdminPause("admin"))
			wflCancelAt(env, 20*time.Second)

			status, err := wflStart(t, env, fakes, "")

			wflAssertCancelEndedTheInstance(t, fakes, status, err)
			assert.Equal(t, []domain.InstanceStatus{domain.InstanceStatusDegraded}, wflStatuses(fakes),
				"no RUNNING write while the pause is held")
			pauses := wflInputs[port.PauseInstanceInput](fakes, port.ActivityPauseInstance)
			require.Len(t, pauses, 1)
			assert.Equal(t, wflTenant, pauses[0].TenantID)
			assert.Equal(t, wflInstance, pauses[0].InstanceID)
			assert.Equal(t, "admin-1", pauses[0].AdminUserID)
			assert.Equal(t, "admin", pauses[0].Initiator)
		})
	}
}
