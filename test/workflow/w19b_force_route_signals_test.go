package workflow_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
)

type w19bWarning struct {
	msg     string
	keyvals []any
}

// w19bWarnings records the warnings of a test workflow environment.
type w19bWarnings struct {
	mu   sync.Mutex
	logs []w19bWarning
}

func (w *w19bWarnings) Debug(string, ...any) {}
func (w *w19bWarnings) Info(string, ...any)  {}
func (w *w19bWarnings) Error(string, ...any) {}
func (w *w19bWarnings) Warn(msg string, keyvals ...any) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.logs = append(w.logs, w19bWarning{msg: msg, keyvals: keyvals})
}

// carrying returns the warnings that carry the field with the given value.
func (w *w19bWarnings) carrying(field string, value any) []w19bWarning {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []w19bWarning
	for _, l := range w.logs {
		for i := 0; i+1 < len(l.keyvals); i += 2 {
			if l.keyvals[i] == field && l.keyvals[i+1] == value {
				out = append(out, l)
				break
			}
		}
	}
	return out
}

func w19bNewEnv(warnings *w19bWarnings) *testsuite.TestWorkflowEnvironment {
	var suite testsuite.WorkflowTestSuite
	suite.SetLogger(warnings)
	return suite.NewTestWorkflowEnvironment()
}

func w19bSupersededTasks(fakes *wflFakes) []string {
	var out []string
	for _, in := range wflInputs[port.UpdateTaskStatusInput](fakes, port.ActivityUpdateTaskStatus) {
		if in.Status == domain.TaskStatusSuperseded {
			out = append(out, in.TaskID)
		}
	}
	return out
}

// A force-forward or force-back that reaches a workflow already paused (the
// status changed after the HTTP check) is dropped with a warning: no audit
// record, no task superseded, no stage re-entered, and the run carries on from
// where it was once it is resumed.
func TestExecute_AForceRouteSignalReachingAPausedInstance_IsDroppedWithAWarningAndChangesNothing(t *testing.T) {
	cases := []struct {
		name   string
		signal string
	}{
		{"force-forward", "instance-force-forward"},
		{"force-back", "instance-force-back"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			warnings := &w19bWarnings{}
			env := w19bNewEnv(warnings)
			fakes := wflNewFakes()
			wflRegisterAll(env, fakes, wflTwoDeptsCollab())
			wflSignalAt(env, 2*time.Second, "instance-pause", wflAdminPause(domain.InitiatorAdmin))
			wflSignalAt(env, 4*time.Second, tc.signal, adminSignalWire{AdminUserID: "admin-2", TargetNodeKey: "deptB/B1", RecordVersion: 4})
			var whilePaused wfengine.WorkflowStatusQuery
			wflQueryStatusAt(t, env, 5*time.Second, &whilePaused)
			wflSignalAt(env, 6*time.Second, "instance-resume", adminSignalWire{AdminUserID: "admin-1", Initiator: domain.InitiatorAdmin, RecordVersion: 5})
			wflSignalAt(env, 8*time.Second, "stage-transition", wflDone("deptA", "A1", 1))
			wflSignalAt(env, 10*time.Second, "stage-transition", wflDone("deptB", "B1", 1))

			status, err := wflStart(t, env, fakes, "")

			require.NoError(t, err)
			assert.Equal(t, domain.InstanceStatusCompleted, status)
			assert.Equal(t, domain.InstanceStatusPaused, whilePaused.Status, "the dropped signal did not change the paused instance")
			assert.Len(t, warnings.carrying("signal", tc.signal), 1, "the drop is logged once, naming the signal")
			assert.Empty(t, wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute), "no route is audited")
			assert.Empty(t, w19bSupersededTasks(fakes), "no open task is superseded")
			assert.Equal(t, []domain.NodeKey{"deptA/A1", "deptB/B1"}, wflNodesCreated(fakes), "each stage ran once, in plan order")
		})
	}
}

// A force-forward to a department that is in no step, no boundary path and no
// call path runs that department alone: the stages after the abandoned step
// do not run, and the instance completes when the department's own stages do.
func TestExecute_AForceForwardToADepartmentOutsideEveryStepPath_RunsItAloneAndTheInstanceCompletes(t *testing.T) {
	collab := &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main",
			Departments: []dsl.DepartmentDef{
				{ID: "intake", Stages: wflStages("A1")},
				{ID: "closing", Stages: wflStages("C1")},
				{ID: "orphan", Stages: wflStages("O1", "O2")},
			},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"intake"}}, {Sequential: []string{"closing"}}}},
		}},
	}
	env := w19bNewEnv(&w19bWarnings{})
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, collab)
	wflSignalAt(env, 5*time.Second, "instance-force-forward", adminSignalWire{AdminUserID: "admin-1", TargetNodeKey: "orphan/O1", RecordVersion: 3})
	wflSignalAt(env, 10*time.Second, "stage-transition", wflDone("orphan", "O1", 1))
	wflSignalAt(env, 15*time.Second, "stage-transition", wflDone("orphan", "O2", 1))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.Equal(t, []domain.NodeKey{"intake/A1", "orphan/O1", "orphan/O2"}, wflNodesCreated(fakes), "the department runs alone; closing never starts")
	routes := wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute)
	require.Len(t, routes, 1)
	assert.Equal(t, port.RecordForceRouteInput{
		InstanceID: wflInstance, TenantID: wflTenant, OldNodeKeys: []domain.NodeKey{"intake/A1"},
		TargetNodeID: "orphan/O1", Direction: domain.ForceRouteDirectionForward, AdminUserID: "admin-1", RecordVersion: 3,
	}, routes[0])
}

// A second force-forward naming a branch of a parallel step that the first one
// already resolved is dropped with a warning that names the department: it
// writes no audit record and changes nothing, and the step still completes
// when its remaining branch does.
func TestExecute_AForceForwardNamingABranchNoLongerActive_IsDroppedWithAWarningNamingTheDepartment(t *testing.T) {
	warnings := &w19bWarnings{}
	env := w19bNewEnv(warnings)
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, parallelCollaboration())
	forward := adminSignalWire{AdminUserID: "admin-1", TargetDeptID: "billing", TargetNodeKey: "billing/prep", RecordVersion: 1}
	wflSignalAt(env, 5*time.Second, "instance-force-forward", forward)
	wflSignalAt(env, 6*time.Second, "instance-force-forward", forward)
	wflSignalAt(env, 10*time.Second, "stage-transition", wflDoneStage("warehouse", "prep", 1))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.Len(t, warnings.carrying("target_dept_id", "billing"), 1, "only the second signal is dropped, and the warning names the department")
	routes := wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute)
	require.Len(t, routes, 1, "only the first signal is audited")
	assert.Equal(t, []domain.NodeKey{"billing/prep"}, routes[0].OldNodeKeys)
	assert.ElementsMatch(t, []domain.NodeKey{"warehouse/prep", "billing/prep"}, wflNodesCreated(fakes), "no stage is re-entered")
}
