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
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

func wflRefuse(name string) error {
	return temporal.NewApplicationError(name+" refused", "ValidationError")
}

func wflAdminPause(initiator string) adminSignalWire {
	return adminSignalWire{AdminUserID: "admin-1", Initiator: initiator, RecordVersion: 2}
}

// A pause whose status write is refused changes nothing: the instance stays
// RUNNING, so the open task still completes and the run finishes COMPLETED.
func TestExecute_PauseWhoseWriteIsRefusedLeavesTheInstanceRunning(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	fakes.failFor = func(name string, _ any) error {
		if name == port.ActivityPauseInstance {
			return wflRefuse(name)
		}
		return nil
	}
	wflRegisterAll(env, fakes, wflOneDept("sales", dsl.StageDef{Type: "prep"}))
	wflSignalAt(env, 2*time.Second, "instance-pause", wflAdminPause(domain.InitiatorAdmin))
	var queried wfengine.WorkflowStatusQuery
	wflQueryStatusAt(t, env, 4*time.Second, &queried)
	wflSignalAt(env, 6*time.Second, "stage-transition", wflDoneStage("sales", "prep", 1))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.Equal(t, domain.InstanceStatusRunning, queried.Status, "the refused pause did not change the status")
	pauses := wflInputs[port.PauseInstanceInput](fakes, port.ActivityPauseInstance)
	require.Len(t, pauses, 1)
	assert.Equal(t, wflTenant, pauses[0].TenantID)
	assert.Equal(t, wflInstance, pauses[0].InstanceID)
}

// A resume whose status write is refused changes nothing: the instance stays
// PAUSED and the result that arrived during the pause stays held, so the task
// is never completed.
func TestExecute_ResumeWhoseWriteIsRefusedKeepsTheInstancePausedAndTheResultHeld(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	fakes.failFor = func(name string, _ any) error {
		if name == port.ActivityResumeInstance {
			return wflRefuse(name)
		}
		return nil
	}
	wflRegisterAll(env, fakes, wflOneDept("sales", dsl.StageDef{Type: "prep"}))
	wflSignalAt(env, 2*time.Second, "instance-pause", wflAdminPause(domain.InitiatorAdmin))
	wflSignalAt(env, 4*time.Second, "stage-transition", wflDoneStage("sales", "prep", 1))
	wflSignalAt(env, 6*time.Second, "instance-resume", wflAdminPause(domain.InitiatorAdmin))
	var queried wfengine.WorkflowStatusQuery
	wflQueryStatusAt(t, env, 8*time.Second, &queried)
	wflSignalAt(env, 10*time.Second, "instance-cancel", wflAdminPause(domain.InitiatorAdmin))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusTerminated, status)
	assert.Equal(t, domain.InstanceStatusPaused, queried.Status, "the refused resume did not change the status")
	assert.Zero(t, fakes.attemptsOf(port.ActivityCompleteAssignment), "the held result was not released")
	resumes := wflInputs[port.ResumeInstanceInput](fakes, port.ActivityResumeInstance)
	require.Len(t, resumes, 1)
	assert.Equal(t, wflTenant, resumes[0].TenantID)
	assert.Equal(t, wflInstance, resumes[0].InstanceID)
}

func wflTwoDeptsCollab() *dsl.CompiledCollaboration {
	return &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main",
			Departments: []dsl.DepartmentDef{
				{ID: "deptA", Stages: wflStages("A1")},
				{ID: "deptB", Stages: wflStages("B1")},
			},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"deptA", "deptB"}}}},
		}},
	}
}

// A top-level force route whose audit write (the superseding of the abandoned
// tasks and the force-routed event) is refused fails the run with that error
// and does not redirect: the target department is not started.
func TestExecute_TopLevelForceRouteWhoseAuditWriteIsRefusedFailsTheRunWithoutRedirecting(t *testing.T) {
	cases := []struct {
		name        string
		signal      string
		target      string
		prepare     func(env *testsuite.TestWorkflowEnvironment)
		wantCreated []domain.NodeKey
	}{
		{
			name: "forward", signal: "instance-force-forward", target: "deptB/B1",
			wantCreated: []domain.NodeKey{"deptA/A1"},
		},
		{
			name: "back", signal: "instance-force-back", target: "deptA/A1",
			prepare: func(env *testsuite.TestWorkflowEnvironment) {
				wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("deptA", "A1", 1))
			},
			wantCreated: []domain.NodeKey{"deptA/A1", "deptB/B1"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			fakes := wflNewFakes()
			fakes.failFor = func(name string, _ any) error {
				if name == port.ActivityRecordForceRoute {
					return wflRefuse(name)
				}
				return nil
			}
			wflRegisterAll(env, fakes, wflTwoDeptsCollab())
			if tc.prepare != nil {
				tc.prepare(env)
			}
			wflSignalAt(env, 5*time.Second, tc.signal, adminSignalWire{AdminUserID: "admin-1", TargetNodeKey: tc.target, RecordVersion: 3})

			status, err := wflStart(t, env, fakes, "")

			require.Error(t, err)
			assert.Contains(t, err.Error(), port.ActivityRecordForceRoute)
			assert.Equal(t, []domain.NodeKey(nil), wflStartedAfter(fakes, tc.wantCreated), "no department was started after the refusal")
			assert.NotEqual(t, domain.InstanceStatusCompleted, status)
			assert.Equal(t, []domain.InstanceStatus{domain.InstanceStatusFailed}, wflStatuses(fakes))
			audits := wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute)
			require.Len(t, audits, 1)
			assert.Equal(t, wflTenant, audits[0].TenantID)
			assert.Equal(t, wflInstance, audits[0].InstanceID)
		})
	}
}

// wflStartedAfter lists the tasks created beyond the expected ones.
func wflStartedAfter(f *wflFakes, expected []domain.NodeKey) []domain.NodeKey {
	created := wflNodesCreated(f)
	if len(created) <= len(expected) {
		return nil
	}
	return created[len(expected):]
}

// A top-level force route whose current-node write is refused fails the run
// and does not start the target department.
func TestExecute_TopLevelForceForwardWhoseCurrentNodeWriteIsRefusedFailsTheRun(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	fakes.failFor = func(name string, in any) error {
		if v, ok := in.(port.UpdateInstanceNodesInput); ok && len(v.NodeKeys) == 1 && v.NodeKeys[0] == "deptB/B1" {
			return wflRefuse(name)
		}
		return nil
	}
	wflRegisterAll(env, fakes, wflTwoDeptsCollab())
	wflSignalAt(env, 5*time.Second, "instance-force-forward", adminSignalWire{AdminUserID: "admin-1", TargetNodeKey: "deptB/B1", RecordVersion: 3})

	_, err := wflStart(t, env, fakes, "")

	require.Error(t, err)
	assert.Contains(t, err.Error(), port.ActivityUpdateInstanceNodes)
	assert.Equal(t, []domain.NodeKey{"deptA/A1"}, wflNodesCreated(fakes), "the target department was not started")
}

// An interrupting boundary that fires on a task fails the run when the write
// that marks the host task SUPERSEDED is refused, and the fired path does not
// run while the host task is still open.
func TestExecute_InterruptingBoundaryWhoseSupersedeWriteIsRefusedFailsTheRunWithoutRunningThePath(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main",
			Departments: []dsl.DepartmentDef{
				{ID: "ops", Stages: []dsl.StageDef{
					{Type: "prep", NodeID: "Prepare", BoundaryTimer: &dsl.BoundaryTimer{Duration: "1h", Interrupting: true, TargetDept: "esc"}},
				}},
				{ID: "esc", Stages: wflStages("Escalate")},
			},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"ops"}}}},
		}},
	}
	fakes := wflNewFakes()
	fakes.failFor = func(name string, in any) error {
		if v, ok := in.(port.UpdateTaskStatusInput); ok && v.Status == domain.TaskStatusSuperseded {
			return wflRefuse(name)
		}
		return nil
	}
	wflRegisterAll(env, fakes, collab)

	_, err := wflStart(t, env, fakes, "")

	require.Error(t, err)
	assert.Contains(t, err.Error(), port.ActivityUpdateTaskStatus)
	assert.Equal(t, []domain.NodeKey{"ops/Prepare"}, wflNodesCreated(fakes), "the escalation path did not start")
	updates := wflInputs[port.UpdateTaskStatusInput](fakes, port.ActivityUpdateTaskStatus)
	require.Len(t, updates, 1)
	assert.Equal(t, "ops/Prepare", updates[0].TaskID)
	assert.Equal(t, wflTenant, updates[0].TenantID)
}
