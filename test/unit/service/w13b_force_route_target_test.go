package service_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var w13bPlanStart = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// w13bBranchPlan is a plan whose main route runs one parallel step of the
// "legal" branch (with the given steps) and the "finance" branch, then
// "archive". The extra departments are in the plan but on no route.
func w13bBranchPlan(legal dsl.DepartmentDef, legalSteps []dsl.ExecutionStep, extra ...dsl.DepartmentDef) *dsl.CompiledPlan {
	depts := []dsl.DepartmentDef{legal, frfixDept("finance", "check"), frfixDept("archive", "file")}
	return &dsl.CompiledPlan{
		Name: "main", TaskQueue: "wf-queue-default",
		Departments: append(depts, extra...),
		Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{
			{Parallel: []dsl.ParallelBranch{
				{DeptID: "legal", Steps: legalSteps},
				{DeptID: "finance", Steps: []dsl.ExecutionStep{{Sequential: []string{"finance"}}}},
			}},
			{Sequential: []string{"archive"}},
		}},
	}
}

func w13bLegalOnly() []dsl.ExecutionStep {
	return []dsl.ExecutionStep{{Sequential: []string{"legal"}}}
}

// w13bRoutedRig is a running instance with the plan, whose compiled plan is
// served by a recording fake, and one task of the node.
func w13bRoutedRig(t *testing.T, plan *dsl.CompiledPlan, openNode string, status domain.TaskStatus) (*w7bForceRig, *csdDefinitions) {
	t.Helper()
	rig := w7bNewForceRig(t, frfixPlanJSON(t, dsl.CurrentSchemaVersion, plan))
	rig.instance.WorkflowVersionID = uuid.New()
	defs := &csdDefinitions{resp: publishedCompiledWorkflow(uuid.New(), rig.instance.WorkflowVersionID, frfixPlanJSON(t, dsl.CurrentSchemaVersion, plan))}
	rig.svc.Definitions = defs
	if openNode != "" {
		rig.addTasks(1, openNode, status, w13bPlanStart)
	}
	return rig, defs
}

func (r *w7bForceRig) w13bRequireSignalled(t *testing.T, target string) {
	t.Helper()
	require.Len(t, r.signalled(), 1, "the jump reaches the workflow once")
	call := r.signalled()[0]
	assert.Equal(t, port.SignalInstanceForceFwd, call.SignalName)
	assert.Equal(t, r.instance.ID, call.InstanceID)
	assert.Equal(t, "tenant:biz", call.TemporalWorkflowID)
	assert.Equal(t, target, w7bSignalTarget(t, call))
}

func TestForceForward_TaskStatusInABranch_OpensTheParallelStepOnlyWhenReadyOrInProgress(t *testing.T) {
	cases := []struct {
		name   string
		status domain.TaskStatus
		open   bool
	}{
		{"ready opens the step", domain.TaskStatusReady, true},
		{"in progress opens the step", domain.TaskStatusInProgress, true},
		{"completed does not", domain.TaskStatusCompleted, false},
		{"failed does not", domain.TaskStatusFailed, false},
		{"deferred does not", domain.TaskStatusDeferred, false},
		{"superseded does not", domain.TaskStatusSuperseded, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig, _ := w13bRoutedRig(t, w13bBranchPlan(frfixDept("legal", "review"), w13bLegalOnly()), "legal/review", tc.status)

			err := rig.forward("archive/file")

			if tc.open {
				require.ErrorIs(t, err, port.ErrTargetNodeNotFound, "a node after an open parallel step is refused")
				assert.Empty(t, rig.signalled())
			} else {
				require.NoError(t, err, "with no open task there is no open step to stay inside")
				rig.w13bRequireSignalled(t, "archive/file")
			}
			rig.requireListedForRig(t)
		})
	}
}

func TestForceForward_CompiledPlanReadFails_ReturnsTheCauseAndSignalsNothing(t *testing.T) {
	t.Run("the Definitions read fails", func(t *testing.T) {
		rig, defs := w13bRoutedRig(t, w13bBranchPlan(frfixDept("legal", "review"), w13bLegalOnly()), "legal/review", domain.TaskStatusReady)
		defs.resp, defs.err = nil, port.ErrDefinitionUnavailable

		err := rig.forward("finance/check")

		require.ErrorIs(t, err, port.ErrDefinitionUnavailable)
		assert.NotErrorIs(t, err, port.ErrTargetNodeNotFound)
		assert.NotErrorIs(t, err, port.ErrVersionInvalid)
		assert.Equal(t, []csdIDCall{{TenantID: rig.tenantID, ID: rig.instance.WorkflowVersionID}}, defs.calls)
		assert.Empty(t, rig.signalled())
		assert.Empty(t, rig.tasks.pages, "the open tasks are not read without a plan")
	})

	t.Run("a call to a plan the collaboration does not hold cannot be expanded", func(t *testing.T) {
		plan := w13bBranchPlan(frfixDept("legal", "review"), []dsl.ExecutionStep{
			{Sequential: []string{"legal"}},
			{CallPlan: &dsl.CallPlanStep{NodeID: "CA_1", Name: "CA_1", Plan: "missing"}},
		})
		rig, defs := w13bRoutedRig(t, plan, "legal/review", domain.TaskStatusReady)

		err := rig.forward("finance/check")

		require.ErrorIs(t, err, port.ErrVersionInvalid)
		assert.NotErrorIs(t, err, port.ErrTargetNodeNotFound)
		assert.Equal(t, []csdIDCall{{TenantID: rig.tenantID, ID: rig.instance.WorkflowVersionID}}, defs.calls)
		assert.Empty(t, rig.signalled())
		assert.Empty(t, rig.tasks.pages)
	})

	t.Run("control: the same plan without the bad call is accepted", func(t *testing.T) {
		rig, _ := w13bRoutedRig(t, w13bBranchPlan(frfixDept("legal", "review"), w13bLegalOnly()), "legal/review", domain.TaskStatusReady)

		require.NoError(t, rig.forward("finance/check"))
		rig.w13bRequireSignalled(t, "finance/check")
	})
}

func TestForceForward_DegradedInstance_ReadsNoTasksAfterThePlanCheck(t *testing.T) {
	plan := w13bBranchPlan(frfixDept("legal", "review"), w13bLegalOnly())

	t.Run("a degraded instance accepts a node outside the open step without listing tasks", func(t *testing.T) {
		rig, _ := w13bRoutedRig(t, plan, "legal/review", domain.TaskStatusFailed)
		rig.instance.Status = domain.InstanceStatusDegraded

		require.NoError(t, rig.forward("archive/file"))

		rig.w13bRequireSignalled(t, "archive/file")
		assert.Empty(t, rig.tasks.pages, "no task is read")
	})

	t.Run("a degraded instance still refuses a node the plan does not hold", func(t *testing.T) {
		rig, _ := w13bRoutedRig(t, plan, "legal/review", domain.TaskStatusFailed)
		rig.instance.Status = domain.InstanceStatusDegraded

		require.ErrorIs(t, rig.forward("archive/typo"), port.ErrTargetNodeNotFound)
		assert.Empty(t, rig.signalled())
		assert.Empty(t, rig.tasks.pages)
	})

	t.Run("control: a running instance with an open task reads the tasks and refuses the same node", func(t *testing.T) {
		rig, _ := w13bRoutedRig(t, plan, "legal/review", domain.TaskStatusReady)

		require.ErrorIs(t, rig.forward("archive/file"), port.ErrTargetNodeNotFound)
		assert.NotEmpty(t, rig.tasks.pages)
		assert.Empty(t, rig.signalled())
	})
}

func TestForceForward_TargetOutsideTheOpenStep_NamesTheDepartmentInTheRefusal(t *testing.T) {
	plan := w13bBranchPlan(frfixDept("legal", "review"), w13bLegalOnly(), frfixDept("intake", "open"))
	cases := []struct {
		name   string
		target string
		detail string
	}{
		{"a department after the step", "archive/file", `department "archive" is not a branch of the open parallel step`},
		{"a department that is on no route", "intake/open", `department "intake" is not a branch of the open parallel step`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig, _ := w13bRoutedRig(t, plan, "legal/review", domain.TaskStatusReady)

			err := rig.forward(tc.target)

			require.ErrorIs(t, err, port.ErrTargetNodeNotFound)
			assert.ErrorContains(t, err, tc.detail)
			assert.Empty(t, rig.signalled())
		})
	}

	t.Run("a target the plan does not hold is refused without a department detail", func(t *testing.T) {
		rig, _ := w13bRoutedRig(t, plan, "legal/review", domain.TaskStatusReady)

		err := rig.forward("archive/typo")

		require.ErrorIs(t, err, port.ErrTargetNodeNotFound)
		assert.NotContains(t, err.Error(), "open parallel step")
	})

	t.Run("control: a branch of the open step is accepted", func(t *testing.T) {
		rig, _ := w13bRoutedRig(t, plan, "legal/review", domain.TaskStatusReady)

		require.NoError(t, rig.forward("finance/check"))
		rig.w13bRequireSignalled(t, "finance/check")
	})
}

// w13bBoundaryTimer is a stage timer that leads to a department, to steps, or both.
func w13bBoundaryTimer(target string, steps ...dsl.ExecutionStep) *dsl.BoundaryTimer {
	return &dsl.BoundaryTimer{Duration: "PT1H", TargetDept: target, Terminates: target == "", Steps: steps}
}

func w13bCollabJSON(t *testing.T, collab dsl.CompiledCollaboration) string {
	t.Helper()
	raw, err := json.Marshal(collab)
	require.NoError(t, err)
	return string(raw)
}

func TestForceForward_BranchReachesDepartmentsThroughItsBoundaries_AcceptsOnlyThose(t *testing.T) {
	cases := []struct {
		name     string
		legal    func(d *dsl.DepartmentDef)
		extra    []dsl.DepartmentDef
		target   string
		accepted bool
	}{
		{
			name: "a message boundary target",
			legal: func(d *dsl.DepartmentDef) {
				d.Stages[0].BoundaryMessage = &dsl.MessagePath{MessageName: "m", TargetDept: "late"}
			},
			extra:  []dsl.DepartmentDef{frfixDept("late", "chase")},
			target: "late/chase", accepted: true,
		},
		{
			name: "a department only a timer boundary's own steps run",
			legal: func(d *dsl.DepartmentDef) {
				d.Stages[0].BoundaryTimer = w13bBoundaryTimer("", dsl.ExecutionStep{Sequential: []string{"late"}})
			},
			extra:  []dsl.DepartmentDef{frfixDept("late", "chase")},
			target: "late/chase", accepted: true,
		},
		{
			name: "a department only a message boundary's own steps run",
			legal: func(d *dsl.DepartmentDef) {
				d.Stages[0].BoundaryMessage = &dsl.MessagePath{MessageName: "m", Terminates: true, Steps: []dsl.ExecutionStep{{Sequential: []string{"late"}}}}
			},
			extra:  []dsl.DepartmentDef{frfixDept("late", "chase")},
			target: "late/chase", accepted: true,
		},
		{
			name: "a department two boundaries away",
			legal: func(d *dsl.DepartmentDef) {
				d.Stages[0].BoundaryTimer = w13bBoundaryTimer("late")
			},
			extra: []dsl.DepartmentDef{
				w13bWithMessageBoundary(frfixDept("late", "chase"), "later"), frfixDept("later", "nudge"),
			},
			target: "later/nudge", accepted: true,
		},
		{
			name:   "control: a department no boundary leads to",
			legal:  func(d *dsl.DepartmentDef) { d.Stages[0].BoundaryTimer = w13bBoundaryTimer("late") },
			extra:  []dsl.DepartmentDef{frfixDept("late", "chase"), frfixDept("stray", "idle")},
			target: "stray/idle", accepted: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			legal := frfixDept("legal", "review")
			tc.legal(&legal)
			rig, _ := w13bRoutedRig(t, w13bBranchPlan(legal, w13bLegalOnly(), tc.extra...), "legal/review", domain.TaskStatusReady)

			err := rig.forward(tc.target)

			if tc.accepted {
				require.NoError(t, err)
				rig.w13bRequireSignalled(t, tc.target)
				return
			}
			require.ErrorIs(t, err, port.ErrTargetNodeNotFound)
			assert.Empty(t, rig.signalled())
		})
	}
}

func w13bWithMessageBoundary(d dsl.DepartmentDef, target string) dsl.DepartmentDef {
	d.Stages[0].BoundaryMessage = &dsl.MessagePath{MessageName: "m2", TargetDept: target}
	return d
}

func TestForceForward_BranchWalkOverUnknownAndRepeatedDepartments_StillFindsTheTarget(t *testing.T) {
	t.Run("a boundary to a department the plan does not define is skipped", func(t *testing.T) {
		legal := frfixDept("legal", "review")
		legal.Stages[0].BoundaryTimer = w13bBoundaryTimer("ghost")
		legal.Stages = append(legal.Stages, dsl.StageDef{Type: "userTask", NodeID: "sign", BoundaryMessage: &dsl.MessagePath{MessageName: "m", TargetDept: "late"}})
		plan := w13bBranchPlan(legal, w13bLegalOnly(), frfixDept("late", "chase"))

		rig, _ := w13bRoutedRig(t, plan, "legal/review", domain.TaskStatusReady)
		require.NoError(t, rig.forward("late/chase"), "the walk goes on to the next boundary after the unknown department")
		rig.w13bRequireSignalled(t, "late/chase")

		other, _ := w13bRoutedRig(t, plan, "legal/review", domain.TaskStatusReady)
		require.ErrorIs(t, other.forward("archive/file"), port.ErrTargetNodeNotFound, "control: the unknown department leads nowhere")
		assert.Empty(t, other.signalled())
	})

	t.Run("boundaries that loop back to a department already walked end the walk", func(t *testing.T) {
		legal := frfixDept("legal", "review")
		legal.Stages[0].BoundaryTimer = w13bBoundaryTimer("loopa")
		loopa := frfixDept("loopa", "a")
		loopa.Stages[0].BoundaryTimer = w13bBoundaryTimer("legal")
		loopa.Stages = append(loopa.Stages, dsl.StageDef{Type: "userTask", NodeID: "a2", BoundaryMessage: &dsl.MessagePath{MessageName: "m", TargetDept: "loopb"}})
		plan := w13bBranchPlan(legal, w13bLegalOnly(), loopa, frfixDept("loopb", "b"))

		beyond, _ := w13bRoutedRig(t, plan, "legal/review", domain.TaskStatusReady)
		require.NoError(t, beyond.forward("loopb/b"), "a department past the loop is still reached")
		beyond.w13bRequireSignalled(t, "loopb/b")

		outside, _ := w13bRoutedRig(t, plan, "legal/review", domain.TaskStatusReady)
		require.ErrorIs(t, outside.forward("archive/file"), port.ErrTargetNodeNotFound, "the loop ends and the department after the step is refused")
		assert.Empty(t, outside.signalled())
	})
}

func TestForceForward_BranchStepsInsideGatewaysAndCalls_AreWalkedLikeTheInterpreterRunsThem(t *testing.T) {
	exclusiveDeep := []dsl.ExecutionStep{
		{Sequential: []string{"legal"}},
		{Exclusive: []dsl.ExclusiveBranch{
			{Target: "esc", ConditionExpression: "true", Steps: []dsl.ExecutionStep{{Sequential: []string{"deep"}}}},
			{RevertToDept: "redo", ConditionExpression: "false"},
		}},
	}
	called := []dsl.ExecutionStep{
		{Sequential: []string{"legal"}},
		{CallPlan: &dsl.CallPlanStep{
			NodeID: "CA_1", Name: "CA_1", Plan: "sub",
			ErrorPaths:   []dsl.ErrorPath{{ErrorCode: "E", TargetDept: "rescue"}},
			TimerPaths:   []dsl.TimerPath{{Duration: "PT1H", Terminates: true, Steps: []dsl.ExecutionStep{{Sequential: []string{"chaser"}}}}},
			MessagePaths: []dsl.MessagePath{{MessageName: "m", TargetDept: "notify"}},
		}},
	}
	cases := []struct {
		name     string
		steps    []dsl.ExecutionStep
		target   string
		accepted bool
	}{
		{"a department run by the steps of an exclusive branch", exclusiveDeep, "deep/work", true},
		{"the target of that exclusive branch", exclusiveDeep, "esc/look", true},
		{"a department a call's error path leads to", called, "rescue/fix", true},
		{"a department a call's timer path runs", called, "chaser/nudge", true},
		{"a department a call's message path leads to", called, "notify/ping", true},
		{"the department an exclusive branch reverts to", exclusiveDeep, "redo/again", true},
		{"control: a department of no step of the branch", exclusiveDeep, "archive/file", false},
		{"control: a department of no call path", called, "archive/file", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan := w13bBranchPlan(frfixDept("legal", "review"), tc.steps,
				frfixDept("esc", "look"), frfixDept("deep", "work"), frfixDept("redo", "again"),
				frfixDept("rescue", "fix"), frfixDept("chaser", "nudge"), frfixDept("notify", "ping"))
			collab := dsl.CompiledCollaboration{
				SchemaVersion: dsl.CurrentSchemaVersion, MainPlan: "main", Plans: []*dsl.CompiledPlan{plan, {
					Name: "sub", Departments: []dsl.DepartmentDef{frfixDept("inner", "work")},
					Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"inner"}}}},
				}},
			}
			rig, defs := w13bRoutedRig(t, plan, "legal/review", domain.TaskStatusReady)
			defs.resp = publishedCompiledWorkflow(uuid.New(), rig.instance.WorkflowVersionID, w13bCollabJSON(t, collab))

			err := rig.forward(tc.target)

			if tc.accepted {
				require.NoError(t, err)
				rig.w13bRequireSignalled(t, tc.target)
				return
			}
			require.ErrorIs(t, err, port.ErrTargetNodeNotFound)
			assert.Empty(t, rig.signalled())
		})
	}
}
