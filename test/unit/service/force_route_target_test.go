package service_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func frfixDept(id string, nodeIDs ...string) dsl.DepartmentDef {
	stages := make([]dsl.StageDef, len(nodeIDs))
	for i, n := range nodeIDs {
		stages[i] = dsl.StageDef{Type: "userTask", NodeID: n}
	}
	return dsl.DepartmentDef{ID: id, Label: id, Stages: stages}
}

func frfixPlanJSON(t *testing.T, schema int, plans ...*dsl.CompiledPlan) string {
	t.Helper()
	collab := dsl.CompiledCollaboration{SchemaVersion: schema, MainPlan: plans[0].Name, Plans: plans}
	b, err := json.Marshal(collab)
	require.NoError(t, err)
	return string(b)
}

// frfixParallelMain is a plan whose main route runs "intake", then a parallel
// step of the "legal" and "finance" branches, then "archive".
func frfixParallelMain() *dsl.CompiledPlan {
	return &dsl.CompiledPlan{
		Name: "main", TaskQueue: "wf-queue-default",
		Departments: []dsl.DepartmentDef{
			frfixDept("intake", "open"), frfixDept("legal", "review", "sign"),
			frfixDept("finance", "check"), frfixDept("archive", "file"),
			frfixDept("ops", "run"), frfixDept("risk", "score"),
		},
		Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{
			{Sequential: []string{"intake"}},
			{Parallel: []dsl.ParallelBranch{
				{DeptID: "legal", Steps: []dsl.ExecutionStep{{Sequential: []string{"legal"}}}},
				{DeptID: "finance", Steps: []dsl.ExecutionStep{{Sequential: []string{"finance"}}}},
			}},
			{Sequential: []string{"archive"}},
			{Parallel: []dsl.ParallelBranch{
				{DeptID: "ops", Steps: []dsl.ExecutionStep{{Sequential: []string{"ops"}}}},
				{DeptID: "risk", Steps: []dsl.ExecutionStep{{Sequential: []string{"risk"}}}},
			}},
		}},
	}
}

type frfixRig struct {
	svc interface {
		ForceForward(ctx context.Context, tenantID, instanceID, actorUserID uuid.UUID, targetNodeKey string, recordVersion int64) error
		ForceBack(ctx context.Context, tenantID, instanceID, actorUserID uuid.UUID, targetNodeKey string, recordVersion int64) error
	}
	tenantID uuid.UUID
	inst     *domain.Instance
	signals  func() int
}

func newFrfixRig(t *testing.T, status domain.InstanceStatus, planJSON string, tasks ...*domain.Task) *frfixRig {
	t.Helper()
	svc, instances, taskRepo, _, _, temporal, definitions, _ := newInstanceServiceHarness()
	tenantID := uuid.New()
	inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: status, RecordVersion: 1, TemporalWorkflowID: "tenant:biz"}
	instances.byID[inst.ID] = inst
	definitions.resp = publishedCompiledWorkflow(uuid.New(), uuid.New(), planJSON)
	for _, task := range tasks {
		task.ID, task.TenantID, task.WorkflowInstanceID = uuid.New(), tenantID, inst.ID
		taskRepo.byID[task.ID] = task
	}
	return &frfixRig{svc: svc, tenantID: tenantID, inst: inst, signals: func() int { return len(temporal.signals) }}
}

func (r *frfixRig) forward(target string) error {
	return r.svc.ForceForward(context.Background(), r.tenantID, r.inst.ID, uuid.New(), target, 1)
}

func (r *frfixRig) back(target string) error {
	return r.svc.ForceBack(context.Background(), r.tenantID, r.inst.ID, uuid.New(), target, 1)
}

func frfixTask(nodeKey string, status domain.TaskStatus) *domain.Task {
	return &domain.Task{NodeKey: nodeKey, Status: status}
}

func TestForceForward_NodeOnlyInAPlanTheMainPlanNeverReaches_IsRefusedBeforeTheSignal(t *testing.T) {
	main := &dsl.CompiledPlan{Name: "main", Departments: []dsl.DepartmentDef{frfixDept("finance", "settlement")}}
	orphan := &dsl.CompiledPlan{Name: "orphan", Departments: []dsl.DepartmentDef{frfixDept("legal", "review")}}
	rig := newFrfixRig(t, domain.InstanceStatusRunning, frfixPlanJSON(t, dsl.CurrentSchemaVersion, main, orphan))

	err := rig.forward("legal/review")

	require.ErrorIs(t, err, port.ErrTargetNodeNotFound)
	assert.Zero(t, rig.signals())
	require.NoError(t, rig.forward("finance/settlement"), "control: a node of the main plan is still accepted")
	assert.Equal(t, 1, rig.signals())
}

func TestForceForward_NodeOfACalledPlan_IsAcceptedOnceExpanded(t *testing.T) {
	main := &dsl.CompiledPlan{
		Name: "main", Departments: []dsl.DepartmentDef{frfixDept("finance", "settlement")},
		Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{
			{Sequential: []string{"finance"}},
			{CallPlan: &dsl.CallPlanStep{NodeID: "CA_1", Name: "CA_1", Plan: "sub"}},
		}},
	}
	sub := &dsl.CompiledPlan{
		Name: "sub", Departments: []dsl.DepartmentDef{frfixDept("legal", "review")},
		Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"legal"}}}},
	}
	rig := newFrfixRig(t, domain.InstanceStatusRunning, frfixPlanJSON(t, dsl.CurrentSchemaVersion, main, sub))

	require.NoError(t, rig.forward("CA_1::legal/review"))
	assert.Equal(t, 1, rig.signals())
	require.ErrorIs(t, rig.forward("legal/review"), port.ErrTargetNodeNotFound, "the called plan's own name for the node is not a node of the expanded plan")
}

func TestForceForward_StageOfAnotherDepartment_IsRefusedBeforeTheSignal(t *testing.T) {
	rig := newFrfixRig(t, domain.InstanceStatusRunning, frfixPlanJSON(t, dsl.CurrentSchemaVersion, frfixParallelMain()))

	err := rig.forward("finance/review")

	require.ErrorIs(t, err, port.ErrTargetNodeNotFound)
	assert.Zero(t, rig.signals())
}

func TestForceForward_InsideAParallelStep_TargetMustBeABranchOfThatStep(t *testing.T) {
	plan := frfixPlanJSON(t, dsl.CurrentSchemaVersion, frfixParallelMain())
	running := []*domain.Task{frfixTask("legal/review", domain.TaskStatusReady)}

	cases := []struct {
		name    string
		target  string
		refused bool
	}{
		{"a stage of the running branch", "legal/sign", false},
		{"a branch of the step with no open task", "finance/check", false},
		{"a node after the parallel step", "archive/file", true},
		{"a node before the parallel step", "intake/open", true},
		{"a branch of another parallel step", "ops/run", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := newFrfixRig(t, domain.InstanceStatusRunning, plan, frfixTask(running[0].NodeKey, running[0].Status))

			err := rig.forward(tc.target)

			if tc.refused {
				require.ErrorIs(t, err, port.ErrTargetNodeNotFound)
				assert.Zero(t, rig.signals())
				return
			}
			require.NoError(t, err)
			assert.Equal(t, 1, rig.signals())
		})
	}
}

func TestForceForward_OutsideAParallelStep_AnyNodeOfThePlanIsAccepted(t *testing.T) {
	rig := newFrfixRig(t, domain.InstanceStatusRunning, frfixPlanJSON(t, dsl.CurrentSchemaVersion, frfixParallelMain()),
		frfixTask("intake/open", domain.TaskStatusReady), frfixTask("legal/review", domain.TaskStatusCompleted))

	require.NoError(t, rig.forward("archive/file"))
	assert.Equal(t, 1, rig.signals())
}

func TestForceForward_Degraded_AnyNodeOfThePlanIsAccepted(t *testing.T) {
	plan := frfixPlanJSON(t, dsl.CurrentSchemaVersion, frfixParallelMain())
	staleFailure := frfixTask("legal/review", domain.TaskStatusFailed)

	t.Run("a node of a branch with no failed task is accepted", func(t *testing.T) {
		rig := newFrfixRig(t, domain.InstanceStatusDegraded, plan, staleFailure)
		require.NoError(t, rig.forward("finance/check"))
		assert.Equal(t, 1, rig.signals())
	})
	t.Run("a node outside the parallel step is accepted", func(t *testing.T) {
		rig := newFrfixRig(t, domain.InstanceStatusDegraded, plan, staleFailure)
		require.NoError(t, rig.forward("archive/file"))
		assert.Equal(t, 1, rig.signals())
	})
	t.Run("a node that is not in the plan is refused", func(t *testing.T) {
		rig := newFrfixRig(t, domain.InstanceStatusDegraded, plan, staleFailure)
		require.ErrorIs(t, rig.forward("finance/typo"), port.ErrTargetNodeNotFound)
		assert.Zero(t, rig.signals())
	})
}

func TestForceBack_DegradedWithATarget_IsSignalledWithoutABranchCheck(t *testing.T) {
	rig := newFrfixRig(t, domain.InstanceStatusDegraded, frfixPlanJSON(t, dsl.CurrentSchemaVersion, frfixParallelMain()),
		frfixTask("legal/review", domain.TaskStatusFailed))

	require.NoError(t, rig.back("finance/check"))
	assert.Equal(t, 1, rig.signals())
}

// frfixRoutedMain is a plan whose open parallel step holds "legal" (open
// task) and "finance". Departments the interpreter reaches only through an
// exclusive gateway, a revert, a sub-workflow boundary or a stage boundary
// belong to the step by those routes.
func frfixRoutedMain() *dsl.CompiledPlan {
	legal := frfixDept("legal", "review")
	legal.Stages[0].BoundaryTimer = &dsl.BoundaryTimer{Duration: "PT1H", TargetDept: "stagelate"}
	return &dsl.CompiledPlan{
		Name: "main", TaskQueue: "wf-queue-default",
		Departments: []dsl.DepartmentDef{
			legal, frfixDept("finance", "check"), frfixDept("esc", "look"), frfixDept("redo", "again"),
			frfixDept("late", "chase"), frfixDept("stagelate", "nudge"), frfixDept("sub", "work"),
			frfixDept("archive", "file"), frfixDept("inner1", "a"), frfixDept("inner2", "b"),
		},
		Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{
			{Parallel: []dsl.ParallelBranch{
				{DeptID: "legal", Steps: []dsl.ExecutionStep{
					{Sequential: []string{"legal"}},
					{SubWorkflow: &dsl.SubWorkflowStep{
						Plan:       dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"sub"}}}},
						TimerPaths: []dsl.TimerPath{{Duration: "PT1H", TargetDept: "late"}},
						ErrorPaths: []dsl.ErrorPath{{Terminates: true, Steps: []dsl.ExecutionStep{{Parallel: []dsl.ParallelBranch{
							{DeptID: "inner1", Steps: []dsl.ExecutionStep{{Sequential: []string{"inner1"}}}},
							{DeptID: "inner2", Steps: []dsl.ExecutionStep{{Sequential: []string{"inner2"}}}},
						}}}}},
					}},
				}},
				{DeptID: "finance", Steps: []dsl.ExecutionStep{
					{Sequential: []string{"finance"}},
					{Exclusive: []dsl.ExclusiveBranch{
						{Target: "esc", ConditionExpression: "true"},
						{RevertToDept: "redo", ConditionExpression: "false"},
					}},
				}},
			}},
			{Sequential: []string{"archive"}},
		}},
	}
}

func TestForceForward_InsideAParallelStep_AcceptsDepartmentsTheInterpreterReachesFromTheBranch(t *testing.T) {
	plan := frfixPlanJSON(t, dsl.CurrentSchemaVersion, frfixRoutedMain())

	cases := []struct {
		name    string
		target  string
		open    string
		refused bool
	}{
		{"an exclusive branch target", "esc/look", "legal/review", false},
		{"a revert department", "redo/again", "legal/review", false},
		{"a sub-workflow boundary path target", "late/chase", "finance/check", false},
		{"a stage boundary target", "stagelate/nudge", "finance/check", false},
		{"a department inside the sub-workflow", "sub/work", "finance/check", false},
		{"a branch of a parallel step nested in a boundary path", "inner2/b", "inner1/a", false},
		{"a node after the step while a nested step is open", "archive/file", "inner1/a", true},
		{"a node after the step", "archive/file", "legal/review", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := newFrfixRig(t, domain.InstanceStatusRunning, plan, frfixTask(tc.open, domain.TaskStatusReady))

			err := rig.forward(tc.target)

			if tc.refused {
				require.ErrorIs(t, err, port.ErrTargetNodeNotFound)
				assert.Zero(t, rig.signals())
				return
			}
			require.NoError(t, err)
			assert.Equal(t, 1, rig.signals())
		})
	}
}
