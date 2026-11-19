package workflow

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

func w28aEntryPlan() *dsl.CompiledPlan {
	return &dsl.CompiledPlan{
		Name: "main",
		Departments: []dsl.DepartmentDef{
			{ID: "prev", Stages: []dsl.StageDef{{Type: "prep", NodeID: "P1"}}},
			{ID: "billing", Stages: []dsl.StageDef{{Type: "prep", NodeID: "X1"}, {Type: "prep", NodeID: "X2"}}},
			{ID: "typed", Stages: []dsl.StageDef{{Type: "review"}}},
			{ID: "empty"},
		},
	}
}

func TestDepartmentEntryNode_IsTheFirstStageOrTheFallback(t *testing.T) {
	plan := w28aEntryPlan()
	cases := []struct {
		name     string
		deptID   string
		fallback domain.NodeKey
		want     domain.NodeKey
	}{
		{name: "first stage by node id", deptID: "billing", fallback: "fallback/x", want: "billing/X1"},
		{name: "first stage by type when it has no node id", deptID: "typed", fallback: "fallback/x", want: "typed/review"},
		{name: "unknown department gives the fallback", deptID: "nope", fallback: "fallback/x", want: "fallback/x"},
		{name: "department without stages gives the fallback", deptID: "empty", fallback: "fallback/x", want: "fallback/x"},
		{name: "empty fallback is returned as it is", deptID: "empty", fallback: "", want: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, departmentEntryNode(plan, tc.deptID, tc.fallback))
		})
	}
}

func TestFirstNodeOf_IsTheEntryOfTheFirstSequentialDepartment(t *testing.T) {
	plan := w28aEntryPlan()
	cases := []struct {
		name  string
		steps []dsl.ExecutionStep
		want  domain.NodeKey
	}{
		{name: "no steps", steps: nil, want: ""},
		{
			name:  "the first department of the first sequential step",
			steps: []dsl.ExecutionStep{{Sequential: []string{"billing", "prev"}}},
			want:  "billing/X1",
		},
		{
			name: "a step that is not sequential is skipped",
			steps: []dsl.ExecutionStep{
				{Exclusive: []dsl.ExclusiveBranch{{ConditionExpression: `a == "b"`, Target: "prev"}}},
				{Sequential: []string{"prev"}},
			},
			want: "prev/P1",
		},
		{
			name:  "only steps that are not sequential",
			steps: []dsl.ExecutionStep{{Exclusive: []dsl.ExclusiveBranch{{ConditionExpression: `a == "b"`, Target: "prev"}}}},
			want:  "",
		},
		{name: "a first department without stages gives nothing", steps: []dsl.ExecutionStep{{Sequential: []string{"empty"}}}, want: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, firstNodeOf(plan, tc.steps))
		})
	}
}

func TestResumeNode_PicksTheNodeAFailedBranchReEntersAt(t *testing.T) {
	plan := w28aEntryPlan()
	twoDepts := []dsl.ExecutionStep{{Sequential: []string{"prev", "billing"}}}
	cases := []struct {
		name string
		fb   failedBranch
		want domain.NodeKey
	}{
		{
			name: "the stage after the last completed one",
			fb:   failedBranch{DeptID: "prev", Steps: twoDepts, LastCompletedNode: "billing/X1"},
			want: "billing/X2",
		},
		{
			name: "the first stage of the next department when the last one finished its department",
			fb:   failedBranch{DeptID: "prev", Steps: twoDepts, LastCompletedNode: "prev/P1"},
			want: "billing/X1",
		},
		{
			name: "the branch's first stage when nothing completed",
			fb:   failedBranch{DeptID: "prev", Steps: twoDepts},
			want: "prev/P1",
		},
		{
			name: "the last completed node when no department follows",
			fb: failedBranch{DeptID: "prev", LastCompletedNode: "prev/P1", Steps: []dsl.ExecutionStep{
				{Sequential: []string{"prev"}},
				{Exclusive: []dsl.ExclusiveBranch{{ConditionExpression: `a == "b"`, Target: "billing"}}},
			}},
			want: "prev/P1",
		},
		{
			name: "the last completed node when the department that follows has no stages",
			fb:   failedBranch{DeptID: "prev", LastCompletedNode: "prev/P1", Steps: []dsl.ExecutionStep{{Sequential: []string{"prev", "empty"}}}},
			want: "prev/P1",
		},
		{
			name: "the branch's first stage when the last completed node names an unknown department",
			fb:   failedBranch{DeptID: "prev", LastCompletedNode: "ghost/G1", Steps: twoDepts},
			want: "prev/P1",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, resumeNode(plan, tc.fb))
		})
	}
}

func TestTakeOverPause_OnlyASafetyNetPauseTakesAHeldPauseFromAnotherOwner(t *testing.T) {
	first := adminSignal{AdminUserID: "admin-1", Initiator: domain.InitiatorOOO, RecordVersion: 1}
	cases := []struct {
		name          string
		held          bool
		owner         string
		incoming      adminSignal
		wantTaken     bool
		wantOwner     string
		wantHeldAdmin string
	}{
		{
			name: "a safety net pause takes an out of office pause",
			held: true, owner: domain.InitiatorOOO,
			incoming:  adminSignal{AdminUserID: "admin-2", Initiator: domain.InitiatorSafetyNet, RecordVersion: 2},
			wantTaken: true, wantOwner: domain.InitiatorSafetyNet, wantHeldAdmin: "admin-2",
		},
		{
			name: "a safety net pause takes a tenant state pause",
			held: true, owner: domain.InitiatorTenantState,
			incoming:  adminSignal{AdminUserID: "admin-2", Initiator: domain.InitiatorSafetyNet, RecordVersion: 2},
			wantTaken: true, wantOwner: domain.InitiatorSafetyNet, wantHeldAdmin: "admin-2",
		},
		{
			name: "a pause owned by the safety net is never replaced by another safety net pause",
			held: true, owner: domain.InitiatorSafetyNet,
			incoming:  adminSignal{AdminUserID: "admin-2", Initiator: domain.InitiatorSafetyNet, RecordVersion: 2},
			wantTaken: false, wantOwner: domain.InitiatorSafetyNet, wantHeldAdmin: "admin-1",
		},
		{
			name: "a pause owned by the safety net is never replaced by an administrator",
			held: true, owner: domain.InitiatorSafetyNet,
			incoming:  adminSignal{AdminUserID: "admin-2", Initiator: domain.InitiatorAdmin, RecordVersion: 2},
			wantTaken: false, wantOwner: domain.InitiatorSafetyNet, wantHeldAdmin: "admin-1",
		},
		{
			name: "an out of office pause does not take a held pause",
			held: true, owner: domain.InitiatorTenantState,
			incoming:  adminSignal{AdminUserID: "admin-2", Initiator: domain.InitiatorOOO, RecordVersion: 2},
			wantTaken: false, wantOwner: domain.InitiatorTenantState, wantHeldAdmin: "admin-1",
		},
		{
			name: "with no pause held nothing is taken over",
			held: false, owner: "",
			incoming:  adminSignal{AdminUserID: "admin-2", Initiator: domain.InitiatorSafetyNet, RecordVersion: 2},
			wantTaken: false, wantOwner: "", wantHeldAdmin: "admin-1",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			in := &interpreter{pauseHeld: tc.held, pauseInitiator: tc.owner, pauseSignal: first}

			got := in.takeOverPause(tc.incoming)

			assert.Equal(t, tc.wantTaken, got)
			assert.Equal(t, tc.wantOwner, in.pauseInitiator)
			assert.Equal(t, tc.wantHeldAdmin, in.pauseSignal.AdminUserID)
			assert.Equal(t, tc.held, in.pauseHeld)
		})
	}
}
