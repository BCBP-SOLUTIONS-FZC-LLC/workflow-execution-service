package workflow

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// The ISO 8601 period parser accepts what fits in an int64 of nanoseconds and
// refuses everything past it with "is too long": a single unit, a unit count of
// twenty digits, and a sum of units that crosses the limit.
func TestParseISODuration_AtTheInt64Limit(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		want    time.Duration
		tooLong bool
	}{
		{name: "the largest week count fits", in: "P15250W", want: 15250 * 7 * 24 * time.Hour},
		{name: "the largest day count fits", in: "P106751D", want: 106751 * 24 * time.Hour},
		{name: "one week past the limit", in: "P15251W", tooLong: true},
		{name: "one day past the limit", in: "P106752D", tooLong: true},
		{name: "a sum of units past the limit", in: "P106751DT24H", tooLong: true},
		{name: "a twenty digit count", in: "P99999999999999999999D", tooLong: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseISODuration(tc.in)
			if tc.tooLong {
				require.Error(t, err)
				assert.Contains(t, err.Error(), fmt.Sprintf("ISO 8601 period %q is too long", tc.in))
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// matchErrorPath prefers an exact code over any catch-all, takes the first of
// several catch-alls or of duplicate exact codes, and answers nil when nothing
// matches.
func TestMatchErrorPath_Selection(t *testing.T) {
	cases := []struct {
		name     string
		paths    []dsl.ErrorPath
		code     string
		wantDept string
		wantNil  bool
	}{
		{
			name:     "an exact code listed after a catch-all wins",
			paths:    []dsl.ErrorPath{{TargetDept: "catch"}, {ErrorCode: "E1", TargetDept: "exact"}},
			code:     "E1",
			wantDept: "exact",
		},
		{
			name:     "with only catch-alls the first wins",
			paths:    []dsl.ErrorPath{{TargetDept: "first-catch"}, {TargetDept: "second-catch"}},
			code:     "E9",
			wantDept: "first-catch",
		},
		{
			name:     "duplicate exact codes give the first",
			paths:    []dsl.ErrorPath{{ErrorCode: "E1", TargetDept: "first"}, {ErrorCode: "E1", TargetDept: "second"}},
			code:     "E1",
			wantDept: "first",
		},
		{
			name:     "a catch-all answers a code nobody lists",
			paths:    []dsl.ErrorPath{{ErrorCode: "E1", TargetDept: "exact"}, {TargetDept: "catch"}},
			code:     "E2",
			wantDept: "catch",
		},
		{name: "an empty list matches nothing", paths: nil, code: "E1", wantNil: true},
		{
			name:    "no match and no catch-all",
			paths:   []dsl.ErrorPath{{ErrorCode: "E1", TargetDept: "exact"}},
			code:    "E2",
			wantNil: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := matchErrorPath(tc.paths, tc.code)
			if tc.wantNil {
				assert.Nil(t, got)
				return
			}
			require.NotNil(t, got)
			assert.Equal(t, tc.wantDept, got.TargetDept)
		})
	}
}

// A field that is absent or null equals no literal, so only "!=" matches it; a
// number literal does not equal the same digits as text, and a bool literal does
// not equal the text "true".
func TestEvaluateCondition_AbsentNullAndMismatchedKinds(t *testing.T) {
	cases := []struct {
		name       string
		expr       string
		resultJSON string
		want       bool
	}{
		{name: "absent field with !=", expr: `decision != "approved"`, resultJSON: `{"other":1}`, want: true},
		{name: "absent field with ==", expr: `decision == "approved"`, resultJSON: `{"other":1}`, want: false},
		{name: "null field with !=", expr: `decision != "approved"`, resultJSON: `{"decision":null}`, want: true},
		{name: "null field with ==", expr: `decision == "approved"`, resultJSON: `{"decision":null}`, want: false},
		{name: "null field with a bool literal and !=", expr: `approved != true`, resultJSON: `{"approved":null}`, want: true},
		{name: "number literal against a string value", expr: `amount == 5`, resultJSON: `{"amount":"5"}`, want: false},
		{name: "number literal against a string value with !=", expr: `amount != 5`, resultJSON: `{"amount":"5"}`, want: true},
		{name: "bool literal against the string true", expr: `approved == true`, resultJSON: `{"approved":"true"}`, want: false},
		{name: "bool literal against the string true with !=", expr: `approved != true`, resultJSON: `{"approved":"true"}`, want: true},
		{name: "leading marker with an absent field and !=", expr: `= decision != "approved"`, resultJSON: `{}`, want: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := evaluateCondition(tc.expr, tc.resultJSON)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// An expression outside the grammar is refused with the dsl package's message
// behind "workflow: ", and the cause stays reachable.
func TestEvaluateCondition_OutsideTheGrammarNamesTheInvalidExpression(t *testing.T) {
	cases := []struct {
		name string
		expr string
	}{
		{name: "empty", expr: ""},
		{name: "no operator", expr: `decision`},
		{name: "unquoted text", expr: `decision == approved`},
		{name: "a bad escape", expr: `decision == "a\nb"`},
		{name: "an operator outside the grammar", expr: `a > 1`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := evaluateCondition(tc.expr, `{"a":2}`)
			require.Error(t, err)
			assert.ErrorIs(t, err, dsl.ErrInvalidCondition)
			assert.Contains(t, err.Error(), fmt.Sprintf("workflow: dsl: invalid condition expression %q", tc.expr))
		})
	}
}

// selectBranch hands the same error back, and takes no branch.
func TestSelectBranch_AnInvalidConditionIsReturnedAndNoBranchIsTaken(t *testing.T) {
	branches := []dsl.ExclusiveBranch{
		{ConditionExpression: `a > 1`, Target: "first"},
		{Target: "else"},
	}
	got, err := selectBranch(branches, `{"a":2}`)
	require.Error(t, err)
	assert.ErrorIs(t, err, dsl.ErrInvalidCondition)
	assert.Contains(t, err.Error(), `workflow: dsl: invalid condition expression "a > 1"`)
	assert.Nil(t, got)
}

// An invalid condition reaches the instance: runExclusive hands the refusal back
// before any branch or department runs.
func TestRunExclusive_AnInvalidConditionFailsTheStepWithoutRunningABranch(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var err error
	var out stepOutcome
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		in := newInterpreter("tenant", "instance", "", nil, nil)
		out, err = in.runExclusive(ctx, &dsl.CompiledPlan{Name: "main"}, []dsl.ExclusiveBranch{
			{ConditionExpression: `a > 1`, Target: "never"},
			{Target: "never-either"},
		}, `{"a":2}`, nil)
		return nil
	})
	require.NoError(t, env.GetWorkflowError())
	require.Error(t, err)
	assert.ErrorIs(t, err, dsl.ErrInvalidCondition)
	assert.Equal(t, stepOutcome{}, out)
}

// An implicit else is used only when no conditional branch matches, even when it
// is listed first; a second else is ignored.
func TestSelectBranch_ImplicitElseRules(t *testing.T) {
	cases := []struct {
		name       string
		branches   []dsl.ExclusiveBranch
		resultJSON string
		wantTarget string
	}{
		{
			name: "an else listed first does not beat a later match",
			branches: []dsl.ExclusiveBranch{
				{Target: "else"},
				{ConditionExpression: `decision == "approved"`, Target: "conditional"},
			},
			resultJSON: `{"decision":"approved"}`,
			wantTarget: "conditional",
		},
		{
			name: "an else listed first is used when nothing matches",
			branches: []dsl.ExclusiveBranch{
				{Target: "else"},
				{ConditionExpression: `decision == "approved"`, Target: "conditional"},
			},
			resultJSON: `{"decision":"rejected"}`,
			wantTarget: "else",
		},
		{
			name: "a second else is ignored",
			branches: []dsl.ExclusiveBranch{
				{Target: "first-else"},
				{ConditionExpression: `decision == "approved"`, Target: "conditional"},
				{Target: "second-else"},
			},
			resultJSON: `{"decision":"rejected"}`,
			wantTarget: "first-else",
		},
		{
			name: "the first of several matching branches wins in plan order",
			branches: []dsl.ExclusiveBranch{
				{ConditionExpression: `decision != "x"`, Target: "plan-first"},
				{ConditionExpression: `decision == "approved"`, Target: "plan-second"},
			},
			resultJSON: `{"decision":"approved"}`,
			wantTarget: "plan-first",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := selectBranch(tc.branches, tc.resultJSON)
			require.NoError(t, err)
			require.NotNil(t, got)
			assert.Equal(t, tc.wantTarget, got.Target)
		})
	}
}

// A node key names its department before the first slash; a key with no slash
// is its own department; the empty key has none.
func TestDeptFromNodeKey_Forms(t *testing.T) {
	cases := []struct {
		key  domain.NodeKey
		want string
	}{
		{key: "dept/stage", want: "dept"},
		{key: "dept", want: "dept"},
		{key: "", want: ""},
		{key: "a/b/c", want: "a"},
	}
	for _, tc := range cases {
		t.Run(string(tc.key), func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, deptFromNodeKey(tc.key))
		})
	}
}

// currentPendingNodes lists every pending node key once per visit, sorted, and
// pendingNodesIn keeps only the keys whose department the given steps run.
func TestPendingNodes_AreSortedAndFilteredByTheStepsDepartments(t *testing.T) {
	in := &interpreter{pending: map[nodeVisitKey]wf.Channel{
		{Node: "zeta/last", Visit: 1}:    nil,
		{Node: "alpha/second", Visit: 1}: nil,
		{Node: "alpha/first", Visit: 2}:  nil,
		{Node: "mid/only", Visit: 1}:     nil,
	}}

	assert.Equal(t, []domain.NodeKey{"alpha/first", "alpha/second", "mid/only", "zeta/last"}, in.currentPendingNodes())

	steps := []dsl.ExecutionStep{{Sequential: []string{"zeta"}}, {Exclusive: []dsl.ExclusiveBranch{{Steps: []dsl.ExecutionStep{{Sequential: []string{"alpha"}}}}}}}
	assert.Equal(t, []domain.NodeKey{"alpha/first", "alpha/second", "zeta/last"}, in.pendingNodesIn(steps))
	assert.Empty(t, in.pendingNodesIn([]dsl.ExecutionStep{{Sequential: []string{"nobody"}}}))
	assert.Empty(t, (&interpreter{pending: map[nodeVisitKey]wf.Channel{}}).currentPendingNodes())
}

// stepsRun finds a department anywhere in the nested steps, and stepsAfterDept
// returns what runs after it: the rest of the same Sequential list, then the
// later steps; nothing for the last department or an absent one; and a copy of
// the rest of a Sequential list, never a slice of the plan.
func TestStepsRunAndStepsAfterDept(t *testing.T) {
	seq := dsl.ExecutionStep{Sequential: []string{"a", "b", "c"}}
	after := dsl.ExecutionStep{Sequential: []string{"z"}}

	t.Run("stepsRun reaches every nested body", func(t *testing.T) {
		t.Parallel()
		steps := []dsl.ExecutionStep{
			{Parallel: []dsl.ParallelBranch{{DeptID: "p", Steps: []dsl.ExecutionStep{{Sequential: []string{"in-parallel"}}}}}},
			{Exclusive: []dsl.ExclusiveBranch{{Steps: []dsl.ExecutionStep{{Sequential: []string{"in-exclusive"}}}}}},
			{SubWorkflow: &dsl.SubWorkflowStep{Plan: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"in-sub"}}}}}},
			{Sequential: []string{"top"}},
		}
		for _, dept := range []string{"in-parallel", "in-exclusive", "in-sub", "top"} {
			assert.True(t, stepsRun(steps, dept), dept)
		}
		assert.False(t, stepsRun(steps, "absent"))
		assert.False(t, stepsRun(nil, "top"))
	})

	t.Run("a department at the start of a Sequential list leaves the rest of it and the later steps", func(t *testing.T) {
		t.Parallel()
		got := stepsAfterDept([]dsl.ExecutionStep{seq, after}, "a")
		assert.Equal(t, []dsl.ExecutionStep{{Sequential: []string{"b", "c"}}, after}, got)
	})

	t.Run("a department in the middle", func(t *testing.T) {
		t.Parallel()
		got := stepsAfterDept([]dsl.ExecutionStep{seq, after}, "b")
		assert.Equal(t, []dsl.ExecutionStep{{Sequential: []string{"c"}}, after}, got)
	})

	t.Run("a department at the end leaves only the later steps", func(t *testing.T) {
		t.Parallel()
		got := stepsAfterDept([]dsl.ExecutionStep{seq, after}, "c")
		assert.Equal(t, []dsl.ExecutionStep{after}, got)
	})

	t.Run("the last department of the last step leaves nothing", func(t *testing.T) {
		t.Parallel()
		assert.Empty(t, stepsAfterDept([]dsl.ExecutionStep{seq}, "c"))
	})

	t.Run("an absent department leaves nothing", func(t *testing.T) {
		t.Parallel()
		assert.Empty(t, stepsAfterDept([]dsl.ExecutionStep{seq, after}, "absent"))
		assert.Empty(t, stepsAfterDept(nil, "a"))
	})

	t.Run("a department inside a Parallel branch is found from the branch steps", func(t *testing.T) {
		t.Parallel()
		inner := dsl.ExecutionStep{Sequential: []string{"x", "y"}}
		steps := []dsl.ExecutionStep{{Parallel: []dsl.ParallelBranch{{DeptID: "x", Steps: []dsl.ExecutionStep{inner}}}}, after}
		got := stepsAfterDept(steps, "x")
		assert.Equal(t, []dsl.ExecutionStep{{Sequential: []string{"y"}}, after}, got)
	})

	t.Run("the rest of a Sequential list is a copy, not an alias of the plan", func(t *testing.T) {
		t.Parallel()
		plan := []dsl.ExecutionStep{{Sequential: []string{"a", "b", "c"}}}
		got := stepsAfterDept(plan, "a")
		require.Len(t, got, 1)
		got[0].Sequential[0] = "changed"
		assert.Equal(t, []string{"a", "b", "c"}, plan[0].Sequential)
	})
}

// failedBranchFor finds the failed branch a force route names: the only branch
// for an empty target, then by branch department, then by a department the
// branch's steps run, then by the department of the last completed node, and -1
// when none fits.
func TestFailedBranchFor_Lookup(t *testing.T) {
	failed := []failedBranch{
		{DeptID: "billing", Steps: []dsl.ExecutionStep{{Sequential: []string{"billing", "audit"}}}, LastCompletedNode: "billing/prep"},
		{DeptID: "shipping", Steps: []dsl.ExecutionStep{{Sequential: []string{"shipping"}}}, LastCompletedNode: "packing/box"},
	}
	cases := []struct {
		name   string
		failed []failedBranch
		sig    adminSignal
		want   int
	}{
		{name: "an empty target with one failed branch", failed: failed[:1], sig: adminSignal{}, want: 0},
		{name: "an empty target with two failed branches names none", failed: failed, sig: adminSignal{}, want: -1},
		{name: "the exact department", failed: failed, sig: adminSignal{TargetDeptID: "shipping"}, want: 1},
		{name: "a department inside a branch's steps", failed: failed, sig: adminSignal{TargetDeptID: "audit"}, want: 0},
		{name: "the department of the last completed node", failed: failed, sig: adminSignal{TargetDeptID: "packing"}, want: 1},
		{name: "a department no branch holds", failed: failed, sig: adminSignal{TargetDeptID: "legal"}, want: -1},
		{name: "no failed branches", failed: nil, sig: adminSignal{TargetDeptID: "billing"}, want: -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, failedBranchFor(tc.failed, tc.sig))
		})
	}
}

// w23aStageEnv is a workflow test environment whose task activities answer in
// the way a stage needs: every task id is its node key, and every completion is
// the last one.
func w23aStageEnv(created *[]domain.NodeKey) *testsuite.TestWorkflowEnvironment {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	w23aRegisterStageActivities(env, created)
	return env
}

// A department started at or past its last stage runs no stage and returns an
// empty node; a stage with an empty result keeps the department's earlier one.
func TestRunDepartmentFrom_StartIndexAndEmptyResults(t *testing.T) {
	plan := &dsl.CompiledPlan{
		Name:        "main",
		Departments: []dsl.DepartmentDef{{ID: "dept", Stages: []dsl.StageDef{{Type: "prep", NodeID: "One"}, {Type: "prep", NodeID: "Two"}}}},
	}

	t.Run("a start index past the last stage runs nothing", func(t *testing.T) {
		var created []domain.NodeKey
		env := w23aStageEnv(&created)
		var node domain.NodeKey
		var result string
		var err error
		env.ExecuteWorkflow(func(ctx wf.Context) error {
			in := newInterpreter("tenant", "instance", "", nil, nil)
			node, result, err = in.runDepartmentFrom(ctx, plan, "dept", len(plan.Departments[0].Stages))
			return nil
		})
		require.NoError(t, env.GetWorkflowError())
		require.NoError(t, err)
		assert.Empty(t, node)
		assert.Empty(t, result)
		assert.Empty(t, created, "no task may be created")
	})

	t.Run("stageIndexAfter the last stage is past the end, so the department resumes with nothing to run", func(t *testing.T) {
		assert.Equal(t, 2, stageIndexAfter(&plan.Departments[0], "dept/Two"))
		assert.Equal(t, 1, stageIndexAfter(&plan.Departments[0], "dept/One"))
	})

	t.Run("a stage with an empty result keeps the earlier result", func(t *testing.T) {
		var created []domain.NodeKey
		env := w23aStageEnv(&created)
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow("stage-transition:instance", stageTransitionSignal{DeptID: "dept", NodeID: "One", ResultJSON: `{"first":true}`, VisitCount: 1})
		}, time.Millisecond)
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow("stage-transition:instance", stageTransitionSignal{DeptID: "dept", NodeID: "Two", ResultJSON: "", VisitCount: 1})
		}, time.Second)
		var node domain.NodeKey
		var result string
		var err error
		env.ExecuteWorkflow(func(ctx wf.Context) error {
			in := newInterpreter("tenant", "instance", "", nil, nil)
			admin := wf.NewBufferedChannel(ctx, 1)
			baseAdmin := wf.NewBufferedChannel(ctx, 1)
			wf.Go(ctx, func(gctx wf.Context) { in.runSignalRouter(gctx, admin, baseAdmin) })
			node, result, err = in.runDepartmentFrom(ctx, plan, "dept", 0)
			return nil
		})
		require.NoError(t, env.GetWorkflowError())
		require.NoError(t, err)
		assert.Equal(t, domain.NodeKey("dept/Two"), node)
		assert.Equal(t, `{"first":true}`, result)
		assert.Equal(t, []domain.NodeKey{"dept/One", "dept/Two"}, created)
	})
}

// runStepList hands the end of a path up to its caller, runSteps swallows it.
func TestRunSteps_SwallowsAPathEndThatRunStepListPassesUp(t *testing.T) {
	steps := []dsl.ExecutionStep{{Exclusive: []dsl.ExclusiveBranch{{Terminates: true}}}}

	var listErr, stepsErr error
	var listOut, stepsOut stepOutcome
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		in := newInterpreter("tenant", "instance", "", nil, nil)
		plan := &dsl.CompiledPlan{Name: "main"}
		listOut, listErr = in.runStepList(ctx, plan, steps, nil)
		stepsOut, stepsErr = in.runSteps(ctx, plan, steps, nil)
		return nil
	})
	require.NoError(t, env.GetWorkflowError())
	assert.True(t, errors.Is(listErr, errPathEnded), "runStepList = %v", listErr)
	assert.NoError(t, stepsErr)
	assert.False(t, listOut.Terminated)
	assert.False(t, stepsOut.Terminated)
}

// A failed Application error keeps its type and its non-retryable flag behind
// the wrapping, and a nil error stays nil.
func TestWrapActivityErr_KeepsTheCauseReachable(t *testing.T) {
	t.Run("nil stays nil", func(t *testing.T) {
		assert.NoError(t, wrapActivityErr("CreateTaskActivity", nil))
	})

	t.Run("the text names the activity and the cause", func(t *testing.T) {
		cause := errors.New("connection reset")
		got := wrapActivityErr("CreateTaskActivity", cause)
		require.Error(t, got)
		assert.Equal(t, "workflow: CreateTaskActivity: connection reset", got.Error())
		assert.ErrorIs(t, got, cause)
	})

	t.Run("an application error keeps its type and non-retryable flag", func(t *testing.T) {
		cause := temporal.NewNonRetryableApplicationError("version is not published", "DefinitionServiceClientError", nil)
		got := wrapActivityErr("GetCompiledPlanActivity", cause)
		var app *temporal.ApplicationError
		require.ErrorAs(t, got, &app)
		assert.Equal(t, "DefinitionServiceClientError", app.Type())
		assert.True(t, app.NonRetryable())
		assert.Equal(t, "workflow: GetCompiledPlanActivity: version is not published (type: DefinitionServiceClientError, retryable: false)", got.Error())
		assert.ErrorIs(t, got, cause)
	})

	t.Run("a retryable application error stays retryable", func(t *testing.T) {
		cause := temporal.NewApplicationError("busy", "VersionConflict")
		var app *temporal.ApplicationError
		require.ErrorAs(t, wrapActivityErr("DeferTaskActivity", cause), &app)
		assert.False(t, app.NonRetryable())
	})
}

func w23aRegisterStageActivities(env *testsuite.TestWorkflowEnvironment, created *[]domain.NodeKey) {
	env.RegisterActivityWithOptions(
		func(_ context.Context, in port.CreateTaskInput) (port.CreateTaskOutput, error) {
			*created = append(*created, in.NodeKey)
			return port.CreateTaskOutput{TaskID: string(in.NodeKey)}, nil
		},
		activity.RegisterOptions{Name: port.ActivityCreateTask},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, _ port.UpdateInstanceNodesInput) error { return nil },
		activity.RegisterOptions{Name: port.ActivityUpdateInstanceNodes},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, _ port.CompleteAssignmentInput) (port.CompleteAssignmentOutput, error) {
			return port.CompleteAssignmentOutput{AllDone: true}, nil
		},
		activity.RegisterOptions{Name: port.ActivityCompleteAssignment},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, _ port.UpdateTaskStatusInput) error { return nil },
		activity.RegisterOptions{Name: port.ActivityUpdateTaskStatus},
	)
}
