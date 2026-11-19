package workflow

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

func w23bDepts(branches []failedBranch) []string {
	out := make([]string, 0, len(branches))
	for _, b := range branches {
		out = append(out, b.DeptID)
	}
	return out
}

func TestRemoveFailedBranch_Position_RemovesOnlyThatBranchAndLeavesTheCallersSlice(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		idx  int
		want []string
	}{
		{name: "first", idx: 0, want: []string{"b", "c"}},
		{name: "middle", idx: 1, want: []string{"a", "c"}},
		{name: "last", idx: 2, want: []string{"a", "b"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			input := []failedBranch{{DeptID: "a"}, {DeptID: "b"}, {DeptID: "c"}}
			got := removeFailedBranch(input, tc.idx)
			assert.Equal(t, tc.want, w23bDepts(got))
			assert.Equal(t, []string{"a", "b", "c"}, w23bDepts(input), "the caller's slice keeps every entry in its place")
		})
	}
}

func TestIndexOfFailedBranch_Lookup_ReturnsFirstMatchOrMinusOne(t *testing.T) {
	t.Parallel()
	failed := []failedBranch{{DeptID: "a"}, {DeptID: "b", Err: context.Canceled}, {DeptID: "b"}}
	cases := []struct {
		name   string
		failed []failedBranch
		dept   string
		want   int
	}{
		{name: "first entry", failed: failed, dept: "a", want: 0},
		{name: "duplicate gives the first match", failed: failed, dept: "b", want: 1},
		{name: "unknown department", failed: failed, dept: "zzz", want: -1},
		{name: "empty list", failed: nil, dept: "a", want: -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, indexOfFailedBranch(tc.failed, tc.dept))
		})
	}
}

func TestFindDepartment_IDLookup_ReturnsFirstMatchOrNil(t *testing.T) {
	t.Parallel()
	plan := &dsl.CompiledPlan{Name: "main", Departments: []dsl.DepartmentDef{
		{ID: "a", IAMDepartmentID: "first-a"},
		{ID: "b"},
		{ID: "a", IAMDepartmentID: "second-a"},
	}}
	cases := []struct {
		name string
		id   string
		want *dsl.DepartmentDef
	}{
		{name: "present", id: "b", want: &plan.Departments[1]},
		{name: "duplicate gives the first", id: "a", want: &plan.Departments[0]},
		{name: "absent", id: "zzz", want: nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := findDepartment(plan, tc.id)
			if tc.want == nil {
				assert.Nil(t, got)
				return
			}
			assert.Same(t, tc.want, got)
		})
	}
}

func TestFindPlan_NameLookup_ReturnsFirstMatchOrNil(t *testing.T) {
	t.Parallel()
	collab := &dsl.CompiledCollaboration{Plans: []*dsl.CompiledPlan{{Name: "main"}, {Name: "side"}, {Name: "main"}}}
	cases := []struct {
		name string
		plan string
		want *dsl.CompiledPlan
	}{
		{name: "present", plan: "side", want: collab.Plans[1]},
		{name: "duplicate gives the first", plan: "main", want: collab.Plans[0]},
		{name: "absent", plan: "zzz", want: nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := findPlan(collab, tc.plan)
			if tc.want == nil {
				assert.Nil(t, got)
				return
			}
			assert.Same(t, tc.want, got)
		})
	}
}

func TestRunDepartment_UnknownDepartment_FailsWithDepartmentNotFoundInPlan(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	var node domain.NodeKey
	var runErr, keyErr error
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		in := newInterpreter("tenant", "instance", "", nil, nil)
		plan := &dsl.CompiledPlan{Name: "main", Departments: []dsl.DepartmentDef{{ID: "a"}}}
		node, _, runErr = in.runDepartment(ctx, plan, "ghost")
		_, _, keyErr = in.runDepartmentAtKey(ctx, plan, "ghost", "ghost/prep")
		return nil
	})
	require.NoError(t, env.GetWorkflowError())

	assert.Empty(t, node)
	require.Error(t, runErr)
	assert.Equal(t, `workflow: department "ghost" not found in plan "main"`, runErr.Error())
	require.Error(t, keyErr)
	assert.Equal(t, `workflow: department "ghost" not found in plan "main"`, keyErr.Error())
}

func TestExecute_MainPlanMissing_FailsTheInstanceNamingThePlan(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	rec := &w23bRecorder{}
	env.RegisterActivityWithOptions(
		func(_ context.Context, in port.GetCompiledPlanInput) (port.GetCompiledPlanOutput, error) {
			rec.mu.Lock()
			rec.planReads = append(rec.planReads, in)
			rec.mu.Unlock()
			return port.GetCompiledPlanOutput{Collaboration: dsl.CompiledCollaboration{
				MainPlan: "missing", Plans: []*dsl.CompiledPlan{{Name: "other"}}, SchemaVersion: dsl.CurrentSchemaVersion,
			}}, nil
		},
		activity.RegisterOptions{Name: port.ActivityGetCompiledPlan},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, in port.UpdateInstanceStatusInput) error {
			rec.mu.Lock()
			defer rec.mu.Unlock()
			rec.statuses = append(rec.statuses, in)
			return nil
		},
		activity.RegisterOptions{Name: port.ActivityUpdateInstanceStatus},
	)

	env.ExecuteWorkflow(Execute, ExecuteInput{TenantID: "tenant-7", InstanceID: "instance-7", VersionID: "version-7"})

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	assert.ErrorContains(t, err, `main plan "missing" not found in compiled collaboration`)
	require.Len(t, rec.planReads, 1)
	assert.Equal(t, "tenant-7", rec.planReads[0].TenantID)
	assert.Equal(t, "version-7", rec.planReads[0].VersionID)
	require.Len(t, rec.statuses, 1)
	assert.Equal(t, domain.InstanceStatusFailed, rec.statuses[0].Status)
	assert.Equal(t, "tenant-7", rec.statuses[0].TenantID)
	assert.Equal(t, "instance-7", rec.statuses[0].InstanceID)
}

func TestApplyIOMapping_NoInputsAndUnparseableContext_ReturnsTheContextUntouched(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		mapping *dsl.IOMapping
	}{
		{name: "nil mapping", mapping: nil},
		{name: "zero inputs", mapping: &dsl.IOMapping{}},
	}
	for _, tc := range cases {
		for _, raw := range []string{"{", "[1,2]", "not json at all"} {
			t.Run(tc.name+" "+raw, func(t *testing.T) {
				t.Parallel()
				got, err := applyIOMapping(raw, tc.mapping)
				require.NoError(t, err)
				assert.Equal(t, raw, got)
			})
		}
	}
}

func TestApplyIOMapping_UnparseableContextWithInputs_ReturnsTheWrappedParseError(t *testing.T) {
	t.Parallel()
	mapping := &dsl.IOMapping{Inputs: []dsl.IOVar{{Source: "=a", Target: "b"}}}
	for _, raw := range []string{"{", "[1,2]"} {
		t.Run(raw, func(t *testing.T) {
			t.Parallel()
			got, err := applyIOMapping(raw, mapping)
			require.Error(t, err)
			assert.Empty(t, got)
			assert.ErrorContains(t, err, "workflow: parsing context_json for io_mapping: ")
			var syntaxErr *json.SyntaxError
			var typeErr *json.UnmarshalTypeError
			assert.True(t, errorsAs(err, &syntaxErr) || errorsAs(err, &typeErr), "the JSON error is wrapped, not replaced")
		})
	}
}

func errorsAs[T error](err error, target *T) bool {
	for err != nil {
		if e, ok := err.(T); ok { //nolint:errorlint // walks the chain by hand to keep the helper generic
			*target = e
			return true
		}
		u, ok := err.(interface{ Unwrap() error }) //nolint:errorlint // same
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

func TestNodeHistoryPopTo_TargetPresentTwice_PopsFromTheLastOccurrence(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		pushed     []domain.NodeKey
		target     domain.NodeKey
		wantPopped []domain.NodeKey
		wantStack  []domain.NodeKey
	}{
		{
			name:       "target twice in the middle",
			pushed:     []domain.NodeKey{"a/1", "a/2", "a/3", "a/2", "a/4"},
			target:     "a/2",
			wantPopped: []domain.NodeKey{"a/2", "a/4"},
			wantStack:  []domain.NodeKey{"a/1", "a/2", "a/3"},
		},
		{
			name:       "target is both bottom and top",
			pushed:     []domain.NodeKey{"a/1", "a/2", "a/1"},
			target:     "a/1",
			wantPopped: []domain.NodeKey{"a/1"},
			wantStack:  []domain.NodeKey{"a/1", "a/2"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newNodeHistory()
			for _, k := range tc.pushed {
				h.Push(k)
			}
			assert.Equal(t, tc.wantPopped, h.PopTo(tc.target))
			assert.Equal(t, tc.wantStack, h.stack)
		})
	}
}

// w23bReceived is what one waiter of a message buffer saw.
type w23bReceived struct {
	node domain.NodeKey
	done bool
}

func TestMessageBufferSend_TwoBlockedWaiters_ReleasesTheOldestFirstAndBuffersNothing(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	var first, second w23bReceived
	var queueBefore, queueAfterOne, queueAfterTwo int
	var bufferedAfterOne, bufferedAfterTwo int
	var firstDoneAfterOne, secondDoneAfterOne bool
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		b := newMessageBuffer()
		wf.Go(ctx, func(gctx wf.Context) {
			first.node = b.Receive(gctx, "m")
			first.done = true
		})
		wf.Go(ctx, func(gctx wf.Context) {
			second.node = b.Receive(gctx, "m")
			second.done = true
		})
		if err := wf.Await(ctx, func() bool { return len(b.blocked["m"]) == 2 }); err != nil {
			return err
		}
		queueBefore = len(b.blocked["m"])

		b.Send(ctx, "m", "sender-a/send")
		queueAfterOne = len(b.blocked["m"])
		bufferedAfterOne = len(b.buf["m"])
		if err := wf.Await(ctx, func() bool { return first.done }); err != nil {
			return err
		}
		firstDoneAfterOne, secondDoneAfterOne = first.done, second.done

		b.Send(ctx, "m", "sender-b/send")
		queueAfterTwo = len(b.blocked["m"])
		bufferedAfterTwo = len(b.buf["m"])
		return wf.Await(ctx, func() bool { return second.done })
	})
	require.NoError(t, env.GetWorkflowError())

	assert.Equal(t, 2, queueBefore)
	assert.Equal(t, 1, queueAfterOne, "one send takes exactly one waiter off the queue")
	assert.Zero(t, bufferedAfterOne, "a send that found a waiter buffers nothing")
	assert.True(t, firstDoneAfterOne, "the oldest waiter is released by the first send")
	assert.False(t, secondDoneAfterOne, "the younger waiter keeps waiting")
	assert.Zero(t, queueAfterTwo)
	assert.Zero(t, bufferedAfterTwo)
	assert.Equal(t, w23bReceived{node: "sender-a/send", done: true}, first)
	assert.Equal(t, w23bReceived{node: "sender-b/send", done: true}, second)
}

func TestMessageBufferWaitChannel_CancelFunction_RemovesOnlyItsOwnWaiter(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	type result struct {
		cancelledGot, otherGot bool
		otherNode              domain.NodeKey
		queueLen, bufferedLen  int
	}
	var res result
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		b := newMessageBuffer()
		cancelled, cancelCancelled := b.waitChannel(ctx, "m")
		other, _ := b.waitChannel(ctx, "m")
		cancelCancelled()

		res.queueLen = len(b.blocked["m"])
		b.Send(ctx, "m", "sender/send")
		var n domain.NodeKey
		res.cancelledGot = cancelled.ReceiveAsync(&n)
		res.otherGot = other.ReceiveAsync(&res.otherNode)
		res.bufferedLen = len(b.buf["m"])
		return nil
	})
	require.NoError(t, env.GetWorkflowError())

	assert.Equal(t, 1, res.queueLen, "only the cancelled waiter left the queue")
	assert.False(t, res.cancelledGot, "the cancelled waiter's channel is not sent to")
	assert.True(t, res.otherGot, "the send went to the waiter that stayed")
	assert.Equal(t, domain.NodeKey("sender/send"), res.otherNode)
	assert.Zero(t, res.bufferedLen)
}

func TestMessageBufferWaitChannel_CancelAfterDeliveryOrOnBufferedHit_ChangesNothing(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	var afterDeliveryQueue, afterBufferedHitQueue int
	var nextWaiterGot bool
	var nextNode domain.NodeKey
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		b := newMessageBuffer()
		delivered, cancelDelivered := b.waitChannel(ctx, "m")
		waiting, _ := b.waitChannel(ctx, "m")
		b.Send(ctx, "m", "first/send")
		var n domain.NodeKey
		if !delivered.ReceiveAsync(&n) {
			return context.Canceled
		}
		cancelDelivered()
		afterDeliveryQueue = len(b.blocked["m"])

		b.Send(ctx, "other", "buffered/send")
		hit, cancelHit := b.waitChannel(ctx, "other")
		cancelHit()
		if !hit.ReceiveAsync(&n) {
			return context.Canceled
		}
		afterBufferedHitQueue = len(b.blocked["m"])

		b.Send(ctx, "m", "second/send")
		nextWaiterGot = waiting.ReceiveAsync(&nextNode)
		return nil
	})
	require.NoError(t, env.GetWorkflowError())

	assert.Equal(t, 1, afterDeliveryQueue, "cancelling a delivered waiter does not drop the one still waiting")
	assert.Equal(t, 1, afterBufferedHitQueue, "cancelling after a buffered hit does not touch the queue")
	assert.True(t, nextWaiterGot, "the waiting receiver still gets the next send")
	assert.Equal(t, domain.NodeKey("second/send"), nextNode)
}

func TestMessageBufferWaitChannel_CancelledWaiter_LaterSendIsBufferedNotSwallowed(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	var buffered []domain.NodeKey
	var received domain.NodeKey
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		b := newMessageBuffer()
		_, cancel := b.waitChannel(ctx, "m")
		cancel()
		b.Send(ctx, "m", "sender/send")
		buffered = append(buffered, b.buf["m"]...)
		received = b.Receive(ctx, "m")
		return nil
	})
	require.NoError(t, env.GetWorkflowError())

	assert.Equal(t, []domain.NodeKey{"sender/send"}, buffered)
	assert.Equal(t, domain.NodeKey("sender/send"), received)
}

func TestFindRedirectTarget_RedirectOutOfParallelBranches_LeavesThePlanUnchanged(t *testing.T) {
	t.Parallel()
	build := func() []dsl.ExecutionStep {
		inner := dsl.ExecutionStep{Exclusive: []dsl.ExclusiveBranch{
			{ConditionExpression: `x == "y"`, Steps: []dsl.ExecutionStep{{Sequential: []string{"Ops"}}}},
			{Steps: []dsl.ExecutionStep{{Sequential: []string{"Ops~2"}}}},
		}}
		return []dsl.ExecutionStep{
			{Sequential: []string{"Intake"}},
			{Parallel: []dsl.ParallelBranch{
				{DeptID: "Ops", Steps: []dsl.ExecutionStep{inner}},
				{DeptID: "Eng", Steps: []dsl.ExecutionStep{{Sequential: []string{"Eng", "Review"}}}},
			}},
			{Sequential: []string{"Close"}},
		}
	}
	cases := []struct {
		name string
		dept string
		want []dsl.ExecutionStep
	}{
		{
			name: "department named by a gateway-led branch",
			dept: "Ops",
			want: []dsl.ExecutionStep{{Sequential: []string{"Ops"}}, {Sequential: []string{"Close"}}},
		},
		{
			name: "department inside a branch's own steps",
			dept: "Review",
			want: []dsl.ExecutionStep{{Sequential: []string{"Review"}}, {Sequential: []string{"Close"}}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			steps := build()
			got, ok := findRedirectTarget(steps, tc.dept)
			require.True(t, ok)
			assert.Equal(t, tc.want, got)
			assert.Equal(t, build(), steps, "the plan's own steps are the same after the redirect")
		})
	}
}

// w23bPathNames lists the first department of each path, to compare a search
// order without caring about the rest of the path.
func w23bPathNames(paths [][]dsl.ExecutionStep) []string {
	names := make([]string, 0, len(paths))
	for _, p := range paths {
		if p[0].SubWorkflow != nil {
			names = append(names, "call:"+p[0].SubWorkflow.Name)
			continue
		}
		names = append(names, p[0].Sequential[0])
	}
	return names
}

func w23bPath(name string, rest ...string) []dsl.ExecutionStep {
	steps := []dsl.ExecutionStep{{Sequential: []string{name}}}
	for _, r := range rest {
		steps = append(steps, dsl.ExecutionStep{Sequential: []string{r}})
	}
	return steps
}

func w23bCallWithPaths(name string, inner []dsl.ExecutionStep) dsl.ExecutionStep {
	return dsl.ExecutionStep{SubWorkflow: &dsl.SubWorkflowStep{
		Name:         name,
		Plan:         dsl.ExecutionPlan{Steps: inner},
		ErrorPaths:   []dsl.ErrorPath{{Steps: w23bPath(name + "-error")}},
		TimerPaths:   []dsl.TimerPath{{Steps: w23bPath(name + "-timer")}},
		MessagePaths: []dsl.MessagePath{{Steps: w23bPath(name + "-message")}},
	}}
}

func TestBoundaryPaths_NestedPaths_AreCollectedInAFixedOrder(t *testing.T) {
	t.Parallel()
	inner := w23bCallWithPaths("inner", nil)
	outer := w23bCallWithPaths("outer", []dsl.ExecutionStep{inner})
	plan := &dsl.CompiledPlan{
		Departments: []dsl.DepartmentDef{
			{ID: "d1", Stages: []dsl.StageDef{
				{Type: "a", BoundaryTimer: &dsl.BoundaryTimer{Steps: w23bPath("stage-timer")}, BoundaryMessage: &dsl.MessagePath{Steps: w23bPath("stage-message")}},
				{Type: "b", BoundaryTimer: &dsl.BoundaryTimer{Steps: []dsl.ExecutionStep{w23bCallWithPaths("in-path", nil)}}},
			}},
			{ID: "d2", Stages: []dsl.StageDef{{Type: "c", BoundaryMessage: &dsl.MessagePath{Steps: w23bPath("second-message")}}}},
		},
		Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{
			{Parallel: []dsl.ParallelBranch{{DeptID: "p", Steps: []dsl.ExecutionStep{
				{Exclusive: []dsl.ExclusiveBranch{{Steps: []dsl.ExecutionStep{w23bCallWithPaths("in-branch", nil)}}}},
			}}}},
			outer,
		}},
	}

	got := w23bPathNames(boundaryPaths(plan))

	// The calls of the main steps come first (a parallel branch's exclusive
	// call, then the outer call: its own paths, then the call nested in it and
	// that call's paths). Each stage path follows in department order.
	assert.Equal(t, []string{
		"in-branch-error", "in-branch-timer", "in-branch-message",
		"outer-error", "outer-timer", "outer-message",
		"inner-error", "inner-timer", "inner-message",
		"stage-timer",
		"stage-message",
		"call:in-path",
		"in-path-error", "in-path-timer", "in-path-message",
		"second-message",
	}, got)
}

func TestRedirectSteps_DepartmentOnTwoPaths_UsesTheFirstPathInSearchOrder(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		plan *dsl.CompiledPlan
		dept string
		want []dsl.ExecutionStep
	}{
		{
			name: "stage timer path before the stage message path",
			plan: &dsl.CompiledPlan{
				Departments: []dsl.DepartmentDef{{ID: "d1", Stages: []dsl.StageDef{{
					Type:            "a",
					BoundaryMessage: &dsl.MessagePath{Steps: w23bPath("dup", "after-message")},
					BoundaryTimer:   &dsl.BoundaryTimer{Steps: w23bPath("dup", "after-timer")},
				}}}},
				Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"d1"}}}},
			},
			dept: "dup",
			want: w23bPath("dup", "after-timer"),
		},
		{
			name: "earlier department's stage path before a later one",
			plan: &dsl.CompiledPlan{
				Departments: []dsl.DepartmentDef{
					{ID: "d1", Stages: []dsl.StageDef{{Type: "a", BoundaryTimer: &dsl.BoundaryTimer{Steps: w23bPath("dup", "from-d1")}}}},
					{ID: "d2", Stages: []dsl.StageDef{{Type: "a", BoundaryTimer: &dsl.BoundaryTimer{Steps: w23bPath("dup", "from-d2")}}}},
				},
				Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"d1", "d2"}}}},
			},
			dept: "dup",
			want: w23bPath("dup", "from-d1"),
		},
		{
			name: "sub-workflow error path before its message path",
			plan: planOf([]dsl.ExecutionStep{{SubWorkflow: &dsl.SubWorkflowStep{
				Name:         "call",
				ErrorPaths:   []dsl.ErrorPath{{Steps: w23bPath("dup", "after-error")}},
				MessagePaths: []dsl.MessagePath{{Steps: w23bPath("dup", "after-message")}},
			}}}),
			dept: "dup",
			want: w23bPath("dup", "after-error"),
		},
		{
			name: "department only on a sub-workflow message path",
			plan: planOf([]dsl.ExecutionStep{{SubWorkflow: &dsl.SubWorkflowStep{
				Name:         "call",
				MessagePaths: []dsl.MessagePath{{Steps: w23bPath("only-here", "then")}},
			}}}),
			dept: "only-here",
			want: w23bPath("only-here", "then"),
		},
		{
			name: "department only on a boundary message path",
			plan: &dsl.CompiledPlan{
				Departments: []dsl.DepartmentDef{{ID: "d1", Stages: []dsl.StageDef{{Type: "a", BoundaryMessage: &dsl.MessagePath{Steps: w23bPath("only-here", "then")}}}}},
				Execution:   dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"d1"}}}},
			},
			dept: "only-here",
			want: w23bPath("only-here", "then"),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, redirectSteps(tc.plan, tc.dept))
		})
	}
}

// w23bRecorder records what the interpreter asked of each activity, with the
// tenant, instance and task ids it carried, and can refuse a call.
type w23bRecorder struct {
	mu          sync.Mutex
	planReads   []port.GetCompiledPlanInput
	created     []port.CreateTaskInput
	nodes       []port.UpdateInstanceNodesInput
	taskStatus  []port.UpdateTaskStatusInput
	completions []port.CompleteAssignmentInput
	statuses    []port.UpdateInstanceStatusInput
	slaBreaches []port.RecordSLABreachInput
	slaWarnings []port.RecordSLAWarningInput

	// notAllDone makes CompleteAssignment answer AllDone=false.
	notAllDone bool
	failCreate func(port.CreateTaskInput) error
	failNodes  func(port.UpdateInstanceNodesInput) error
}

func (r *w23bRecorder) createdKeys() []domain.NodeKey {
	r.mu.Lock()
	defer r.mu.Unlock()
	keys := make([]domain.NodeKey, 0, len(r.created))
	for _, in := range r.created {
		keys = append(keys, in.NodeKey)
	}
	return keys
}

func (r *w23bRecorder) statusesOf(taskID string) []domain.TaskStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []domain.TaskStatus
	for _, in := range r.taskStatus {
		if in.TaskID == taskID {
			out = append(out, in.Status)
		}
	}
	return out
}

// w23bRegister answers every activity an interpreter calls with recording
// fakes; a task's id is its node key.
func w23bRegister(env *testsuite.TestWorkflowEnvironment, r *w23bRecorder) {
	env.RegisterActivityWithOptions(
		func(_ context.Context, in port.CreateTaskInput) (port.CreateTaskOutput, error) {
			r.mu.Lock()
			r.created = append(r.created, in)
			fail := r.failCreate
			r.mu.Unlock()
			if fail != nil {
				if err := fail(in); err != nil {
					return port.CreateTaskOutput{}, err
				}
			}
			return port.CreateTaskOutput{TaskID: string(in.NodeKey)}, nil
		},
		activity.RegisterOptions{Name: port.ActivityCreateTask},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, in port.UpdateInstanceNodesInput) error {
			r.mu.Lock()
			r.nodes = append(r.nodes, in)
			fail := r.failNodes
			r.mu.Unlock()
			if fail != nil {
				return fail(in)
			}
			return nil
		},
		activity.RegisterOptions{Name: port.ActivityUpdateInstanceNodes},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, in port.CompleteAssignmentInput) (port.CompleteAssignmentOutput, error) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.completions = append(r.completions, in)
			return port.CompleteAssignmentOutput{AllDone: !r.notAllDone}, nil
		},
		activity.RegisterOptions{Name: port.ActivityCompleteAssignment},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, in port.UpdateTaskStatusInput) error {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.taskStatus = append(r.taskStatus, in)
			return nil
		},
		activity.RegisterOptions{Name: port.ActivityUpdateTaskStatus},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, in port.UpdateInstanceStatusInput) error {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.statuses = append(r.statuses, in)
			return nil
		},
		activity.RegisterOptions{Name: port.ActivityUpdateInstanceStatus},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, in port.RecordSLABreachInput) error {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.slaBreaches = append(r.slaBreaches, in)
			return nil
		},
		activity.RegisterOptions{Name: port.ActivityRecordSLABreach},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, in port.RecordSLAWarningInput) error {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.slaWarnings = append(r.slaWarnings, in)
			return nil
		},
		activity.RegisterOptions{Name: port.ActivityRecordSLAWarning},
	)
}

var w23bStart = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
