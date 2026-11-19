package workflow

import (
	"context"
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

func w23aExclusivePlan() *dsl.CompiledPlan {
	return &dsl.CompiledPlan{
		Name: "main",
		Departments: []dsl.DepartmentDef{
			{ID: "steps", Stages: []dsl.StageDef{{Type: "prep", NodeID: "S1"}}},
			{ID: "target", Stages: []dsl.StageDef{{Type: "prep", NodeID: "T1"}}},
		},
	}
}

// w23aExclusiveRun runs runExclusive over branches with the result given and
// completes the task of each (department, node) pair in completes, one a minute.
type w23aExclusiveResult struct {
	out     stepOutcome
	err     error
	created []domain.NodeKey
}

func w23aExclusiveRun(t *testing.T, branches []dsl.ExclusiveBranch, resultJSON string, completes ...[2]string) w23aExclusiveResult {
	t.Helper()
	var res w23aExclusiveResult
	env := w23aStageEnv(&res.created)
	for i, done := range completes {
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow("stage-transition:instance", stageTransitionSignal{DeptID: done[0], NodeID: done[1], ResultJSON: "{}", VisitCount: 1})
		}, time.Duration(i+1)*time.Minute)
	}
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		in := newInterpreter("tenant", "instance", "", nil, nil)
		admin := wf.NewBufferedChannel(ctx, 1)
		baseAdmin := wf.NewBufferedChannel(ctx, 1)
		wf.Go(ctx, func(gctx wf.Context) { in.runSignalRouter(gctx, admin, baseAdmin) })
		res.out, res.err = in.runExclusive(ctx, w23aExclusivePlan(), branches, resultJSON, admin)
		return nil
	})
	require.NoError(t, env.GetWorkflowError())
	return res
}

// A winning branch with steps runs them; with Terminates set the path then ends,
// without it the gateway returns normally.
func TestRunExclusive_WinningBranchWithStepsRunsThemAndEndsThePathOnlyWhenItTerminates(t *testing.T) {
	cases := []struct {
		name       string
		terminates bool
	}{
		{name: "steps without terminates", terminates: false},
		{name: "steps with terminates", terminates: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			branches := []dsl.ExclusiveBranch{{
				ConditionExpression: `decision == "go"`, Terminates: tc.terminates,
				Steps: []dsl.ExecutionStep{{Sequential: []string{"steps"}}},
			}}

			res := w23aExclusiveRun(t, branches, `{"decision":"go"}`, [2]string{"steps", "S1"})

			assert.Equal(t, []domain.NodeKey{"steps/S1"}, res.created, "the branch's steps ran")
			assert.Equal(t, domain.NodeKey("steps/S1"), res.out.LastNode)
			assert.False(t, res.out.Terminated)
			if tc.terminates {
				assert.ErrorIs(t, res.err, errPathEnded)
			} else {
				assert.NoError(t, res.err)
			}
		})
	}
}

// A branch with neither steps, Terminates nor Target continues with the next
// step: nothing is created and the result handed in is handed on.
func TestRunExclusive_WinningBranchWithoutTargetContinuesWithTheNextStep(t *testing.T) {
	res := w23aExclusiveRun(t, []dsl.ExclusiveBranch{{ConditionExpression: `decision == "go"`}}, `{"decision":"go"}`)

	require.NoError(t, res.err)
	assert.Empty(t, res.created, "no department ran")
	assert.Equal(t, stepOutcome{LastResult: `{"decision":"go"}`}, res.out)
}

// A winning branch with a Target runs that department from its first stage.
func TestRunExclusive_WinningBranchWithATargetRunsThatDepartment(t *testing.T) {
	res := w23aExclusiveRun(t,
		[]dsl.ExclusiveBranch{{ConditionExpression: `decision == "go"`, Target: "target"}}, `{"decision":"go"}`,
		[2]string{"target", "T1"})

	require.NoError(t, res.err)
	assert.Equal(t, []domain.NodeKey{"target/T1"}, res.created)
	assert.Equal(t, domain.NodeKey("target/T1"), res.out.LastNode)
}

// A cancel that ends a nested step inside the winning branch comes back as a
// terminated outcome, not as the end of a path, even when the branch also has
// Terminates set.
func TestRunExclusive_ANestedTerminatedOutcomePassesThrough(t *testing.T) {
	var cancelled []port.CancelInstanceInput
	var mu sync.Mutex
	var created []domain.NodeKey
	env := w23aStageEnv(&created)
	env.RegisterActivityWithOptions(
		func(_ context.Context, in port.CancelInstanceInput) error {
			mu.Lock()
			defer mu.Unlock()
			cancelled = append(cancelled, in)
			return nil
		},
		activity.RegisterOptions{Name: port.ActivityCancelInstance},
	)

	branches := []dsl.ExclusiveBranch{{
		ConditionExpression: `decision == "go"`, Terminates: true,
		Steps: []dsl.ExecutionStep{{Parallel: []dsl.ParallelBranch{
			{DeptID: "steps", Steps: []dsl.ExecutionStep{{Sequential: []string{"steps"}}}},
		}}},
	}}
	var out stepOutcome
	var err error
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		in := newInterpreter("tenant-4", "instance-4", "", nil, nil)
		admin := wf.NewBufferedChannel(ctx, 1)
		admin.Send(ctx, adminSignalEnvelope{Kind: SignalInstanceCancel, Signal: adminSignal{AdminUserID: "admin-4", RecordVersion: 7}})
		out, err = in.runExclusive(ctx, w23aExclusivePlan(), branches, `{"decision":"go"}`, admin)
		return nil
	})

	require.NoError(t, env.GetWorkflowError())
	require.NoError(t, err)
	assert.True(t, out.Terminated)
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, cancelled, 1, "the instance is cancelled once")
	assert.Equal(t, "tenant-4", cancelled[0].TenantID)
	assert.Equal(t, "instance-4", cancelled[0].InstanceID)
	assert.Equal(t, "admin-4", cancelled[0].AdminUserID)
	assert.EqualValues(t, 7, cancelled[0].RecordVersion)
}

// w23aAnswerRun drives handleExternalAnswer for an instance in the given status
// that has a wait open at "Ops/Wait", and reports whether the wait was released,
// the variables afterwards and the warnings logged.
func w23aAnswerRun(t *testing.T, status domain.InstanceStatus, key string) (released bool, contextJSON string, warnings []string) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	logger := &w23aRecordingLogger{}
	suite.SetLogger(logger)
	env := suite.NewTestWorkflowEnvironment()
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		in := newInterpreter("tenant", "instance", `{"kept":"yes"}`, nil, nil)
		in.status = status
		ch, cancel := in.waitExternal(ctx, "Ops/Wait")
		defer cancel()
		in.handleExternalAnswer(ctx, externalAnswerSignal{Key: key, Variables: map[string]any{"leaked": "no"}})
		sel := wf.NewSelector(ctx)
		sel.AddReceive(ch, func(wf.ReceiveChannel, bool) {})
		released = sel.HasPending()
		contextJSON = in.contextJSON
		return nil
	})
	require.NoError(t, env.GetWorkflowError())
	return released, contextJSON, logger.messages()
}

// An answer that arrives while the instance is PAUSED or already finished, or
// for a key nothing waits at, is dropped with a warning: the wait is not
// released and the variables are not merged. A RUNNING instance takes it.
func TestHandleExternalAnswer_IsDroppedUnlessTheInstanceIsRunningOrDegradedAndTheKeyWaits(t *testing.T) {
	cases := []struct {
		name         string
		status       domain.InstanceStatus
		key          string
		wantReleased bool
		wantWarn     string
	}{
		{name: "paused", status: domain.InstanceStatusPaused, key: "Ops/Wait", wantWarn: "dropping external answer"},
		{name: "completed", status: domain.InstanceStatusCompleted, key: "Ops/Wait", wantWarn: "dropping external answer"},
		{name: "failed", status: domain.InstanceStatusFailed, key: "Ops/Wait", wantWarn: "dropping external answer"},
		{name: "terminated", status: domain.InstanceStatusTerminated, key: "Ops/Wait", wantWarn: "dropping external answer"},
		{name: "a key nothing waits at", status: domain.InstanceStatusRunning, key: "Ops/Elsewhere", wantWarn: "dropping external answer: nothing is waiting there"},
		{name: "running with a waiting key", status: domain.InstanceStatusRunning, key: "Ops/Wait", wantReleased: true},
		{name: "degraded with a waiting key", status: domain.InstanceStatusDegraded, key: "Ops/Wait", wantReleased: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			released, contextJSON, warnings := w23aAnswerRun(t, tc.status, tc.key)

			assert.Equal(t, tc.wantReleased, released)
			if tc.wantReleased {
				assert.JSONEq(t, `{"kept":"yes","leaked":"no"}`, contextJSON)
				assert.Empty(t, warnings)
				return
			}
			assert.JSONEq(t, `{"kept":"yes"}`, contextJSON, "variables of a dropped answer are not merged")
			assert.Equal(t, []string{tc.wantWarn}, warnings)
		})
	}
}

// With no collaboration there is nothing to wait for; with receive tasks no send
// task feeds, each one is a key.
func TestExternalWaitKeys_NilCollaborationAndSeveralWaits(t *testing.T) {
	t.Run("a nil collaboration gives an empty set", func(t *testing.T) {
		got := externalWaitKeys(nil)
		assert.NotNil(t, got)
		assert.Empty(t, got)
	})

	t.Run("each unfed receive task is one key", func(t *testing.T) {
		collab := &dsl.CompiledCollaboration{
			MainPlan: "main",
			Plans: []*dsl.CompiledPlan{{
				Name: "main",
				Departments: []dsl.DepartmentDef{{ID: "Ops", Stages: []dsl.StageDef{
					{Type: "receive_task", NodeID: "Receive_B", Extras: map[string]string{"message": "b"}},
					{Type: "receive_task", NodeID: "Receive_A", Extras: map[string]string{"message": "a"}},
				}}},
				Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"Ops"}}}},
			}},
		}
		assert.Equal(t, map[string]bool{"Ops/Receive_A": true, "Ops/Receive_B": true}, externalWaitKeys(collab))
	})
}

// The waiting-external query lists the open waits' keys in sorted order, however
// they were opened.
func TestRegisterWaitingExternalQuery_ListsTheKeysSorted(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var got []string
	var queryErr error
	env.RegisterDelayedCallback(func() {
		value, err := env.QueryWorkflow(QueryWaitingExternal)
		if err != nil {
			queryErr = err
			return
		}
		queryErr = value.Get(&got)
	}, time.Minute)

	env.ExecuteWorkflow(func(ctx wf.Context) error {
		in := newInterpreter("tenant", "instance", "", nil, nil)
		if err := in.registerWaitingExternalQuery(ctx); err != nil {
			return err
		}
		for _, key := range []string{"Ops/zeta", "Ops/alpha", "Ops/mid#b1"} {
			_, _ = in.waitExternal(ctx, key)
		}
		return wf.Sleep(ctx, time.Hour)
	})

	require.NoError(t, env.GetWorkflowError())
	require.NoError(t, queryErr)
	assert.Equal(t, []string{"Ops/alpha", "Ops/mid#b1", "Ops/zeta"}, got)
}
