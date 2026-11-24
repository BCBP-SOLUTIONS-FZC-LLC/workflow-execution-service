package workflow

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/wflclock"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

func w23bNewEnv(rec *w23bRecorder) *testsuite.TestWorkflowEnvironment {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetStartTime(w23bStart)
	w23bRegister(env, rec)
	return env
}

// w23bStartRouter runs the signal router of in, as Execute does.
func w23bStartRouter(ctx wf.Context, in *interpreter) wf.Channel {
	admin := wf.NewBufferedChannel(ctx, 8)
	baseAdmin := wf.NewBufferedChannel(ctx, 8)
	wf.Go(ctx, func(gctx wf.Context) { in.runSignalRouter(gctx, admin, baseAdmin) })
	return admin
}

// w23bAnswer delivers a stage result to the instance used by these tests. A
// result that arrives before its stage waits is held for it.
func w23bAnswer(env *testsuite.TestWorkflowEnvironment, sig stageTransitionSignal) {
	w23bAnswers(env, sig)
}

func w23bAnswers(env *testsuite.TestWorkflowEnvironment, sigs ...stageTransitionSignal) {
	sends := make([]func(), len(sigs))
	for i, sig := range sigs {
		sends[i] = func() { env.SignalWorkflow("stage-transition:instance", sig) }
	}
	wflclock.Together(env, time.Millisecond, sends...)
}

func w23bRefusal(text string) error {
	return temporal.NewNonRetryableApplicationError(text, "ValidationError", nil)
}

func TestRunStage_SendTask_BuffersTheMessageAndPushesHistoryWithoutAnActivity(t *testing.T) {
	rec := &w23bRecorder{}
	env := w23bNewEnv(rec)

	var key domain.NodeKey
	var stack []domain.NodeKey
	var buffered []domain.NodeKey
	var stageErr error
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		in := newInterpreter("tenant", "instance", "", nil, nil)
		key, _, stageErr = in.runStage(ctx, &dsl.CompiledPlan{Name: "main"}, "sales",
			&dsl.StageDef{Type: "send_task", Extras: map[string]string{"message": "order-ready"}})
		stack = append(stack, in.history.stack...)
		buffered = append(buffered, in.msgBuf.buf["order-ready"]...)
		return nil
	})
	require.NoError(t, env.GetWorkflowError())

	require.NoError(t, stageErr)
	assert.Equal(t, domain.NodeKey("sales/send_task"), key)
	assert.Equal(t, []domain.NodeKey{"sales/send_task"}, buffered)
	assert.Equal(t, []domain.NodeKey{"sales/send_task"}, stack)
	assert.Empty(t, rec.created, "a send task creates no task")
	assert.Empty(t, rec.nodes, "a send task records no node")
	assert.Empty(t, rec.taskStatus)
	assert.Empty(t, rec.completions)
}

func TestRunStage_ReceiveTask_RecordsItsNodeThenPushesHistoryOnceTheMessageArrives(t *testing.T) {
	rec := &w23bRecorder{}
	env := w23bNewEnv(rec)

	var historyWhileWaiting, historyAfter []domain.NodeKey
	var nodesWhileWaiting []port.UpdateInstanceNodesInput
	var key domain.NodeKey
	var stageErr error
	finished := false
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		in := newInterpreter("tenant", "instance", "", nil, nil)
		wf.Go(ctx, func(gctx wf.Context) {
			key, _, stageErr = in.runStage(gctx, &dsl.CompiledPlan{Name: "main"}, "shipping",
				&dsl.StageDef{Type: "receive_task", Extras: map[string]string{"message": "order-ready"}})
			finished = true
		})
		if err := wf.Await(ctx, func() bool { return len(in.msgBuf.blocked["order-ready"]) == 1 }); err != nil {
			return err
		}
		historyWhileWaiting = append(historyWhileWaiting, in.history.stack...)
		rec.mu.Lock()
		nodesWhileWaiting = append(nodesWhileWaiting, rec.nodes...)
		rec.mu.Unlock()

		in.msgBuf.Send(ctx, "order-ready", "sales/send_task")
		if err := wf.Await(ctx, func() bool { return finished }); err != nil {
			return err
		}
		historyAfter = append(historyAfter, in.history.stack...)
		return nil
	})
	require.NoError(t, env.GetWorkflowError())

	require.NoError(t, stageErr)
	assert.Equal(t, domain.NodeKey("shipping/receive_task"), key)
	assert.Empty(t, historyWhileWaiting, "history is pushed only after the message arrives")
	require.Len(t, nodesWhileWaiting, 1, "the node is recorded before the stage waits")
	assert.Equal(t, "tenant", nodesWhileWaiting[0].TenantID)
	assert.Equal(t, "instance", nodesWhileWaiting[0].InstanceID)
	assert.Equal(t, []domain.NodeKey{"shipping/receive_task"}, nodesWhileWaiting[0].NodeKeys)
	assert.Equal(t, []domain.NodeKey{"shipping/receive_task"}, historyAfter)
	assert.Empty(t, rec.created, "a receive task creates no task")
}

func TestRunStage_ReceiveTaskNodeWriteRefused_EndsTheStageWithThatError(t *testing.T) {
	rec := &w23bRecorder{failNodes: func(port.UpdateInstanceNodesInput) error { return w23bRefusal("node write refused") }}
	env := w23bNewEnv(rec)

	var key domain.NodeKey
	var stageErr error
	var stack []domain.NodeKey
	var waiters int
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		in := newInterpreter("tenant", "instance", "", nil, nil)
		key, _, stageErr = in.runStage(ctx, &dsl.CompiledPlan{Name: "main"}, "shipping",
			&dsl.StageDef{Type: "receive_task", Extras: map[string]string{"message": "order-ready"}})
		stack = append(stack, in.history.stack...)
		waiters = len(in.msgBuf.blocked["order-ready"])
		return nil
	})
	require.NoError(t, env.GetWorkflowError())

	require.Error(t, stageErr)
	var appErr *temporal.ApplicationError
	require.True(t, errors.As(stageErr, &appErr))
	assert.Equal(t, "ValidationError", appErr.Type())
	assert.ErrorContains(t, stageErr, "node write refused")
	assert.Equal(t, domain.NodeKey("shipping/receive_task"), key)
	assert.Empty(t, stack, "a stage that failed leaves no history entry")
	assert.Zero(t, waiters, "the stage never started waiting for the message")
	assert.Equal(t, 1, len(rec.nodes))
}

func TestRunStage_TaskStageOutcome_PushesHistoryOnlyWhenTheStageCompletes(t *testing.T) {
	cases := []struct {
		name        string
		answer      *stageTransitionSignal
		cancel      bool
		wantErr     func(error) bool
		wantHistory []domain.NodeKey
		wantStatus  []domain.TaskStatus
		wantComplet int
	}{
		{
			name:        "completed",
			answer:      &stageTransitionSignal{DeptID: "sales", NodeID: "n1", UserID: "user-1", ResultJSON: `{"ok":true}`, RecordVersion: 3, VisitCount: 1},
			wantErr:     func(err error) bool { return err == nil },
			wantHistory: []domain.NodeKey{"sales/n1"},
			wantStatus:  []domain.TaskStatus{domain.TaskStatusCompleted},
			wantComplet: 1,
		},
		{
			name:    "failed by the connector",
			answer:  &stageTransitionSignal{DeptID: "sales", NodeID: "n1", Failed: true, Reason: "timeout", RecordVersion: 4, VisitCount: 1},
			wantErr: func(err error) bool { var f *stageFailedError; return errors.As(err, &f) && f.Reason == "timeout" },
			wantStatus: []domain.TaskStatus{
				domain.TaskStatusFailed,
			},
		},
		{
			name:       "abandoned by a cancelled run",
			cancel:     true,
			wantErr:    func(err error) bool { return errors.Is(err, errStageAbandoned) },
			wantStatus: []domain.TaskStatus{domain.TaskStatusSuperseded},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &w23bRecorder{}
			env := w23bNewEnv(rec)
			if tc.answer != nil {
				w23bAnswer(env, *tc.answer)
			}

			var key domain.NodeKey
			var stageErr error
			var stack []domain.NodeKey
			env.ExecuteWorkflow(func(ctx wf.Context) error {
				in := newInterpreter("tenant", "instance", "", nil, nil)
				w23bStartRouter(ctx, in)
				runCtx, cancelRun := wf.WithCancel(ctx)
				if tc.cancel {
					wf.Go(ctx, func(gctx wf.Context) {
						_ = wf.Await(gctx, func() bool { return len(in.pending) > 0 })
						cancelRun()
					})
				}
				key, _, stageErr = in.runStage(runCtx, &dsl.CompiledPlan{Name: "main"}, "sales", &dsl.StageDef{Type: "approve", NodeID: "n1"})
				stack = append(stack, in.history.stack...)
				return nil
			})
			require.NoError(t, env.GetWorkflowError())

			assert.True(t, tc.wantErr(stageErr), "stage error = %v", stageErr)
			assert.Equal(t, domain.NodeKey("sales/n1"), key)
			assert.Equal(t, tc.wantHistory, stack)
			assert.Equal(t, tc.wantStatus, rec.statusesOf("sales/n1"))
			assert.Len(t, rec.completions, tc.wantComplet)
			if tc.wantComplet == 1 {
				assert.Equal(t, "sales/n1", rec.completions[0].TaskID)
				assert.Equal(t, "tenant", rec.completions[0].TenantID)
				assert.Equal(t, "user-1", rec.completions[0].UserID)
				assert.Equal(t, int64(3), rec.completions[0].RecordVersion)
			}
		})
	}
}

func TestResolveTaskStage_AnswerThatDoesNotFinishTheTask_StillPushesHistoryMergesTheResultAndAdvances(t *testing.T) {
	cases := []struct {
		name       string
		notAllDone bool
		wantStatus []domain.TaskStatus
	}{
		{name: "last assignment", notAllDone: false, wantStatus: []domain.TaskStatus{domain.TaskStatusCompleted}},
		{name: "the answer does not finish the task", notAllDone: true, wantStatus: nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &w23bRecorder{notAllDone: tc.notAllDone}
			env := w23bNewEnv(rec)

			var node domain.NodeKey
			var result string
			var resolveErr error
			var stack []domain.NodeKey
			var contextJSON string
			env.ExecuteWorkflow(func(ctx wf.Context) error {
				in := newInterpreter("tenant", "instance", `{"existing":1}`, nil, nil)
				node, result, resolveErr = in.resolveTaskStage(ctx, "sales/approval",
					port.CreateTaskOutput{TaskID: "task-1"},
					stageTransitionSignal{UserID: "user-1", ResultJSON: `{"decision":"approved"}`, RecordVersion: 7})
				stack = append(stack, in.history.stack...)
				contextJSON = in.contextJSON
				return nil
			})
			require.NoError(t, env.GetWorkflowError())

			require.NoError(t, resolveErr)
			assert.Equal(t, domain.NodeKey("sales/approval"), node)
			assert.JSONEq(t, `{"decision":"approved"}`, result)
			assert.Equal(t, []domain.NodeKey{"sales/approval"}, stack)
			assert.JSONEq(t, `{"existing":1,"decision":"approved"}`, contextJSON)
			assert.Equal(t, tc.wantStatus, rec.statusesOf("task-1"))
			require.Len(t, rec.completions, 1)
			assert.Equal(t, "task-1", rec.completions[0].TaskID)
			assert.Equal(t, int64(7), rec.completions[0].RecordVersion)
		})
	}
}

// w23bWarnings records the warnings of a test workflow environment.
type w23bWarnings struct {
	mu   sync.Mutex
	logs []w23bWarning
}

type w23bWarning struct {
	msg     string
	keyvals []any
}

func (w *w23bWarnings) Debug(string, ...any) {}
func (w *w23bWarnings) Info(string, ...any)  {}
func (w *w23bWarnings) Error(string, ...any) {}
func (w *w23bWarnings) Warn(msg string, keyvals ...any) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.logs = append(w.logs, w23bWarning{msg: msg, keyvals: keyvals})
}

func (w *w23bWarnings) with(text string) []w23bWarning {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []w23bWarning
	for _, l := range w.logs {
		if strings.Contains(l.msg, text) {
			out = append(out, l)
		}
	}
	return out
}

func TestRunStage_UnrecognizedStageType_WarnsWithTheEngineNoteAndStillCreatesATask(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	warnings := &w23bWarnings{}
	suite.SetLogger(warnings)
	env := suite.NewTestWorkflowEnvironment()
	env.SetStartTime(w23bStart)
	rec := &w23bRecorder{}
	w23bRegister(env, rec)
	w23bAnswer(env, stageTransitionSignal{DeptID: "sales", ToStage: "custom_role", UserID: "user-1", ResultJSON: "{}", VisitCount: 1})

	var key domain.NodeKey
	var stageErr error
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		in := newInterpreter("tenant", "instance", "", nil, nil)
		w23bStartRouter(ctx, in)
		key, _, stageErr = in.runStage(ctx, &dsl.CompiledPlan{Name: "main"}, "sales", &dsl.StageDef{
			Type: "custom_role", EngineNote: "stage type 'custom_role' is not a defined class",
		})
		return nil
	})
	require.NoError(t, env.GetWorkflowError())

	require.NoError(t, stageErr)
	assert.Equal(t, domain.NodeKey("sales/custom_role"), key)
	require.Len(t, rec.created, 1)
	assert.Equal(t, "tenant", rec.created[0].TenantID)
	assert.Equal(t, "instance", rec.created[0].InstanceID)
	assert.Equal(t, domain.NodeKey("sales/custom_role"), rec.created[0].NodeKey)
	logged := warnings.with("dispatching stage with engine note")
	require.Len(t, logged, 1)
	assert.Contains(t, logged[0].keyvals, "stage type 'custom_role' is not a defined class")
	assert.Contains(t, logged[0].keyvals, "sales/custom_role")
}

func TestRunTaskStage_CancelledWhileWaiting_SupersedesTheTaskUnlessAForceRouteIsInProgress(t *testing.T) {
	cases := []struct {
		name       string
		routing    bool
		wantStatus []domain.TaskStatus
	}{
		{name: "not routing", routing: false, wantStatus: []domain.TaskStatus{domain.TaskStatusSuperseded}},
		{name: "a force route is in progress", routing: true, wantStatus: nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &w23bRecorder{}
			env := w23bNewEnv(rec)

			var stageErr error
			var pendingAfter int
			env.ExecuteWorkflow(func(ctx wf.Context) error {
				in := newInterpreter("tenant", "instance", "", nil, nil)
				in.routing = tc.routing
				runCtx, cancelRun := wf.WithCancel(ctx)
				wf.Go(ctx, func(gctx wf.Context) {
					_ = wf.Await(gctx, func() bool { return len(in.pending) > 0 })
					cancelRun()
				})
				_, _, stageErr = in.runTaskStage(runCtx, &dsl.CompiledPlan{Name: "main"}, "sales", &dsl.StageDef{Type: "approve", NodeID: "n1"}, "sales/n1")
				pendingAfter = len(in.pending)
				return nil
			})
			require.NoError(t, env.GetWorkflowError())

			assert.ErrorIs(t, stageErr, errStageAbandoned)
			assert.Zero(t, pendingAfter, "the stage's pending key is removed")
			assert.Equal(t, tc.wantStatus, rec.statusesOf("sales/n1"))
			for _, in := range rec.taskStatus {
				assert.Equal(t, "tenant", in.TenantID)
			}
		})
	}
}

func TestRunTaskStage_CancelledDuringTheNodeWrite_SupersedesTheNewTask(t *testing.T) {
	cases := []struct {
		name       string
		routing    bool
		wantStatus []domain.TaskStatus
	}{
		{name: "not routing", routing: false, wantStatus: []domain.TaskStatus{domain.TaskStatusSuperseded}},
		{name: "a force route is in progress", routing: true, wantStatus: []domain.TaskStatus{domain.TaskStatusSuperseded}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &w23bRecorder{}
			env := w23bNewEnv(rec)
			// The node write never answers before the run is cancelled.
			env.OnActivity(port.ActivityUpdateInstanceNodes, mock.Anything, mock.Anything).After(time.Hour).Return(nil)

			var stageErr error
			env.ExecuteWorkflow(func(ctx wf.Context) error {
				in := newInterpreter("tenant", "instance", "", nil, nil)
				in.routing = tc.routing
				runCtx, cancelRun := wf.WithCancel(ctx)
				wf.Go(ctx, func(gctx wf.Context) {
					_ = wf.Sleep(gctx, time.Minute)
					cancelRun()
				})
				_, _, stageErr = in.runTaskStage(runCtx, &dsl.CompiledPlan{Name: "main"}, "sales", &dsl.StageDef{Type: "approve", NodeID: "n1"}, "sales/n1")
				return nil
			})
			require.NoError(t, env.GetWorkflowError())

			assert.ErrorIs(t, stageErr, errStageAbandoned)
			assert.Equal(t, tc.wantStatus, rec.statusesOf("sales/n1"))
		})
	}
}

func TestRunTaskStage_NonInterruptingBoundaryFiredThenCancelled_AbandonsHostAndPathTasks(t *testing.T) {
	cases := []struct {
		name       string
		routing    bool
		wantStatus []domain.TaskStatus
	}{
		{name: "not routing", routing: false, wantStatus: []domain.TaskStatus{domain.TaskStatusSuperseded}},
		{name: "a force route is in progress", routing: true, wantStatus: nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &w23bRecorder{}
			env := w23bNewEnv(rec)

			var stageErr error
			var pendingAfter int
			var pendingAtCancel []domain.NodeKey
			env.ExecuteWorkflow(func(ctx wf.Context) error {
				in := newInterpreter("tenant", "instance", "", nil, nil)
				in.routing = tc.routing
				runCtx, cancelRun := wf.WithCancel(ctx)
				wf.Go(ctx, func(gctx wf.Context) {
					// The boundary has fired once its target department's task waits too.
					_ = wf.Await(gctx, func() bool { return len(in.pending) == 2 })
					pendingAtCancel = in.currentPendingNodes()
					cancelRun()
				})
				plan := &dsl.CompiledPlan{
					Name:        "main",
					Departments: []dsl.DepartmentDef{{ID: "escalation", Stages: []dsl.StageDef{{Type: "prep"}}}},
				}
				stage := &dsl.StageDef{
					Type: "approve", NodeID: "n1",
					BoundaryTimer: &dsl.BoundaryTimer{Duration: "1h", Interrupting: false, TargetDept: "escalation"},
				}
				_, _, stageErr = in.runTaskStage(runCtx, plan, "sales", stage, "sales/n1")
				in.awaitAbandonedStages(ctx)
				pendingAfter = len(in.pending)
				return nil
			})
			require.NoError(t, env.GetWorkflowError())

			assert.ErrorIs(t, stageErr, errStageAbandoned)
			assert.Equal(t, []domain.NodeKey{"escalation/prep", "sales/n1"}, pendingAtCancel)
			assert.Zero(t, pendingAfter, "every pending key is removed")
			assert.Equal(t, tc.wantStatus, rec.statusesOf("sales/n1"))
			assert.Equal(t, tc.wantStatus, rec.statusesOf("escalation/prep"))
		})
	}
}
