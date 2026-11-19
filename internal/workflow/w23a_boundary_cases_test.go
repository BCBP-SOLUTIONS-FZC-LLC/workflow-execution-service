package workflow

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// w23aTimerLog records the timers a test workflow schedules and cancels.
type w23aTimerLog struct {
	mu        sync.Mutex
	scheduled []time.Duration
	cancelled int
}

func (l *w23aTimerLog) attach(env *testsuite.TestWorkflowEnvironment) {
	env.SetOnTimerScheduledListener(func(_ string, d time.Duration) {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.scheduled = append(l.scheduled, d)
	})
	env.SetOnTimerCanceledListener(func(string) {
		l.mu.Lock()
		defer l.mu.Unlock()
		l.cancelled++
	})
}

// w23aSettle is the length of the timer a test workflow sleeps on after it
// cancels a case: the test environment reports a cancelled timer only once the
// workflow goes on running. snapshot leaves it out.
const w23aSettle = 7 * time.Minute

func (l *w23aTimerLog) snapshot() ([]time.Duration, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	var scheduled []time.Duration
	for _, d := range l.scheduled {
		if d != w23aSettle {
			scheduled = append(scheduled, d)
		}
	}
	return scheduled, l.cancelled
}

// w23aRecordingLogger records every warning a workflow logs, with its message.
type w23aRecordingLogger struct {
	mu    sync.Mutex
	warns []string
}

func (l *w23aRecordingLogger) Debug(string, ...interface{}) {}
func (l *w23aRecordingLogger) Info(string, ...interface{})  {}
func (l *w23aRecordingLogger) Error(string, ...interface{}) {}
func (l *w23aRecordingLogger) Warn(msg string, _ ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warns = append(l.warns, msg)
}

func (l *w23aRecordingLogger) messages() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.warns...)
}

// An empty duration schedules no timer, hands back a cancel that does nothing,
// and nothing fires however long the workflow runs.
func TestAddTimerCase_EmptyDurationCreatesNoTimerAndNothingFires(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	logger := &w23aRecordingLogger{}
	suite.SetLogger(logger)
	env := suite.NewTestWorkflowEnvironment()
	var timers w23aTimerLog
	timers.attach(env)

	var fires int
	var cancelReturned bool
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		sel := wf.NewSelector(ctx)
		cancel := addTimerCase(ctx, sel, "", true, "dept", func(boundaryFire) { fires++ })
		cancelReturned = cancel != nil
		sel.AddFuture(wf.NewTimer(ctx, 10*time.Hour), func(wf.Future) {})
		sel.Select(ctx)
		if cancel != nil {
			cancel()
		}
		return nil
	})

	require.NoError(t, env.GetWorkflowError())
	assert.True(t, cancelReturned)
	assert.Zero(t, fires)
	assert.Empty(t, logger.messages(), "an empty duration is skipped, not parsed and warned about")
	scheduled, cancelled := timers.snapshot()
	assert.Equal(t, []time.Duration{10 * time.Hour}, scheduled, "only the test's own sentinel timer exists")
	assert.Zero(t, cancelled)
}

// A message boundary with no name registers no wait: a message sent later is
// buffered, not delivered to anything, and the selector has nothing pending.
func TestAddMessageCase_EmptyNameRegistersNothing(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	var fires int
	var pending bool
	var blocked, buffered int
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		buf := newMessageBuffer()
		sel := wf.NewSelector(ctx)
		cancel := addMessageCase(ctx, sel, buf, "", false, "dept", func(boundaryFire) { fires++ })
		cancel()
		buf.Send(ctx, "", "sender/send")
		pending = sel.HasPending()
		blocked = len(buf.blocked[""])
		buffered = len(buf.buf[""])
		return nil
	})

	require.NoError(t, env.GetWorkflowError())
	assert.Zero(t, fires)
	assert.False(t, pending)
	assert.Zero(t, blocked, "no wait was registered")
	assert.Equal(t, 1, buffered, "the message went to the buffer")
}

// A message that has not arrived yet is awaited; the returned cancel removes the
// wait, so the message that comes later is buffered and nothing fires.
func TestAddMessageCase_CancelDeregistersTheWait(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	var fires int
	var blockedBefore, blockedAfter, bufferedAfter int
	var pendingAfterCancel bool
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		buf := newMessageBuffer()
		sel := wf.NewSelector(ctx)
		cancel := addMessageCase(ctx, sel, buf, "escalate", false, "dept", func(boundaryFire) { fires++ })
		blockedBefore = len(buf.blocked["escalate"])
		cancel()
		blockedAfter = len(buf.blocked["escalate"])
		buf.Send(ctx, "escalate", "sender/send")
		bufferedAfter = len(buf.buf["escalate"])
		pendingAfterCancel = sel.HasPending()
		return nil
	})

	require.NoError(t, env.GetWorkflowError())
	assert.Equal(t, 1, blockedBefore)
	assert.Zero(t, blockedAfter)
	assert.Equal(t, 1, bufferedAfter, "the message was kept for the next waiter")
	assert.False(t, pendingAfterCancel)
	assert.Zero(t, fires)
}

// A message that arrives after the wait is registered fires the case once.
func TestAddMessageCase_ALaterMessageFires(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var fired []boundaryFire
	var sleepErr error
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		buf := newMessageBuffer()
		sel := wf.NewSelector(ctx)
		addMessageCase(ctx, sel, buf, "escalate", true, "escalation", func(f boundaryFire) { fired = append(fired, f) })
		wf.Go(ctx, func(gctx wf.Context) {
			sleepErr = wf.Sleep(gctx, time.Minute)
			buf.Send(gctx, "escalate", "sender/send")
		})
		sel.Select(ctx)
		return nil
	})
	require.NoError(t, env.GetWorkflowError())
	require.NoError(t, sleepErr)
	assert.Equal(t, []boundaryFire{{Kind: "message", Interrupting: true, TargetDept: "escalation"}}, fired)
}

// An error boundary with a nil channel, or with no paths, registers nothing: the
// selector has nothing pending and an event on the channel is left unread.
func TestAddErrorCase_NotRegisteredWithoutAChannelOrPaths(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	var pendingNilChannel, pendingNoPaths bool
	var fires int
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		sel := wf.NewSelector(ctx)
		addErrorCase(ctx, sel, nil, []dsl.ErrorPath{{ErrorCode: "E1"}}, func(boundaryFire) { fires++ })
		pendingNilChannel = sel.HasPending()

		ch := wf.NewBufferedChannel(ctx, 1)
		ch.Send(ctx, "E1")
		addErrorCase(ctx, sel, ch, nil, func(boundaryFire) { fires++ })
		pendingNoPaths = sel.HasPending()
		return nil
	})

	require.NoError(t, env.GetWorkflowError())
	assert.False(t, pendingNilChannel)
	assert.False(t, pendingNoPaths, "a case on the channel would be ready: it holds an event")
	assert.Zero(t, fires)
}

// An error code with no matching path and no catch-all is dropped with a
// warning: nothing fires, and the next event is still read.
func TestAddErrorCase_UnmatchedCodeIsDroppedWithAWarning(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	logger := &w23aRecordingLogger{}
	suite.SetLogger(logger)
	env := suite.NewTestWorkflowEnvironment()

	var fired []boundaryFire
	var firesAfterDrop int
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		ch := wf.NewBufferedChannel(ctx, 2)
		ch.Send(ctx, "E9")
		sel := wf.NewSelector(ctx)
		addErrorCase(ctx, sel, ch, []dsl.ErrorPath{{ErrorCode: "E1", TargetDept: "handler"}}, func(f boundaryFire) { fired = append(fired, f) })
		sel.Select(ctx)
		firesAfterDrop = len(fired)
		ch.Send(ctx, "E1")
		sel.Select(ctx)
		return nil
	})

	require.NoError(t, env.GetWorkflowError())
	assert.Zero(t, firesAfterDrop, "the unmatched event started no path")
	assert.Equal(t, []boundaryFire{{Kind: "error", Interrupting: true, TargetDept: "handler", ErrorCode: "E1"}}, fired)
	assert.Contains(t, logger.messages(), "error-boundary event has no matching ErrorPath and no catch-all; dropping")
}

// A matching path fires an interrupting error boundary that carries the path's
// steps and target.
func TestAddErrorCase_MatchingPathFiresInterruptingWithItsSteps(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	steps := []dsl.ExecutionStep{{Sequential: []string{"recovery"}}}
	var fired boundaryFire
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		ch := wf.NewBufferedChannel(ctx, 1)
		ch.Send(ctx, "E1")
		sel := wf.NewSelector(ctx)
		addErrorCase(ctx, sel, ch, []dsl.ErrorPath{{ErrorCode: "E1", TargetDept: "handler", Steps: steps}}, withSteps(steps, func(f boundaryFire) { fired = f }))
		sel.Select(ctx)
		return nil
	})
	require.NoError(t, env.GetWorkflowError())
	assert.Equal(t, boundaryFire{Kind: "error", Interrupting: true, TargetDept: "handler", ErrorCode: "E1", Steps: steps}, fired)
}

// registerTaskBoundaries registers only what is present, and its cancel
// cancels every registered case.
func TestRegisterTaskBoundaries_RegistersOnlyThePresentCases(t *testing.T) {
	timer := &dsl.BoundaryTimer{Duration: "1h", Interrupting: true, TargetDept: "escalation"}
	msg := &dsl.MessagePath{MessageName: "m", Interrupting: false, TargetDept: "side"}
	cases := []struct {
		name          string
		timer         *dsl.BoundaryTimer
		msg           *dsl.MessagePath
		wantTimers    []time.Duration
		wantMsgWaiter int
	}{
		{name: "timer only", timer: timer, wantTimers: []time.Duration{time.Hour}},
		{name: "message only", msg: msg, wantMsgWaiter: 1},
		{name: "both", timer: timer, msg: msg, wantTimers: []time.Duration{time.Hour}, wantMsgWaiter: 1},
		{name: "neither"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			var timers w23aTimerLog
			timers.attach(env)

			var waitersBefore, waitersAfter, bufferedAfter int
			env.ExecuteWorkflow(func(ctx wf.Context) error {
				in := newInterpreter("tenant", "instance", "", nil, nil)
				sel := wf.NewSelector(ctx)
				cancel := registerTaskBoundaries(ctx, sel, in, "dept/node", tc.timer, tc.msg, func(boundaryFire) {})
				waitersBefore = len(in.msgBuf.blocked["m"])
				cancel()
				waitersAfter = len(in.msgBuf.blocked["m"])
				in.msgBuf.Send(ctx, "m", "sender/send")
				bufferedAfter = len(in.msgBuf.buf["m"])
				return wf.Sleep(ctx, w23aSettle)
			})

			require.NoError(t, env.GetWorkflowError())
			scheduled, cancelled := timers.snapshot()
			assert.Equal(t, tc.wantTimers, scheduled)
			assert.Equal(t, len(tc.wantTimers), cancelled, "the returned cancel cancels the timer it registered")
			assert.Equal(t, tc.wantMsgWaiter, waitersBefore)
			assert.Zero(t, waitersAfter, "the returned cancel removes the message wait")
			assert.Equal(t, 1, bufferedAfter, "a message after the cancel is buffered, not delivered to a wait")
		})
	}
}

// With both a timer and a message set, the first to happen fires with its own
// steps and the other case stays registered until the cancel.
func TestRegisterTaskBoundaries_TheFirstOfTimerAndMessageFiresWithItsOwnSteps(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	timerSteps := []dsl.ExecutionStep{{Sequential: []string{"from-timer"}}}
	msgSteps := []dsl.ExecutionStep{{Sequential: []string{"from-message"}}}
	timer := &dsl.BoundaryTimer{Duration: "1h", Interrupting: true, Steps: timerSteps}
	msg := &dsl.MessagePath{MessageName: "m", Interrupting: false, Steps: msgSteps}

	var fired []boundaryFire
	var sleepErr error
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		in := newInterpreter("tenant", "instance", "", nil, nil)
		sel := wf.NewSelector(ctx)
		cancel := registerTaskBoundaries(ctx, sel, in, "dept/node", timer, msg, func(f boundaryFire) { fired = append(fired, f) })
		wf.Go(ctx, func(gctx wf.Context) {
			sleepErr = wf.Sleep(gctx, 10*time.Minute)
			in.msgBuf.Send(gctx, "m", "sender/send")
		})
		sel.Select(ctx)
		cancel()
		return nil
	})
	require.NoError(t, env.GetWorkflowError())
	require.NoError(t, sleepErr)
	assert.Equal(t, []boundaryFire{{Kind: "message", Interrupting: false, Steps: msgSteps}}, fired)
}

// registerSubWorkflowBoundaries makes one timer per timer path, keys each message
// path by the sub-workflow's node id, and the first timer to elapse wins.
func TestRegisterSubWorkflowBoundaries_OneCasePerPathAndTheFirstTimerWins(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var timers w23aTimerLog
	timers.attach(env)

	longSteps := []dsl.ExecutionStep{{Sequential: []string{"slow-path"}}}
	shortSteps := []dsl.ExecutionStep{{Sequential: []string{"fast-path"}}}
	sw := &dsl.SubWorkflowStep{
		NodeID: "Sub_1",
		TimerPaths: []dsl.TimerPath{
			{Duration: "2h", Interrupting: true, TargetDept: "slow", Steps: longSteps},
			{Duration: "1h", Interrupting: false, TargetDept: "fast", Steps: shortSteps},
		},
		MessagePaths: []dsl.MessagePath{
			{MessageName: "m1", BoundaryID: "b1"},
			{MessageName: "m2", BoundaryID: "b2"},
		},
	}

	var fired []boundaryFire
	var waitingKeys []string
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		in := newInterpreter("tenant", "instance", "", nil, nil)
		in.external = map[string]bool{
			dsl.BoundaryWaitKey("Sub_1", "b1"): true,
			dsl.BoundaryWaitKey("Sub_1", "b2"): true,
		}
		sel := wf.NewSelector(ctx)
		cancel := registerSubWorkflowBoundaries(ctx, sel, in, nil, sw, func(f boundaryFire) { fired = append(fired, f) })
		for key := range in.externalWaiting {
			waitingKeys = append(waitingKeys, key)
		}
		sel.Select(ctx)
		cancel()
		return wf.Sleep(ctx, w23aSettle)
	})

	require.NoError(t, env.GetWorkflowError())
	scheduled, cancelled := timers.snapshot()
	assert.ElementsMatch(t, []time.Duration{2 * time.Hour, time.Hour}, scheduled, "one timer per timer path")
	assert.Equal(t, 1, cancelled, "the cancel stops the timer that did not fire")
	assert.ElementsMatch(t, []string{"Sub_1#b1", "Sub_1#b2"}, waitingKeys, "each message path waits under the sub-workflow node id")
	assert.Equal(t, []boundaryFire{{Kind: "timer", Interrupting: false, TargetDept: "fast", Steps: shortSteps}}, fired, "only the first timer fires")
}

// With a message path keyed by a node id, an answer for that key fires the path
// and an answer for another sub-workflow's key does not.
func TestRegisterSubWorkflowBoundaries_AnExternalAnswerFiresOnlyItsOwnKey(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	sw := &dsl.SubWorkflowStep{
		NodeID:       "Sub_1",
		MessagePaths: []dsl.MessagePath{{MessageName: "m1", BoundaryID: "b1", Interrupting: true, TargetDept: "after-message"}},
	}

	var fired []boundaryFire
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		in := newInterpreter("tenant", "instance", "", nil, nil)
		in.external = map[string]bool{"Sub_1#b1": true}
		sel := wf.NewSelector(ctx)
		cancel := registerSubWorkflowBoundaries(ctx, sel, in, nil, sw, func(f boundaryFire) { fired = append(fired, f) })
		ch := in.externalWaiting["Sub_1#b1"]
		require.NotNil(t, ch)
		_, other := in.externalWaiting["Other#b1"]
		assert.False(t, other)
		ch.Send(ctx, domain.NodeKey("Sub_1#b1"))
		sel.Select(ctx)
		cancel()
		return nil
	})

	require.NoError(t, env.GetWorkflowError())
	assert.Equal(t, []boundaryFire{{Kind: "message", Interrupting: true, TargetDept: "after-message"}}, fired)
}
