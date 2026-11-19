package main

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkeystream"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
)

// These tests run inside a synctest bubble: the loops' own tickers and timers run on the bubble's fake clock, so a
// 24-hour interval passes with no waiting and nothing here depends on the wall clock.

type w27bMsgLine struct {
	level  string
	msg    string
	fields map[string]any
}

// w27bMsgLogger keeps the level, message and fields of every line.
type w27bMsgLogger struct {
	mu    sync.Mutex
	lines []w27bMsgLine
}

func (l *w27bMsgLogger) add(level, msg string, f map[string]any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, w27bMsgLine{level: level, msg: msg, fields: f})
}
func (l *w27bMsgLogger) Debug(m string, f map[string]any) { l.add("debug", m, f) }
func (l *w27bMsgLogger) Info(m string, f map[string]any)  { l.add("info", m, f) }
func (l *w27bMsgLogger) Warn(m string, f map[string]any)  { l.add("warn", m, f) }
func (l *w27bMsgLogger) Error(m string, f map[string]any) { l.add("error", m, f) }

func (l *w27bMsgLogger) all() []w27bMsgLine {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]w27bMsgLine(nil), l.lines...)
}

func (l *w27bMsgLogger) messages(level string) []string {
	var out []string
	for _, line := range l.all() {
		if line.level == level {
			out = append(out, line.msg)
		}
	}
	return out
}

// w27bEventLog is an ordered record shared by the fakes of one test.
type w27bEventLog struct {
	mu     sync.Mutex
	events []string
}

func (e *w27bEventLog) add(event string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.events = append(e.events, event)
}

func (e *w27bEventLog) list() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.events...)
}

func (e *w27bEventLog) count(event string) int {
	n := 0
	for _, got := range e.list() {
		if got == event {
			n++
		}
	}
	return n
}

type w27bPruner struct {
	log     *w27bEventLog
	mu      sync.Mutex
	cutoffs []time.Time
}

func (p *w27bPruner) PruneAttempts(_ context.Context, cutoff time.Time, _ int) (int64, error) {
	p.mu.Lock()
	p.cutoffs = append(p.cutoffs, cutoff)
	p.mu.Unlock()
	p.log.add("prune")
	return 0, nil
}

type w27bTrimCall struct {
	stream string
	group  string
	cutoff time.Time
}

// w27bTrimStream is a stream consumer that can trim; every trim is recorded with its arguments.
type w27bTrimStream struct {
	streamConsumer
	log     *w27bEventLog
	mu      sync.Mutex
	pending []w27bTrimCall
	aged    []w27bTrimCall
}

func (c *w27bTrimStream) TrimToOldestPending(_ context.Context, stream, group string) (int64, error) {
	c.mu.Lock()
	c.pending = append(c.pending, w27bTrimCall{stream: stream, group: group})
	c.mu.Unlock()
	c.log.add("trim-pending")
	return 0, nil
}

func (c *w27bTrimStream) TrimOlderThan(_ context.Context, stream string, cutoff time.Time) (int64, error) {
	c.mu.Lock()
	c.aged = append(c.aged, w27bTrimCall{stream: stream, cutoff: cutoff})
	c.mu.Unlock()
	c.log.add("trim-dead")
	return 0, nil
}

func (c *w27bTrimStream) calls() (pending, aged []w27bTrimCall) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]w27bTrimCall(nil), c.pending...), append([]w27bTrimCall(nil), c.aged...)
}

// w27bStartMaintenance runs the maintenance loop in the bubble and returns a stop that cancels it and waits for it.
func w27bStartMaintenance(d *deps, cfg *config.Config) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runMaintenanceLoop(ctx, d, cfg, noopLogger{})
	}()
	synctest.Wait()
	return func() {
		cancel()
		<-done
	}
}

func TestRunMaintenanceLoop_PrunesAgainAfterTwentyFourHours(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		log := &w27bEventLog{}
		pruner := &w27bPruner{log: log}
		start := time.Now()
		d := &deps{pruner: pruner, pools: map[string]*typePool{}}
		cfg := &config.Config{ConnectorAliasRefreshInterval: 1000 * time.Hour, ConnectorAuditRetention: 90 * 24 * time.Hour}

		stop := w27bStartMaintenance(d, cfg)
		assert.Equal(t, 1, log.count("prune"), "one prune at start-up")

		time.Sleep(24*time.Hour - time.Nanosecond)
		synctest.Wait()
		assert.Equal(t, 1, log.count("prune"), "nothing more just before 24 hours")

		time.Sleep(time.Nanosecond)
		synctest.Wait()
		assert.Equal(t, 2, log.count("prune"), "the second prune at 24 hours")

		time.Sleep(24 * time.Hour)
		synctest.Wait()
		assert.Equal(t, 3, log.count("prune"), "and another a day later")
		stop()

		pruner.mu.Lock()
		defer pruner.mu.Unlock()
		require.Len(t, pruner.cutoffs, 3)
		assert.Equal(t, start.Add(-90*24*time.Hour), pruner.cutoffs[0])
		assert.Equal(t, start.Add(24*time.Hour-90*24*time.Hour), pruner.cutoffs[1], "the retention window moves with the clock")
		assert.Equal(t, start.Add(48*time.Hour-90*24*time.Hour), pruner.cutoffs[2])
	})
}

func TestRunMaintenanceLoop_TrimsStreamsRightAfterTheStartupPruneAndEveryFifteenMinutes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		log := &w27bEventLog{}
		stream := &w27bTrimStream{log: log}
		start := time.Now()
		const retention = 72 * time.Hour
		d := &deps{
			pruner: &w27bPruner{log: log}, pools: map[string]*typePool{}, consumer: stream,
			streamKey: "w27b-tasks", streamGroup: "w27b-group", deadLetterKey: "w27b-tasks-dead",
		}
		cfg := &config.Config{ConnectorAliasRefreshInterval: 1000 * time.Hour, ConnectorAuditRetention: time.Hour, ConnectorStreamDeadLetterRetention: retention}

		stop := w27bStartMaintenance(d, cfg)
		assert.Equal(t, []string{"prune", "trim-pending", "trim-dead"}, log.list(), "both trims run right after the start-up prune, task stream first")

		time.Sleep(15*time.Minute - time.Nanosecond)
		synctest.Wait()
		assert.Equal(t, 1, log.count("trim-pending"), "no trim just before 15 minutes")

		time.Sleep(time.Nanosecond)
		synctest.Wait()
		assert.Equal(t, 2, log.count("trim-pending"))
		assert.Equal(t, 2, log.count("trim-dead"))

		time.Sleep(15 * time.Minute)
		synctest.Wait()
		stop()

		pending, aged := stream.calls()
		require.Len(t, pending, 3)
		require.Len(t, aged, 3)
		for _, call := range pending {
			assert.Equal(t, w27bTrimCall{stream: "w27b-tasks", group: "w27b-group"}, call, "the task stream is trimmed to its oldest pending entry of the group")
		}
		for i, call := range aged {
			assert.Equal(t, w27bTrimCall{stream: "w27b-tasks-dead", cutoff: start.Add(time.Duration(i)*15*time.Minute - retention)}, call,
				"dead letters older than now minus the retention are trimmed")
		}
		assert.Equal(t, 1, log.count("prune"), "a trim tick does not prune")
	})
}

func TestTrimStreams_ConsumerThatCannotTrim_WarnsAndTrimsNothing(t *testing.T) {
	cases := []struct {
		name     string
		consumer streamConsumer
	}{
		{name: "a consumer with neither trim method", consumer: &dspfixStreamFake{}},
		{name: "no consumer at all", consumer: nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			log := &w27bMsgLogger{}
			d := &deps{consumer: tc.consumer, streamKey: "w27b-tasks", streamGroup: "w27b-group", deadLetterKey: "w27b-tasks-dead"}

			trimStreams(context.Background(), d, time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC), time.Hour, log)

			require.Len(t, log.all(), 1)
			assert.Equal(t, w27bMsgLine{level: "warn", msg: "connector-worker: stream consumer cannot trim"}, log.all()[0])
			if fake, ok := tc.consumer.(*dspfixStreamFake); ok {
				assert.Empty(t, fake.readList())
				assert.Empty(t, fake.ackList())
				assert.Empty(t, fake.deadList(), "nothing was moved or removed")
			}
		})
	}
}

type w27bTouchCall struct {
	at       time.Time
	stream   string
	group    string
	consumer string
	ids      []string
}

// w27bTouchStream records every heartbeat with the fake time it arrived at.
type w27bTouchStream struct {
	streamConsumer
	mu      sync.Mutex
	touches []w27bTouchCall
}

func (c *w27bTouchStream) Touch(_ context.Context, stream, group, consumer string, ids []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.touches = append(c.touches, w27bTouchCall{at: time.Now(), stream: stream, group: group, consumer: consumer, ids: append([]string(nil), ids...)})
	return nil
}

func (c *w27bTouchStream) list() []w27bTouchCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]w27bTouchCall(nil), c.touches...)
}

func TestHeartbeatLoop_TicksAtAThirdOfTheClaimIdleTimeAndStopsWithItsContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stream := &w27bTouchStream{}
		d := &deps{consumer: stream, claimMinIdle: 30 * time.Second, streamKey: "w27b-tasks", streamGroup: "w27b-group", consumerName: "w27b-worker"}
		require.True(t, d.inflight.claim("w27b-1"))
		start := time.Now()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			heartbeatLoop(ctx, d, noopLogger{})
		}()
		synctest.Wait()
		assert.Empty(t, stream.list(), "no heartbeat before the first tick")

		time.Sleep(10*time.Second - time.Nanosecond)
		synctest.Wait()
		assert.Empty(t, stream.list(), "the first tick is a third of 30s, not earlier")

		time.Sleep(time.Nanosecond)
		synctest.Wait()
		time.Sleep(10 * time.Second)
		synctest.Wait()
		got := stream.list()
		require.Len(t, got, 2)
		assert.Equal(t, w27bTouchCall{at: start.Add(10 * time.Second), stream: "w27b-tasks", group: "w27b-group", consumer: "w27b-worker", ids: []string{"w27b-1"}}, got[0])
		assert.Equal(t, start.Add(20*time.Second), got[1].at)

		cancel()
		<-done
		time.Sleep(time.Minute)
		synctest.Wait()
		assert.Len(t, stream.list(), 2, "no heartbeat after the context is cancelled")
	})
}

func TestShutdownDrain_UsesTheLargestOfTheSixConnectorTimeouts(t *testing.T) {
	const completion = 5 * time.Second
	timeouts := map[string]func(*config.Config, time.Duration){
		"storage":          func(c *config.Config, d time.Duration) { c.ConnectorTimeoutStorage = d },
		"send email":       func(c *config.Config, d time.Duration) { c.ConnectorTimeoutSendEmail = d },
		"document extract": func(c *config.Config, d time.Duration) { c.ConnectorTimeoutDocumentExtract = d },
		"rest call":        func(c *config.Config, d time.Duration) { c.ConnectorTimeoutRestCall = d },
		"sql query":        func(c *config.Config, d time.Duration) { c.ConnectorTimeoutSQLQuery = d },
		"chat notify":      func(c *config.Config, d time.Duration) { c.ConnectorTimeoutChatNotify = d },
	}
	for name, set := range timeouts {
		t.Run(name+" is the longest", func(t *testing.T) {
			cfg := &config.Config{ConnectorCompletionTimeout: completion}
			for _, other := range timeouts {
				other(cfg, 10*time.Second)
			}
			set(cfg, 77*time.Second)

			assert.Equal(t, 77*time.Second+completion+10*time.Second, shutdownDrain(cfg))
		})
	}
}

type w27bReadStep struct {
	err error
	// afterRead runs once the step is served, before the read returns.
	afterRead func()
}

// w27bReadStream serves its script of read errors in order, records the fake time of each read, and blocks on the
// caller's context once the script is spent.
type w27bReadStream struct {
	streamConsumer
	mu       sync.Mutex
	script   []w27bReadStep
	readAt   []time.Time
	ensureAt []time.Time
	ensureOK bool
}

func (c *w27bReadStream) ReadGroup(ctx context.Context, _, _, _ string, _ int64, _ time.Duration) ([]valkeystream.Entry, error) {
	c.mu.Lock()
	c.readAt = append(c.readAt, time.Now())
	var step w27bReadStep
	served := len(c.script) > 0
	if served {
		step, c.script = c.script[0], c.script[1:]
	}
	c.mu.Unlock()
	if !served {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if step.afterRead != nil {
		step.afterRead()
	}
	return nil, step.err
}

func (c *w27bReadStream) EnsureGroup(context.Context, string, string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ensureAt = append(c.ensureAt, time.Now())
	if c.ensureOK {
		return nil
	}
	return assert.AnError
}

func (c *w27bReadStream) ReclaimStale(context.Context, string, string, string, time.Duration, int64) ([]valkeystream.Entry, error) {
	return nil, nil
}

func (c *w27bReadStream) reads() []time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Time(nil), c.readAt...)
}

func w27bRunDispatchLoop(ctx context.Context, d *deps, log *w27bMsgLogger) (done chan struct{}) {
	done = make(chan struct{})
	go func() {
		defer close(done)
		var wg sync.WaitGroup
		runDispatchLoop(ctx, d, &wg, log)
		wg.Wait()
	}()
	return done
}

func TestDispatchLoop_ReadGroupError_WarnsThenReadsAgainAfterOneSecond(t *testing.T) {
	cases := []struct {
		name         string
		readErr      error
		wantError    []string
		wantEnsureOn int
	}{
		{
			name:    "a plain read failure",
			readErr: assert.AnError,
		},
		{
			name:         "a lost group that cannot be recreated",
			readErr:      errDspfixNoGroup,
			wantError:    []string{"connector-worker: consumer group lost and not recreated"},
			wantEnsureOn: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				stream := &w27bReadStream{script: []w27bReadStep{{err: tc.readErr}}}
				log := &w27bMsgLogger{}
				d := &deps{consumer: stream, streamKey: "w27b-tasks", streamGroup: "w27b-group", claimMinIdle: 1000 * time.Hour, batchSize: 10}
				start := time.Now()
				ctx, cancel := context.WithCancel(context.Background())
				done := w27bRunDispatchLoop(ctx, d, log)

				synctest.Wait()
				assert.Equal(t, []time.Time{start}, stream.reads(), "one read, then the loop waits")
				assert.Equal(t, tc.wantEnsureOn, len(stream.ensureAt))
				lines := log.all()
				require.NotEmpty(t, lines)
				assert.Equal(t, w27bMsgLine{level: "warn", msg: "connector-worker: read group failed", fields: map[string]any{"error": tc.readErr.Error()}}, lines[0])
				assert.Equal(t, tc.wantError, log.messages("error"))

				time.Sleep(time.Second - time.Nanosecond)
				synctest.Wait()
				assert.Len(t, stream.reads(), 1, "no read just before the one-second backoff ends")

				time.Sleep(time.Nanosecond)
				synctest.Wait()
				assert.Equal(t, []time.Time{start, start.Add(time.Second)}, stream.reads(), "the retry comes one second after the failure")

				cancel()
				<-done
			})
		})
	}
}

func TestDispatchLoop_ReadGroupErrorAfterCancel_ReturnsWithoutWarning(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		stream := &w27bReadStream{script: []w27bReadStep{{err: context.Canceled, afterRead: cancel}}}
		log := &w27bMsgLogger{}
		d := &deps{consumer: stream, streamKey: "w27b-tasks", streamGroup: "w27b-group", claimMinIdle: 1000 * time.Hour, batchSize: 10}

		done := w27bRunDispatchLoop(ctx, d, log)
		<-done

		assert.Len(t, stream.reads(), 1, "the loop did not read again")
		assert.Empty(t, log.all(), "a read that failed because of the shutdown is not logged")
	})
}

type w27bReclaimCall struct {
	at       time.Time
	stream   string
	group    string
	consumer string
	minIdle  time.Duration
	count    int64
}

// w27bReclaimStream hands out its reclaimed batches one per reclaim call and records the dead letters.
type w27bReclaimStream struct {
	*dspfixStreamFake
	rmu     sync.Mutex
	batches [][]valkeystream.Entry
	calls   []w27bReclaimCall
}

func (c *w27bReclaimStream) ReclaimStale(_ context.Context, stream, group, consumer string, minIdle time.Duration, count int64) ([]valkeystream.Entry, error) {
	c.rmu.Lock()
	defer c.rmu.Unlock()
	c.calls = append(c.calls, w27bReclaimCall{at: time.Now(), stream: stream, group: group, consumer: consumer, minIdle: minIdle, count: count})
	if len(c.batches) == 0 {
		return nil, nil
	}
	batch := c.batches[0]
	c.batches = c.batches[1:]
	return batch, nil
}

func (c *w27bReclaimStream) reclaimCalls() []w27bReclaimCall {
	c.rmu.Lock()
	defer c.rmu.Unlock()
	return append([]w27bReclaimCall(nil), c.calls...)
}

func TestReclaimLoop_ReclaimsEveryClaimIdleTimeAndDispatchesWithTheDeliveryCount(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pastCap, _, _ := dspfixEntry("w27b-9-0", "w27b-unknown", nil)
		pastCap.Deliveries = 4
		stream := &w27bReclaimStream{dspfixStreamFake: &dspfixStreamFake{}, batches: [][]valkeystream.Entry{{pastCap}}}
		d := &deps{
			consumer: stream, dedup: newFakeCacheStore(), pools: map[string]*typePool{},
			streamKey: "w27b-tasks", streamGroup: "w27b-group", consumerName: "w27b-worker", deadLetterKey: "w27b-tasks-dead",
			claimMinIdle: 30 * time.Second, batchSize: 7, maxDeliveries: 3,
		}
		start := time.Now()
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		var wg sync.WaitGroup
		go func() {
			defer close(done)
			reclaimLoop(ctx, d, &wg, noopLogger{})
		}()
		synctest.Wait()
		assert.Empty(t, stream.reclaimCalls(), "nothing is reclaimed before the first idle period")

		time.Sleep(30*time.Second - time.Nanosecond)
		synctest.Wait()
		assert.Empty(t, stream.reclaimCalls())

		time.Sleep(time.Nanosecond)
		synctest.Wait()
		time.Sleep(30 * time.Second)
		synctest.Wait()
		cancel()
		<-done
		wg.Wait()

		assert.Equal(t, []w27bReclaimCall{
			{at: start.Add(30 * time.Second), stream: "w27b-tasks", group: "w27b-group", consumer: "w27b-worker", minIdle: 30 * time.Second, count: 7},
			{at: start.Add(60 * time.Second), stream: "w27b-tasks", group: "w27b-group", consumer: "w27b-worker", minIdle: 30 * time.Second, count: 7},
		}, stream.reclaimCalls())
		assert.Equal(t, []dspfixDeadCall{{
			stream: "w27b-tasks", group: "w27b-group", deadKey: "w27b-tasks-dead", reason: "max_deliveries_exceeded", entry: pastCap,
		}}, stream.deadList(), "the reclaimed entry reached the dispatch with its delivery count (4 past a cap of 3) and was dead-lettered")
	})
}
