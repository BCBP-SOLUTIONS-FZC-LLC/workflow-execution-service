package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/openbao"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkeystream"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/connectors"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/registry"
)

type cwdepsLogLine struct {
	level, msg string
	fields     map[string]any
}

type cwdepsLogger struct {
	mu    sync.Mutex
	lines []cwdepsLogLine
}

func (l *cwdepsLogger) add(level, msg string, fields map[string]any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, cwdepsLogLine{level: level, msg: msg, fields: fields})
}

func (l *cwdepsLogger) Debug(msg string, f map[string]any) { l.add("debug", msg, f) }
func (l *cwdepsLogger) Info(msg string, f map[string]any)  { l.add("info", msg, f) }
func (l *cwdepsLogger) Warn(msg string, f map[string]any)  { l.add("warn", msg, f) }
func (l *cwdepsLogger) Error(msg string, f map[string]any) { l.add("error", msg, f) }

func (l *cwdepsLogger) matching(level, msg string) []cwdepsLogLine {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []cwdepsLogLine
	for _, line := range l.lines {
		if line.level == level && line.msg == msg {
			out = append(out, line)
		}
	}
	return out
}

type cwdepsCacheSet struct {
	key, value string
	ttl        time.Duration
}

// cwdepsCache is a cache store that records what it was asked and can fail or
// script its answers.
type cwdepsCache struct {
	mu     sync.Mutex
	values map[string]string
	gets   map[string]int
	sets   []cwdepsCacheSet
	nxKeys []string
	getFn  func(key string, call int) (string, error)
	setErr error
}

func newCwdepsCache() *cwdepsCache {
	return &cwdepsCache{values: map[string]string{}, gets: map[string]int{}}
}

func (c *cwdepsCache) Get(_ context.Context, key string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.gets[key]++
	if c.getFn != nil {
		return c.getFn(key, c.gets[key])
	}
	return c.values[key], nil
}

func (c *cwdepsCache) Set(_ context.Context, key, value string, ttl time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.setErr != nil {
		return c.setErr
	}
	c.values[key] = value
	c.sets = append(c.sets, cwdepsCacheSet{key: key, value: value, ttl: ttl})
	return nil
}

func (c *cwdepsCache) Del(_ context.Context, keys ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, k := range keys {
		delete(c.values, k)
	}
	return nil
}

func (c *cwdepsCache) SetNX(_ context.Context, key, value string, _ time.Duration) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nxKeys = append(c.nxKeys, key)
	if _, held := c.values[key]; held {
		return false, nil
	}
	c.values[key] = value
	return true, nil
}

func (c *cwdepsCache) Ping(context.Context) error { return nil }

func (c *cwdepsCache) writes() (sets []cwdepsCacheSet, nxKeys []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]cwdepsCacheSet(nil), c.sets...), append([]string(nil), c.nxKeys...)
}

type cwdepsAck struct{ stream, group, id string }

type cwdepsReclaimStep struct {
	err     error
	entries []valkeystream.Entry
	gate    chan struct{}
}

type cwdepsReclaimCall struct {
	stream, group, consumer string
	minIdle                 time.Duration
	count                   int64
}

// cwdepsConsumer records every call with the stream, group and consumer it got.
type cwdepsConsumer struct {
	mu       sync.Mutex
	acks     []cwdepsAck
	reclaims []cwdepsReclaimCall
	script   []cwdepsReclaimStep
	entered  chan int
}

func newCwdepsConsumer() *cwdepsConsumer { return &cwdepsConsumer{entered: make(chan int, 4096)} }

func (c *cwdepsConsumer) ReadGroup(context.Context, string, string, string, int64, time.Duration) ([]valkeystream.Entry, error) {
	return nil, nil
}

func (c *cwdepsConsumer) Ack(_ context.Context, stream, group, id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.acks = append(c.acks, cwdepsAck{stream: stream, group: group, id: id})
	return nil
}

func (c *cwdepsConsumer) ReclaimStale(_ context.Context, stream, group, consumer string, minIdle time.Duration, count int64) ([]valkeystream.Entry, error) {
	c.mu.Lock()
	c.reclaims = append(c.reclaims, cwdepsReclaimCall{stream: stream, group: group, consumer: consumer, minIdle: minIdle, count: count})
	n := len(c.reclaims)
	var step cwdepsReclaimStep
	if n <= len(c.script) {
		step = c.script[n-1]
	}
	c.mu.Unlock()
	select {
	case c.entered <- n:
	default:
	}
	if step.gate != nil {
		<-step.gate
	}
	return step.entries, step.err
}

func (c *cwdepsConsumer) DeadLetter(context.Context, string, string, string, valkeystream.Entry, string) error {
	return nil
}

func (c *cwdepsConsumer) Touch(context.Context, string, string, string, []string) error { return nil }

func (c *cwdepsConsumer) ackList() []cwdepsAck {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]cwdepsAck(nil), c.acks...)
}

type cwdepsCallback struct {
	path   string
	tenant string
	body   map[string]any
}

type cwdepsCallbackServer struct {
	*httptest.Server
	mu    sync.Mutex
	calls []cwdepsCallback
	seen  chan struct{}
}

func newCwdepsCallbackServer(t *testing.T, status int) *cwdepsCallbackServer {
	t.Helper()
	s := &cwdepsCallbackServer{seen: make(chan struct{}, 64)}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		s.mu.Lock()
		s.calls = append(s.calls, cwdepsCallback{path: r.URL.Path, tenant: r.Header.Get("x-tenant-id"), body: body})
		s.mu.Unlock()
		w.WriteHeader(status)
		s.seen <- struct{}{}
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *cwdepsCallbackServer) received() []cwdepsCallback {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]cwdepsCallback(nil), s.calls...)
}

type cwdepsFixture struct {
	d         *deps
	consumer  *cwdepsConsumer
	cache     *cwdepsCache
	log       *cwdepsLogger
	callbacks *cwdepsCallbackServer
}

const (
	cwdepsStream       = "stream-under-test"
	cwdepsGroup        = "group-under-test"
	cwdepsConsumerName = "consumer-under-test"
	cwdepsConnType     = "cwdeps-connector"
)

func newCwdepsFixture(t *testing.T, callbackStatus int, pools map[string]*typePool) *cwdepsFixture {
	t.Helper()
	f := &cwdepsFixture{
		consumer:  newCwdepsConsumer(),
		cache:     newCwdepsCache(),
		log:       &cwdepsLogger{},
		callbacks: newCwdepsCallbackServer(t, callbackStatus),
	}
	f.d = &deps{
		consumer:     f.consumer,
		dedup:        f.cache,
		pools:        pools,
		completion:   newCompletionClient(f.callbacks.URL, time.Second),
		streamKey:    cwdepsStream,
		streamGroup:  cwdepsGroup,
		consumerName: cwdepsConsumerName,
		claimMinIdle: time.Millisecond,
		batchSize:    7,
		retry:        retryBounds{MaxAttempts: 5, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond},
	}
	return f
}

func cwdepsPool(conn connectors.Connector) map[string]*typePool {
	return map[string]*typePool{conn.Type(): {connectorType: conn.Type(), sem: make(chan struct{}, 1), timeout: 5 * time.Second, connector: conn}}
}

type cwdepsEntryIDs struct{ tenant, task uuid.UUID }

func cwdepsEntry(id, connectorType string, extra map[string]string) (valkeystream.Entry, cwdepsEntryIDs) {
	ids := cwdepsEntryIDs{tenant: uuid.New(), task: uuid.New()}
	fields := map[string]string{
		"tenant_id": ids.tenant.String(), "task_id": ids.task.String(),
		"department_id": uuid.NewString(), "connector_type": connectorType,
	}
	for k, v := range extra {
		fields[k] = v
	}
	return valkeystream.Entry{ID: id, Deliveries: 1, Fields: fields}, ids
}

func (f *cwdepsFixture) dispatch(ctx context.Context, entry valkeystream.Entry) {
	var wg sync.WaitGroup
	dispatchBatch(ctx, f.d, &wg, []valkeystream.Entry{entry}, f.log)
	wg.Wait()
}

func cwdepsCallbackPath(taskID uuid.UUID, verb string) string {
	return "/api/v1/internal/connector-tasks/" + taskID.String() + "/" + verb
}

func TestHandleEntry_ConnectorTypeWithoutAPool_FailsTheTaskAndAcks(t *testing.T) {
	for _, connectorType := range []string{"", registry.TypeSQLQuery, "bogus"} {
		t.Run("type "+connectorType, func(t *testing.T) {
			conn := &cwdepsConnector{typ: cwdepsConnType}
			f := newCwdepsFixture(t, http.StatusNoContent, cwdepsPool(conn))
			entry, ids := cwdepsEntry("5-0", connectorType, nil)

			f.dispatch(context.Background(), entry)

			calls := f.callbacks.received()
			require.Len(t, calls, 1)
			assert.Equal(t, cwdepsCallbackPath(ids.task, "fail"), calls[0].path)
			assert.Equal(t, ids.tenant.String(), calls[0].tenant)
			assert.Equal(t, ids.tenant.String(), calls[0].body["tenant_id"])
			assert.Equal(t, "unknown_connector_type", calls[0].body["error_class"])
			assert.NotContains(t, calls[0].body, "output")
			assert.Equal(t, []cwdepsAck{{stream: cwdepsStream, group: cwdepsGroup, id: "5-0"}}, f.consumer.ackList())
			sets, nxKeys := f.cache.writes()
			assert.Empty(t, nxKeys, "no lease is taken for an unknown type")
			assert.Empty(t, sets, "no outcome is recorded for an unknown type")
			assert.Zero(t, conn.calls.Load())
			require.Len(t, f.log.matching("error", "connector-worker: unknown connector type"), 1)
			assert.Equal(t, connectorType, f.log.matching("error", "connector-worker: unknown connector type")[0].fields["connector_type"])
		})
	}
}

func TestHandleEntry_UnknownTypeCallbackFails_LeavesTheEntryPending(t *testing.T) {
	f := newCwdepsFixture(t, http.StatusInternalServerError, cwdepsPool(&cwdepsConnector{typ: cwdepsConnType}))
	entry, ids := cwdepsEntry("5-0", "bogus", nil)

	f.dispatch(context.Background(), entry)

	calls := f.callbacks.received()
	require.Len(t, calls, 1)
	assert.Equal(t, cwdepsCallbackPath(ids.task, "fail"), calls[0].path)
	assert.Empty(t, f.consumer.ackList(), "an unanswered failure callback must stay pending so the same answer is sent again")
	assert.Len(t, f.log.matching("warn", "connector-worker: callback failed, leaving entry unacked for redelivery"), 1)
}

// chat-notify has no provider client in the worker, so the connector library
// refuses every call; the refusal is permanent and answered after one attempt,
// and the worker reads nothing from OpenBao for a type the registry does not list.
func TestHandleEntry_ChatNotifyEntry_FailsWithValidationErrorAfterOneAttemptWithoutOpenBaoRead(t *testing.T) {
	set, err := connectors.New(connectors.Config{})
	require.NoError(t, err)
	pools := buildPools(&config.Config{ConnectorPoolSizeChatNotify: 2, ConnectorTimeoutChatNotify: 5 * time.Second}, set)
	require.Contains(t, pools, registry.TypeChatNotify)
	f := newCwdepsFixture(t, http.StatusNoContent, pools)

	var openbaoReads atomic.Int32
	openbaoSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		openbaoReads.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(openbaoSrv.Close)
	f.d.openbao = openbao.NewReader(openbaoSrv.URL, "token", "secret", time.Second)
	entry, ids := cwdepsEntry("6-0", registry.TypeChatNotify, map[string]string{
		"resolved_inputs": `{"method":"post-message","channelOrUser":"#ops","message":"hello"}`,
	})

	f.dispatch(context.Background(), entry)

	calls := f.callbacks.received()
	require.Len(t, calls, 1)
	assert.Equal(t, cwdepsCallbackPath(ids.task, "fail"), calls[0].path)
	assert.Equal(t, "validation_error", calls[0].body["error_class"])
	assert.Equal(t, []cwdepsAck{{stream: cwdepsStream, group: cwdepsGroup, id: "6-0"}}, f.consumer.ackList())
	assert.Zero(t, openbaoReads.Load(), "no secret is read for chat-notify")
	attempts := f.log.matching("warn", "connector-worker: connector call failed")
	require.Len(t, attempts, 1, "a validation refusal is never retried")
	assert.Equal(t, 1, attempts[0].fields["attempt"])
	assert.Equal(t, registry.TypeChatNotify, attempts[0].fields["connector_type"])
}

// The previous lease holder may record its outcome between this entry's first
// read and the moment this worker takes the lease; the run then re-sends it.
func TestRunJob_OutcomeRecordedByThePreviousLeaseHolder_IsResentNotRerun(t *testing.T) {
	conn := &cwdepsConnector{typ: cwdepsConnType, out: map[string]any{"ran": "again"}}
	f := newCwdepsFixture(t, http.StatusNoContent, cwdepsPool(conn))
	entry, ids := cwdepsEntry("7-0", cwdepsConnType, nil)
	key := outcomeKey(ids.task)
	f.cache.getFn = func(k string, call int) (string, error) {
		if k == key && call >= 2 {
			return `{"output":{"ran":"before"}}`, nil
		}
		return "", nil
	}

	f.dispatch(context.Background(), entry)

	assert.Zero(t, conn.calls.Load(), "the connector must not run when an outcome exists after the lease is taken")
	calls := f.callbacks.received()
	require.Len(t, calls, 1)
	assert.Equal(t, cwdepsCallbackPath(ids.task, "complete"), calls[0].path)
	assert.Equal(t, map[string]any{"ran": "before"}, calls[0].body["output"])
	assert.Equal(t, 2, f.cache.gets[key], "the outcome is read once before and once after the lease")
	_, nxKeys := f.cache.writes()
	assert.Equal(t, []string{leaseKey(ids.task)}, nxKeys)
	assert.Equal(t, []cwdepsAck{{stream: cwdepsStream, group: cwdepsGroup, id: "7-0"}}, f.consumer.ackList())
}

func TestRunJob_ContextCancelledBeforeTheJobStarts_LeavesTheEntryPendingAndReleasesItsSlot(t *testing.T) {
	conn := &cwdepsConnector{typ: cwdepsConnType}
	pools := cwdepsPool(conn)
	f := newCwdepsFixture(t, http.StatusNoContent, pools)
	entry, _ := cwdepsEntry("8-0", cwdepsConnType, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	f.dispatch(ctx, entry)

	assert.Zero(t, conn.calls.Load())
	assert.Empty(t, f.callbacks.received())
	assert.Empty(t, f.consumer.ackList())
	sets, nxKeys := f.cache.writes()
	assert.Empty(t, nxKeys, "no lease is taken")
	assert.Empty(t, sets)
	assert.Zero(t, len(pools[cwdepsConnType].sem), "the pool slot was released")
	assert.Empty(t, f.d.inflight.live(time.Now()), "the entry left the in-flight set")
	assert.True(t, f.d.inflight.claim("8-0"), "the entry can be claimed again")
}

func TestHandleEntry_RecordedOutcomeUnreadable_LeavesEntryPendingWithoutRunning(t *testing.T) {
	cases := []struct {
		name    string
		get     func(string, int) (string, error)
		wantErr string
	}{
		{name: "cache read fails", get: func(string, int) (string, error) { return "", errors.New("valkey down") }, wantErr: "read recorded outcome: valkey down"},
		{name: "stored record is not JSON", get: func(string, int) (string, error) { return "not json", nil }, wantErr: "decode recorded outcome: "},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn := &cwdepsConnector{typ: cwdepsConnType}
			f := newCwdepsFixture(t, http.StatusNoContent, cwdepsPool(conn))
			f.cache.getFn = tc.get
			entry, ids := cwdepsEntry("9-0", cwdepsConnType, nil)

			f.dispatch(context.Background(), entry)

			assert.Zero(t, conn.calls.Load())
			assert.Empty(t, f.callbacks.received(), "nothing is reported for an unreadable outcome")
			assert.Empty(t, f.consumer.ackList())
			_, nxKeys := f.cache.writes()
			assert.Empty(t, nxKeys)
			lines := f.log.matching("warn", "connector-worker: recorded outcome unreadable, leaving entry pending")
			require.Len(t, lines, 1)
			assert.Equal(t, ids.task, lines[0].fields["task_id"])
			assert.Contains(t, lines[0].fields["error"], tc.wantErr)
		})
	}
}

func TestRunJob_ConnectorReturnsNothing_RecordsEmptyObjectAndRedeliveryResendsIt(t *testing.T) {
	conn := &cwdepsConnector{typ: cwdepsConnType}
	f := newCwdepsFixture(t, http.StatusNoContent, cwdepsPool(conn))
	entry, ids := cwdepsEntry("10-0", cwdepsConnType, nil)

	f.dispatch(context.Background(), entry)
	f.dispatch(context.Background(), entry)

	sets, _ := f.cache.writes()
	require.Len(t, sets, 1)
	assert.Equal(t, "connector-outcome:"+ids.task.String(), sets[0].key)
	assert.Equal(t, "{}", sets[0].value)
	assert.Equal(t, 24*time.Hour, sets[0].ttl)
	assert.Equal(t, int32(1), conn.calls.Load(), "the redelivery found the record and did not run again")
	calls := f.callbacks.received()
	require.Len(t, calls, 2)
	for _, c := range calls {
		assert.Equal(t, cwdepsCallbackPath(ids.task, "complete"), c.path)
		assert.Contains(t, c.body, "output")
		assert.Nil(t, c.body["output"])
	}
	assert.Len(t, f.consumer.ackList(), 2)
}

func TestRunJob_OutcomeWriteFails_StillReportsAndAcksAndLogsIt(t *testing.T) {
	conn := &cwdepsConnector{typ: cwdepsConnType, out: map[string]any{"sent": true}}
	f := newCwdepsFixture(t, http.StatusNoContent, cwdepsPool(conn))
	f.cache.setErr = errors.New("valkey read-only")
	entry, ids := cwdepsEntry("11-0", cwdepsConnType, nil)

	f.dispatch(context.Background(), entry)

	assert.Equal(t, int32(1), conn.calls.Load())
	calls := f.callbacks.received()
	require.Len(t, calls, 1)
	assert.Equal(t, cwdepsCallbackPath(ids.task, "complete"), calls[0].path)
	assert.Equal(t, map[string]any{"sent": true}, calls[0].body["output"])
	assert.Equal(t, []cwdepsAck{{stream: cwdepsStream, group: cwdepsGroup, id: "11-0"}}, f.consumer.ackList())
	lines := f.log.matching("error", "connector-worker: outcome not recorded, a redelivery will run the connector again")
	require.Len(t, lines, 1)
	assert.Equal(t, ids.task, lines[0].fields["task_id"])
	assert.Contains(t, lines[0].fields["error"], "valkey read-only")
}

// The loop must survive a failed reclaim: nothing is dispatched for it, the
// next tick asks again, and cancelling the context ends the loop.
func TestReclaimLoop_ReclaimError_IsLoggedAndTheNextTickRetries(t *testing.T) {
	conn := &cwdepsConnector{typ: cwdepsConnType, out: map[string]any{"ok": true}}
	f := newCwdepsFixture(t, http.StatusNoContent, cwdepsPool(conn))
	entry, ids := cwdepsEntry("12-0", cwdepsConnType, nil)
	gate := make(chan struct{})
	f.consumer.script = []cwdepsReclaimStep{
		{err: errors.New("xautoclaim failed")},
		{gate: gate, entries: []valkeystream.Entry{entry}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		reclaimLoop(ctx, f.d, &wg, f.log)
	}()

	for n := range f.consumer.entered {
		if n == 2 {
			break
		}
	}
	assert.Zero(t, conn.calls.Load(), "the failed reclaim dispatched nothing")
	assert.Empty(t, f.callbacks.received())
	warns := f.log.matching("warn", "connector-worker: reclaim stale failed")
	require.Len(t, warns, 1)
	assert.Equal(t, "xautoclaim failed", warns[0].fields["error"])

	close(gate)
	<-f.callbacks.seen
	cancel()
	<-loopDone
	wg.Wait()

	assert.Equal(t, int32(1), conn.calls.Load(), "the entry from the retry ran")
	calls := f.callbacks.received()
	require.Len(t, calls, 1)
	assert.Equal(t, cwdepsCallbackPath(ids.task, "complete"), calls[0].path)
	f.consumer.mu.Lock()
	defer f.consumer.mu.Unlock()
	require.GreaterOrEqual(t, len(f.consumer.reclaims), 2)
	assert.Equal(t, cwdepsReclaimCall{stream: cwdepsStream, group: cwdepsGroup, consumer: cwdepsConsumerName, minIdle: time.Millisecond, count: 7}, f.consumer.reclaims[0])
}
