package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkeystream"
)

const (
	dspfixStream     = "dspfix-stream"
	dspfixGroup      = "dspfix-group"
	dspfixConsumer   = "dspfix-consumer"
	dspfixDeadLetter = "dspfix-dead"
	dspfixGuard      = 10 * time.Second
)

type dspfixStreamCall struct{ stream, group, id string }

type dspfixDeadCall struct {
	stream, group, deadKey, reason string
	entry                          valkeystream.Entry
}

type dspfixReadStep struct {
	entries []valkeystream.Entry
	err     error
	// after runs once the step has been served, before ReadGroup returns.
	after func()
}

// dspfixStreamFake records the stream, group and entry ids of every call. ReadGroup serves its script in order
// and, once the script is spent, blocks on the caller's context like a real blocking read.
type dspfixStreamFake struct {
	mu        sync.Mutex
	script    []dspfixReadStep
	reads     []dspfixStreamCall
	ensures   []dspfixStreamCall
	ensureErr error
	onEnsure  func()
	acks      []dspfixStreamCall
	dead      []dspfixDeadCall
	deadErr   error
}

func (c *dspfixStreamFake) ReadGroup(ctx context.Context, stream, group, _ string, _ int64, _ time.Duration) ([]valkeystream.Entry, error) {
	c.mu.Lock()
	c.reads = append(c.reads, dspfixStreamCall{stream: stream, group: group})
	var step dspfixReadStep
	served := len(c.script) > 0
	if served {
		step, c.script = c.script[0], c.script[1:]
	}
	c.mu.Unlock()
	if !served {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if step.after != nil {
		step.after()
	}
	return step.entries, step.err
}

func (c *dspfixStreamFake) EnsureGroup(_ context.Context, stream, group string) error {
	c.mu.Lock()
	c.ensures = append(c.ensures, dspfixStreamCall{stream: stream, group: group})
	err, fn := c.ensureErr, c.onEnsure
	c.mu.Unlock()
	if fn != nil {
		fn()
	}
	return err
}

func (c *dspfixStreamFake) Ack(_ context.Context, stream, group, entryID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.acks = append(c.acks, dspfixStreamCall{stream: stream, group: group, id: entryID})
	return nil
}

func (c *dspfixStreamFake) ReclaimStale(context.Context, string, string, string, time.Duration, int64) ([]valkeystream.Entry, error) {
	return nil, nil
}

func (c *dspfixStreamFake) Touch(context.Context, string, string, string, []string) error { return nil }

// DeadLetter models the real consumer, which copies the entry and acknowledges the original in one step.
func (c *dspfixStreamFake) DeadLetter(_ context.Context, stream, group, deadKey string, entry valkeystream.Entry, reason string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.deadErr != nil {
		return c.deadErr
	}
	c.dead = append(c.dead, dspfixDeadCall{stream: stream, group: group, deadKey: deadKey, reason: reason, entry: entry})
	return nil
}

func (c *dspfixStreamFake) ackList() []dspfixStreamCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]dspfixStreamCall(nil), c.acks...)
}

func (c *dspfixStreamFake) deadList() []dspfixDeadCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]dspfixDeadCall(nil), c.dead...)
}

func (c *dspfixStreamFake) ensureList() []dspfixStreamCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]dspfixStreamCall(nil), c.ensures...)
}

func (c *dspfixStreamFake) readList() []dspfixStreamCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]dspfixStreamCall(nil), c.reads...)
}

var _ streamConsumer = (*dspfixStreamFake)(nil)

type dspfixCallback struct {
	path, tenant string
	body         map[string]any
}

type dspfixCallbackServer struct {
	*httptest.Server
	mu     sync.Mutex
	status int
	calls  []dspfixCallback
	seen   chan struct{}
}

func newDspfixCallbackServer(t *testing.T, status int) *dspfixCallbackServer {
	t.Helper()
	s := &dspfixCallbackServer{status: status, seen: make(chan struct{}, 64)}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		s.mu.Lock()
		s.calls = append(s.calls, dspfixCallback{path: r.URL.Path, tenant: r.Header.Get("x-tenant-id"), body: body})
		status := s.status
		s.mu.Unlock()
		w.WriteHeader(status)
		s.seen <- struct{}{}
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *dspfixCallbackServer) received() []dspfixCallback {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]dspfixCallback(nil), s.calls...)
}

func dspfixCallbackPath(taskID uuid.UUID, verb string) string {
	return "/api/v1/internal/connector-tasks/" + taskID.String() + "/" + verb
}

// dspfixGate is a connector that reports each run on started, holds it until release closes, and tracks how
// many runs overlap.
type dspfixGate struct {
	typ     string
	started chan string
	release chan struct{}

	once    sync.Once
	running atomic.Int32
	peak    atomic.Int32
	runs    atomic.Int32
}

func newDspfixGate(typ string, t *testing.T) *dspfixGate {
	g := &dspfixGate{typ: typ, started: make(chan string, 16), release: make(chan struct{})}
	t.Cleanup(g.open)
	return g
}

func (g *dspfixGate) Type() string { return g.typ }

// open lets every held and later run finish; it is safe to call more than once.
func (g *dspfixGate) open() { g.once.Do(func() { close(g.release) }) }

func (g *dspfixGate) Execute(_ context.Context, in map[string]any) (map[string]any, error) {
	now := g.running.Add(1)
	defer g.running.Add(-1)
	for {
		peak := g.peak.Load()
		if now <= peak || g.peak.CompareAndSwap(peak, now) {
			break
		}
	}
	g.runs.Add(1)
	label, _ := in["label"].(string)
	g.started <- label
	<-g.release
	return map[string]any{"ok": true}, nil
}

func dspfixPool(conn interface {
	Type() string
	Execute(context.Context, map[string]any) (map[string]any, error)
}, size int) *typePool {
	return &typePool{connectorType: conn.Type(), sem: make(chan struct{}, size), parked: make(chan struct{}, size), timeout: 5 * time.Second, connector: conn}
}

func dspfixEntry(id, connectorType string, fields map[string]string) (valkeystream.Entry, uuid.UUID, uuid.UUID) {
	tenantID, taskID := uuid.New(), uuid.New()
	all := map[string]string{
		"tenant_id": tenantID.String(), "task_id": taskID.String(),
		"department_id": uuid.NewString(), "connector_type": connectorType,
	}
	for k, v := range fields {
		all[k] = v
	}
	return valkeystream.Entry{ID: id, Deliveries: 1, Fields: all}, tenantID, taskID
}

type dspfixWorker struct {
	d         *deps
	stream    *dspfixStreamFake
	cache     *fakeCacheStore
	callbacks *dspfixCallbackServer
}

func newDspfixWorker(t *testing.T, callbackStatus int, pools map[string]*typePool) *dspfixWorker {
	t.Helper()
	w := &dspfixWorker{stream: &dspfixStreamFake{}, cache: newFakeCacheStore(), callbacks: newDspfixCallbackServer(t, callbackStatus)}
	w.d = &deps{
		consumer:      w.stream,
		dedup:         w.cache,
		pools:         pools,
		completion:    newCompletionClient(w.callbacks.URL, time.Second),
		streamKey:     dspfixStream,
		streamGroup:   dspfixGroup,
		consumerName:  dspfixConsumer,
		deadLetterKey: dspfixDeadLetter,
		claimMinIdle:  time.Hour,
		batchSize:     10,
		retry:         retryBounds{MaxAttempts: 1, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond},
	}
	return w
}

func (w *dspfixWorker) dispatch(ctx context.Context, entries ...valkeystream.Entry) {
	var wg sync.WaitGroup
	dispatchBatch(ctx, w.d, &wg, entries, noopLogger{})
	wg.Wait()
}

func dspfixReceive[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(dspfixGuard):
		t.Fatalf("timed out waiting for %s", what)
		panic("unreachable")
	}
}
