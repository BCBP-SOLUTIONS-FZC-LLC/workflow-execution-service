package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkeystream"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type cwcliLine struct {
	level  string
	msg    string
	fields map[string]any
}

// cwcliLogger keeps every line at every level with its message and fields.
type cwcliLogger struct {
	mu    sync.Mutex
	lines []cwcliLine
}

func (l *cwcliLogger) add(level, msg string, f map[string]any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	copied := make(map[string]any, len(f))
	for k, v := range f {
		copied[k] = v
	}
	l.lines = append(l.lines, cwcliLine{level: level, msg: msg, fields: copied})
}

func (l *cwcliLogger) Debug(m string, f map[string]any) { l.add("debug", m, f) }
func (l *cwcliLogger) Info(m string, f map[string]any)  { l.add("info", m, f) }
func (l *cwcliLogger) Warn(m string, f map[string]any)  { l.add("warn", m, f) }
func (l *cwcliLogger) Error(m string, f map[string]any) { l.add("error", m, f) }

func (l *cwcliLogger) withMessage(level, msg string) []cwcliLine {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []cwcliLine
	for _, line := range l.lines {
		if line.level == level && line.msg == msg {
			out = append(out, line)
		}
	}
	return out
}

var _ port.Logger = (*cwcliLogger)(nil)

// cwcliCache is an in-memory CacheStore that remembers every key it was asked to write.
type cwcliCache struct {
	mu     sync.Mutex
	values map[string]string
}

func newCwcliCache() *cwcliCache { return &cwcliCache{values: map[string]string{}} }

func (c *cwcliCache) Get(_ context.Context, key string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.values[key], nil
}

func (c *cwcliCache) Set(_ context.Context, key, value string, _ time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.values[key] = value
	return nil
}

func (c *cwcliCache) Del(_ context.Context, keys ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, k := range keys {
		delete(c.values, k)
	}
	return nil
}

func (c *cwcliCache) SetNX(_ context.Context, key, value string, _ time.Duration) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.values[key]; ok {
		return false, nil
	}
	c.values[key] = value
	return true, nil
}

func (c *cwcliCache) Ping(context.Context) error { return nil }

var _ port.CacheStore = (*cwcliCache)(nil)

type cwcliStreamCall struct {
	stream, group, consumer, entryID string
	ids                              []string
}

// cwcliConsumer is a stream consumer fake: it records the stream, group and entry ids of every call, and each
// method can be scripted with an error or a callback.
type cwcliConsumer struct {
	mu       sync.Mutex
	ackErr   error
	touchErr error
	acks     []cwcliStreamCall
	touches  []cwcliStreamCall
	reads    int
	reclaims int
	readArgs []cwcliReadArgs

	onRead    func(ctx context.Context) ([]valkeystream.Entry, error)
	onReclaim func(ctx context.Context) ([]valkeystream.Entry, error)
}

// cwcliReadArgs is what one ReadGroup call asked the stream for.
type cwcliReadArgs struct {
	stream, group, consumer string
	count                   int64
	block                   time.Duration
}

func (c *cwcliConsumer) ReadGroup(ctx context.Context, stream, group, consumer string, count int64, block time.Duration) ([]valkeystream.Entry, error) {
	c.mu.Lock()
	c.reads++
	c.readArgs = append(c.readArgs, cwcliReadArgs{stream: stream, group: group, consumer: consumer, count: count, block: block})
	fn := c.onRead
	c.mu.Unlock()
	if fn != nil {
		return fn(ctx)
	}
	return nil, ctx.Err()
}

func (c *cwcliConsumer) Ack(_ context.Context, stream, group, entryID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.acks = append(c.acks, cwcliStreamCall{stream: stream, group: group, entryID: entryID})
	return c.ackErr
}

func (c *cwcliConsumer) ReclaimStale(ctx context.Context, _, _, _ string, _ time.Duration, _ int64) ([]valkeystream.Entry, error) {
	c.mu.Lock()
	c.reclaims++
	fn := c.onReclaim
	c.mu.Unlock()
	if fn != nil {
		return fn(ctx)
	}
	return nil, nil
}

func (c *cwcliConsumer) DeadLetter(context.Context, string, string, string, valkeystream.Entry, string) error {
	return nil
}

func (c *cwcliConsumer) Touch(_ context.Context, stream, group, consumer string, ids []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.touches = append(c.touches, cwcliStreamCall{stream: stream, group: group, consumer: consumer, ids: append([]string(nil), ids...)})
	return c.touchErr
}

func (c *cwcliConsumer) ackCalls() []cwcliStreamCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]cwcliStreamCall(nil), c.acks...)
}

func (c *cwcliConsumer) touchCalls() []cwcliStreamCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]cwcliStreamCall(nil), c.touches...)
}

func (c *cwcliConsumer) readCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reads
}

func (c *cwcliConsumer) reclaimCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reclaims
}

var _ streamConsumer = (*cwcliConsumer)(nil)

const (
	cwcliStream       = "cwcli-stream"
	cwcliGroup        = "cwcli-group"
	cwcliConsumerName = "cwcli-consumer"
)

// cwcliConnector counts its runs and answers with a fixed output.
type cwcliConnector struct {
	typ   string
	mu    sync.Mutex
	calls int
}

func (c *cwcliConnector) Type() string { return c.typ }

func (c *cwcliConnector) Execute(context.Context, map[string]any) (map[string]any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	return map[string]any{"result": "ok"}, nil
}

func (c *cwcliConnector) runs() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func cwcliPoolFor(conn *cwcliConnector) map[string]*typePool {
	return map[string]*typePool{conn.typ: {
		connectorType: conn.typ,
		sem:           make(chan struct{}, 1),
		timeout:       time.Second,
		connector:     conn,
	}}
}

func cwcliEntry(tenantID, taskID uuid.UUID, connectorType string, deliveries int64) valkeystream.Entry {
	return valkeystream.Entry{ID: "1-0", Deliveries: deliveries, Fields: map[string]string{
		"tenant_id": tenantID.String(), "task_id": taskID.String(),
		"department_id": uuid.NewString(), "connector_type": connectorType,
	}}
}

// cwcliDispatch hands one entry to the dispatcher and waits for the run it started.
func cwcliDispatch(d *deps, entry valkeystream.Entry, log port.Logger) {
	var wg sync.WaitGroup
	dispatchBatch(context.Background(), d, &wg, []valkeystream.Entry{entry}, log)
	wg.Wait()
}

type cwcliRequest struct {
	method, path, query string
	header              http.Header
	body                []byte
}

// cwcliUpstream is a fake HTTP upstream that records every request it receives.
type cwcliUpstream struct {
	*httptest.Server
	mu    sync.Mutex
	seen  []cwcliRequest
	reply string
}

func (u *cwcliUpstream) setReply(body string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.reply = body
}

func newCwcliUpstream(t *testing.T, status int) *cwcliUpstream {
	t.Helper()
	u := &cwcliUpstream{}
	u.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		u.mu.Lock()
		u.seen = append(u.seen, cwcliRequest{method: r.Method, path: r.URL.Path, query: r.URL.RawQuery, header: r.Header.Clone(), body: body})
		reply := u.reply
		u.mu.Unlock()
		w.WriteHeader(status)
		_, _ = w.Write([]byte(reply))
	}))
	t.Cleanup(u.Close)
	return u
}

func (u *cwcliUpstream) requests() []cwcliRequest {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]cwcliRequest(nil), u.seen...)
}

func cwcliDecodeCallback(t *testing.T, body []byte) completeRequest {
	t.Helper()
	var got completeRequest
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("callback body is not a complete request: %v", err)
	}
	return got
}
