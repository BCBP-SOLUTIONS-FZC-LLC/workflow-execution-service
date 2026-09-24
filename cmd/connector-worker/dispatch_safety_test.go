package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/openbao"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkeystream"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/pkg/registry"
)

// clockCacheStore honours SetNX TTLs against a clock the test advances.
type clockCacheStore struct {
	*fakeCacheStore
	mu      sync.Mutex
	now     time.Time
	expires map[string]time.Time
}

func newClockCacheStore() *clockCacheStore {
	return &clockCacheStore{fakeCacheStore: newFakeCacheStore(), now: time.Unix(0, 0), expires: map[string]time.Time{}}
}

func (c *clockCacheStore) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func (c *clockCacheStore) SetNX(_ context.Context, key, value string, ttl time.Duration) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if exp, ok := c.expires[key]; ok && c.now.Before(exp) {
		return false, nil
	}
	c.values[key] = value
	c.expires[key] = c.now.Add(ttl)
	return true, nil
}

type countingConnector struct {
	typ   string
	calls atomic.Int32
}

func (c *countingConnector) Type() string { return c.typ }
func (c *countingConnector) Execute(context.Context, map[string]any) (map[string]any, error) {
	c.calls.Add(1)
	return map[string]any{"ok": true}, nil
}

func poolFor(conn *countingConnector) map[string]*typePool {
	return map[string]*typePool{conn.typ: {
		connectorType: conn.typ,
		sem:           make(chan struct{}, 1),
		timeout:       time.Second,
		retry:         registry.RetryPolicyUnsafe,
		connector:     conn,
	}}
}

func dispatchOnce(d *deps, entry valkeystream.Entry) {
	var wg sync.WaitGroup
	dispatchEntry(context.Background(), d, &wg, entry, noopLogger{})
	wg.Wait()
}

// A connector's side effect (an email, an external POST) must happen once per
// task. When the completion callback fails, the stream entry is re-claimed
// after CONNECTOR_STREAM_CLAIM_MIN_IDLE (30s by default); that redelivery
// must re-send the callback, not re-run the connector.
func TestDispatch_RedeliveryAfterFailedCallback_DoesNotRerunConnector(t *testing.T) {
	callbackSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(callbackSrv.Close)

	tenantID, taskID := uuid.New(), uuid.New()
	conn := &countingConnector{typ: "counting"}
	dedup := newClockCacheStore()
	d := &deps{
		dedup:      dedup,
		pools:      poolFor(conn),
		completion: newCompletionClient(callbackSrv.URL, "token", map[uuid.UUID]uuid.UUID{tenantID: uuid.New()}, time.Second),
	}
	entry := valkeystream.Entry{ID: "1-0", Fields: map[string]string{
		"tenant_id": tenantID.String(), "task_id": taskID.String(),
		"department_id": uuid.NewString(), "connector_type": conn.typ,
	}}

	dispatchOnce(d, entry)
	dedup.advance(30 * time.Second)
	dispatchOnce(d, entry)

	assert.Equal(t, int32(1), conn.calls.Load(), "connector ran again on redelivery")
}

// A task's secret_ref inputs may only name its own tenant's credentials. A
// path under another tenant's prefix must be refused before any OpenBao read
// and before the connector runs.
func TestDispatch_SecretRefToAnotherTenant_IsRefusedBeforeRead(t *testing.T) {
	var reads atomic.Int32
	openbaoSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reads.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"data":{"accessKey":"tenant-b-secret"}}}`))
	}))
	t.Cleanup(openbaoSrv.Close)
	// Failing the callback keeps the entry unacked, so no stream consumer is needed.
	callbackSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(callbackSrv.Close)

	tenantA, tenantB := uuid.New(), uuid.New()
	conn := &countingConnector{typ: registry.TypeStorage}
	d := &deps{
		dedup:      newFakeCacheStore(),
		openbao:    openbao.NewReader(openbaoSrv.URL, "token", "secret", time.Second),
		pools:      poolFor(conn),
		completion: newCompletionClient(callbackSrv.URL, "token", map[uuid.UUID]uuid.UUID{tenantA: uuid.New()}, time.Second),
	}
	entry := valkeystream.Entry{ID: "1-0", Fields: map[string]string{
		"tenant_id": tenantA.String(), "task_id": uuid.NewString(),
		"department_id": uuid.NewString(), "connector_type": conn.typ,
		"resolved_inputs": `{"accessKey":"connectors/` + tenantB.String() + `/storage/accessKey"}`,
	}}

	dispatchOnce(d, entry)

	assert.Equal(t, int32(0), reads.Load(), "OpenBao was read under another tenant's prefix")
	assert.Equal(t, int32(0), conn.calls.Load(), "connector ran with another tenant's credential")
}
