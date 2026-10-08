package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
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
		completion: newCompletionClient(callbackSrv.URL, "token", time.Second),
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

// capturingConnector records the params it was run with.
type capturingConnector struct {
	typ    string
	params chan map[string]any
}

func (c *capturingConnector) Type() string { return c.typ }
func (c *capturingConnector) Execute(_ context.Context, params map[string]any) (map[string]any, error) {
	c.params <- params
	return map[string]any{"ok": true}, nil
}

// credentialFixture is a worker whose OpenBao holds the given fields of one
// tenant's storage credential, recording every path it is asked to read.
type credentialFixture struct {
	tenantID  uuid.UUID
	conn      *capturingConnector
	failClass chan string
	paths     chan string
	d         *deps
}

func newCredentialFixture(t *testing.T, stored map[string]string, storeStatus int) *credentialFixture {
	t.Helper()
	f := &credentialFixture{
		tenantID: uuid.New(), failClass: make(chan string, 1), paths: make(chan string, 64),
		conn: &capturingConnector{typ: registry.TypeStorage, params: make(chan map[string]any, 1)},
	}
	openbaoSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.paths <- strings.TrimPrefix(r.URL.Path, "/v1/secret/data/")
		if storeStatus != 0 {
			w.WriteHeader(storeStatus)
			return
		}
		field := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		value, ok := stored[field]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"data":{"` + field + `":"` + value + `"}}}`))
	}))
	t.Cleanup(openbaoSrv.Close)
	callbackSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/fail") {
			var body failRequest
			_ = json.NewDecoder(r.Body).Decode(&body)
			f.failClass <- body.ErrorClass
		}
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(callbackSrv.Close)
	pool := &typePool{connectorType: f.conn.typ, sem: make(chan struct{}, 1), timeout: time.Second, retry: registry.RetryPolicyUnsafe, connector: f.conn}
	f.d = &deps{
		dedup:      newFakeCacheStore(),
		openbao:    openbao.NewReader(openbaoSrv.URL, "token", "secret", time.Second),
		pools:      map[string]*typePool{f.conn.typ: pool},
		completion: newCompletionClient(callbackSrv.URL, "token", time.Second),
	}
	return f
}

func (f *credentialFixture) dispatch(resolvedInputs string) {
	dispatchOnce(f.d, valkeystream.Entry{ID: "1-0", Fields: map[string]string{
		"tenant_id": f.tenantID.String(), "task_id": uuid.NewString(),
		"department_id": uuid.NewString(), "connector_type": registry.TypeStorage,
		"resolved_inputs": resolvedInputs,
	}})
}

// ran waits for the connector's run and returns its params, failing the test
// when it does not run.
func (f *credentialFixture) ran(t *testing.T) map[string]any {
	t.Helper()
	select {
	case params := <-f.conn.params:
		return params
	case <-time.After(5 * time.Second):
		t.Fatal("the connector did not run")
		return nil
	}
}

func (f *credentialFixture) readPaths() []string {
	close(f.paths)
	var got []string
	for p := range f.paths {
		got = append(got, p)
	}
	return got
}

// A connector always uses the tenant's stored credential: the worker reads
// each of the connector type's secret fields from the path WriteCredential
// builds from the job's own tenant, type and field name, and nothing the
// diagram says decides which path.
func TestDispatch_ReadsTheTenantsStoredCredentialFromItsOwnPaths(t *testing.T) {
	f := newCredentialFixture(t, map[string]string{"accessKey": "own-access", "secretKey": "own-secret"}, 0)

	f.dispatch(`{"provider":"aws-s3","bucket":"b","key":"k"}`)

	got := f.ran(t)
	assert.Equal(t, "own-access", got["accessKey"])
	assert.Equal(t, "own-secret", got["secretKey"])
	assert.NotContains(t, got, "azureAccountName", "a field the tenant stored no credential for is left out")
	assert.Equal(t, "b", got["bucket"])
	for _, p := range f.readPaths() {
		assert.True(t, strings.HasPrefix(p, "connectors/"+f.tenantID.String()+"/storage/"), "read outside the tenant's own storage credentials: %s", p)
	}
}

// A value an old plan still carries for a secret field is never read or used:
// it is replaced by the stored credential, and a path it names is not fetched.
func TestDispatch_AnAuthoredSecretValueIsNeverUsed(t *testing.T) {
	f := newCredentialFixture(t, map[string]string{"accessKey": "own-access", "secretKey": "own-secret"}, 0)
	foreign := "connectors/" + uuid.NewString() + "/storage/accessKey"

	f.dispatch(`{"provider":"aws-s3","accessKey":"` + foreign + `","secretKey":{"path":"x"}}`)

	got := f.ran(t)
	assert.Equal(t, "own-access", got["accessKey"])
	assert.Equal(t, "own-secret", got["secretKey"])
	for _, p := range f.readPaths() {
		assert.NotEqual(t, foreign, p)
	}
}

// With no credential stored, the connector runs and refuses it by its own
// validation, which names the provider that needs it; the worker does not
// guess which of the type's secret fields a provider requires.
func TestDispatch_NoStoredCredential_LeavesTheConnectorToRefuseIt(t *testing.T) {
	f := newCredentialFixture(t, nil, 0)

	f.dispatch(`{"provider":"aws-s3"}`)

	got := f.ran(t)
	assert.NotContains(t, got, "accessKey")
}

func TestDispatch_CredentialStoreFailure_FailsTheTaskBeforeTheConnectorRuns(t *testing.T) {
	f := newCredentialFixture(t, nil, http.StatusForbidden)

	f.dispatch(`{"provider":"aws-s3"}`)

	select {
	case class := <-f.failClass:
		assert.Equal(t, "secret_unauthorized", class)
	default:
		t.Fatal("the task was not failed")
	}
	select {
	case <-f.conn.params:
		t.Fatal("the connector ran without its credential")
	default:
	}
}

type fakeConsumer struct {
	mu           sync.Mutex
	acked        []string
	touched      []string
	deadLettered map[string]string
	deadErr      error
}

func newFakeConsumer() *fakeConsumer { return &fakeConsumer{deadLettered: map[string]string{}} }

func (c *fakeConsumer) ReadGroup(context.Context, string, string, string, int64, time.Duration) ([]valkeystream.Entry, error) {
	return nil, nil
}

func (c *fakeConsumer) ReclaimStale(context.Context, string, string, string, time.Duration, int64) ([]valkeystream.Entry, error) {
	return nil, nil
}

func (c *fakeConsumer) Ack(_ context.Context, _, _, entryID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.acked = append(c.acked, entryID)
	return nil
}

func (c *fakeConsumer) DeadLetter(_ context.Context, _, _, _ string, entry valkeystream.Entry, reason string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.deadErr != nil {
		return c.deadErr
	}
	c.deadLettered[entry.ID] = reason
	return nil
}

func (c *fakeConsumer) Touch(_ context.Context, _, _, _ string, ids []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.touched = append(c.touched, ids...)
	return nil
}

func (c *fakeConsumer) touchedIDs() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.touched...)
}

func (c *fakeConsumer) ackCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.acked)
}

var _ streamConsumer = (*fakeConsumer)(nil)

func statusServer(t *testing.T, status int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) }))
	t.Cleanup(srv.Close)
	return srv
}

func connectorEntry(tenantID, taskID uuid.UUID, connectorType string) valkeystream.Entry {
	return valkeystream.Entry{ID: "1-0", Deliveries: 1, Fields: map[string]string{
		"tenant_id": tenantID.String(), "task_id": taskID.String(),
		"department_id": uuid.NewString(), "connector_type": connectorType,
	}}
}

type blockingConnector struct {
	countingConnector
	started, release chan struct{}
}

func (c *blockingConnector) Execute(ctx context.Context, in map[string]any) (map[string]any, error) {
	c.started <- struct{}{}
	<-c.release
	return c.countingConnector.Execute(ctx, in)
}

// Another consumer that reclaims an entry while its run is still going must
// neither start a second run nor ack the entry the first run still has to
// report on. The two consumers share only Valkey.
func TestDispatch_RedeliveredToAnotherConsumerWhileRunning_NeitherRunsNorAcks(t *testing.T) {
	tenantID, taskID := uuid.New(), uuid.New()
	conn := &blockingConnector{countingConnector: countingConnector{typ: "blocking"}, started: make(chan struct{}, 2), release: make(chan struct{})}
	cache := newFakeCacheStore()
	callbackURL := statusServer(t, http.StatusNoContent).URL
	newConsumerDeps := func(consumer *fakeConsumer) *deps {
		return &deps{
			consumer: consumer,
			dedup:    cache,
			pools: map[string]*typePool{conn.typ: {
				connectorType: conn.typ, sem: make(chan struct{}, 1), timeout: 5 * time.Second,
				retry: registry.RetryPolicyUnsafe, connector: conn,
			}},
			completion: newCompletionClient(callbackURL, "token", time.Second),
		}
	}
	first, second := newFakeConsumer(), newFakeConsumer()
	entry := connectorEntry(tenantID, taskID, conn.typ)

	var running sync.WaitGroup
	dispatchEntry(context.Background(), newConsumerDeps(first), &running, entry, noopLogger{})
	<-conn.started

	secondDone := make(chan struct{})
	go func() {
		dispatchOnce(newConsumerDeps(second), entry)
		close(secondDone)
	}()
	select {
	case <-conn.started:
		close(conn.release)
		<-secondDone
		running.Wait()
		t.Fatal("the other consumer started a second run while the first was still going")
	case <-secondDone:
	}
	assert.Zero(t, second.ackCount(), "the other consumer acked an entry whose run has not reported")

	close(conn.release)
	running.Wait()
	assert.Equal(t, int32(1), conn.calls.Load(), "connector ran twice")
	assert.Equal(t, 1, first.ackCount(), "the first run's report did not ack")
}

// The stream and the lease share one Valkey, so a lease that can't be taken
// means the run can't be guarded: leave the entry for redelivery.
func TestDispatch_LeaseCacheError_DoesNotRun(t *testing.T) {
	tenantID := uuid.New()
	conn := &countingConnector{typ: "counting"}
	cache := newFakeCacheStore()
	cache.nxErr = assert.AnError
	consumer := newFakeConsumer()
	d := &deps{
		consumer:   consumer,
		dedup:      cache,
		pools:      poolFor(conn),
		completion: newCompletionClient(statusServer(t, http.StatusNoContent).URL, "token", time.Second),
	}

	dispatchOnce(d, connectorEntry(tenantID, uuid.New(), conn.typ))

	assert.Equal(t, int32(0), conn.calls.Load(), "connector ran without a lease")
	assert.Zero(t, consumer.ackCount())
}

func TestDispatch_OverDeliveryCap_DeadLettersWithoutRunning(t *testing.T) {
	tenantID := uuid.New()
	conn := &countingConnector{typ: "counting"}
	consumer := newFakeConsumer()
	d := &deps{
		consumer:      consumer,
		dedup:         newFakeCacheStore(),
		pools:         poolFor(conn),
		completion:    newCompletionClient(statusServer(t, http.StatusNoContent).URL, "token", time.Second),
		maxDeliveries: 3,
		deadLetterKey: "connector-tasks-dead",
	}
	atCap := connectorEntry(tenantID, uuid.New(), conn.typ)
	atCap.ID, atCap.Deliveries = "1-0", 3
	overCap := connectorEntry(tenantID, uuid.New(), conn.typ)
	overCap.ID, overCap.Deliveries = "2-0", 4

	dispatchOnce(d, atCap)
	dispatchOnce(d, overCap)

	assert.Equal(t, int32(1), conn.calls.Load(), "only the entry at the cap may run")
	assert.NotContains(t, consumer.deadLettered, "1-0")
	assert.NotEmpty(t, consumer.deadLettered["2-0"], "the entry over the cap was not dead-lettered with a reason")
}

// ctxAwareConnector behaves like a real provider client: it gives up when its
// context is cancelled.
type ctxAwareConnector struct {
	countingConnector
	started, release chan struct{}
}

func (c *ctxAwareConnector) Execute(ctx context.Context, in map[string]any) (map[string]any, error) {
	c.started <- struct{}{}
	select {
	case <-c.release:
		return c.countingConnector.Execute(ctx, in)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Shutdown cancels the read loop's context. A run already under way has
// possibly reached the provider, so it must finish, record and report, or its
// side effect repeats after the restart.
func TestDispatch_ShutdownMidRun_StillReportsTheOutcome(t *testing.T) {
	tenantID := uuid.New()
	conn := &ctxAwareConnector{countingConnector: countingConnector{typ: "ctx-aware"}, started: make(chan struct{}, 1), release: make(chan struct{})}
	callbacks := make(chan string, 2)
	callbackSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callbacks <- r.URL.Path
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(callbackSrv.Close)
	consumer := newFakeConsumer()
	d := &deps{
		consumer: consumer,
		dedup:    newFakeCacheStore(),
		pools: map[string]*typePool{conn.typ: {
			connectorType: conn.typ, sem: make(chan struct{}, 1), timeout: 5 * time.Second,
			retry: registry.RetryPolicyUnsafe, connector: conn,
		}},
		completion: newCompletionClient(callbackSrv.URL, "token", time.Second),
	}
	taskID := uuid.New()
	ctx, shutdown := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	dispatchEntry(ctx, d, &wg, connectorEntry(tenantID, taskID, conn.typ), noopLogger{})
	<-conn.started
	shutdown()
	close(conn.release)
	wg.Wait()

	select {
	case path := <-callbacks:
		assert.Equal(t, "/api/v1/internal/connector-tasks/"+taskID.String()+"/complete", path)
	default:
		t.Fatal("the outcome was never reported")
	}
	assert.Equal(t, 1, consumer.ackCount())
}

// The reclaim wait is meant to detect a dead consumer. An entry this consumer
// is still queueing or running is touched on every heartbeat, so it never
// looks idle; once it is finished it is not.
func TestHeartbeat_TouchesOnlyEntriesStillBeingHandled(t *testing.T) {
	tenantID := uuid.New()
	conn := &blockingConnector{countingConnector: countingConnector{typ: "blocking"}, started: make(chan struct{}, 1), release: make(chan struct{})}
	consumer := newFakeConsumer()
	d := &deps{
		consumer: consumer,
		dedup:    newFakeCacheStore(),
		pools: map[string]*typePool{conn.typ: {
			connectorType: conn.typ, sem: make(chan struct{}, 1), timeout: 5 * time.Second,
			retry: registry.RetryPolicyUnsafe, connector: conn,
		}},
		completion: newCompletionClient(statusServer(t, http.StatusNoContent).URL, "token", time.Second),
	}
	running := connectorEntry(tenantID, uuid.New(), conn.typ)
	running.ID = "1-0"

	var wg sync.WaitGroup
	dispatchEntry(context.Background(), d, &wg, running, noopLogger{})
	<-conn.started
	heartbeatOnce(context.Background(), d, noopLogger{})
	assert.Equal(t, []string{"1-0"}, consumer.touchedIDs())

	close(conn.release)
	wg.Wait()
	heartbeatOnce(context.Background(), d, noopLogger{})
	assert.Equal(t, []string{"1-0"}, consumer.touchedIDs(), "a finished entry was still touched")
}

// Entries later in a batch wait behind earlier ones for a pool slot, and must
// be kept from looking idle for that whole wait.
func TestDispatchBatch_EntriesQueuedForAPoolSlotAreTouched(t *testing.T) {
	tenantID := uuid.New()
	conn := &blockingConnector{countingConnector: countingConnector{typ: "blocking"}, started: make(chan struct{}, 2), release: make(chan struct{})}
	consumer := newFakeConsumer()
	d := &deps{
		consumer: consumer,
		dedup:    newFakeCacheStore(),
		pools: map[string]*typePool{conn.typ: {
			connectorType: conn.typ, sem: make(chan struct{}, 1), timeout: 5 * time.Second,
			retry: registry.RetryPolicyUnsafe, connector: conn,
		}},
		completion: newCompletionClient(statusServer(t, http.StatusNoContent).URL, "token", time.Second),
	}
	// Three entries: one running, one blocked on the pool slot, and one the
	// batch has not reached yet.
	batch := make([]valkeystream.Entry, 3)
	for i := range batch {
		batch[i] = connectorEntry(tenantID, uuid.New(), conn.typ)
		batch[i].ID = fmt.Sprintf("%d-0", i+1)
	}

	var wg sync.WaitGroup
	batchDone := make(chan struct{})
	go func() {
		dispatchBatch(context.Background(), d, &wg, batch, noopLogger{})
		close(batchDone)
	}()
	<-conn.started
	heartbeatOnce(context.Background(), d, noopLogger{})

	assert.ElementsMatch(t, []string{"1-0", "2-0", "3-0"}, consumer.touchedIDs())
	close(conn.release)
	<-batchDone
	wg.Wait()
}

func TestDispatch_DeadLetterFailure_LeavesEntryPending(t *testing.T) {
	tenantID := uuid.New()
	conn := &countingConnector{typ: "counting"}
	consumer := newFakeConsumer()
	consumer.deadErr = assert.AnError
	d := &deps{
		consumer:      consumer,
		dedup:         newFakeCacheStore(),
		pools:         poolFor(conn),
		completion:    newCompletionClient(statusServer(t, http.StatusNoContent).URL, "token", time.Second),
		maxDeliveries: 1,
	}
	entry := connectorEntry(tenantID, uuid.New(), conn.typ)
	entry.Deliveries = 2

	dispatchOnce(d, entry)

	assert.Equal(t, int32(0), conn.calls.Load())
	assert.Zero(t, consumer.ackCount())
}

// A reclaimed copy of an entry this consumer is still running must leave the
// running copy's heartbeat alone.
func TestDispatch_DuplicateOfARunningEntry_StaysTouched(t *testing.T) {
	tenantID := uuid.New()
	conn := &blockingConnector{countingConnector: countingConnector{typ: "blocking"}, started: make(chan struct{}, 1), release: make(chan struct{})}
	consumer := newFakeConsumer()
	d := &deps{
		consumer: consumer,
		dedup:    newFakeCacheStore(),
		pools: map[string]*typePool{conn.typ: {
			connectorType: conn.typ, sem: make(chan struct{}, 2), timeout: 5 * time.Second,
			retry: registry.RetryPolicyUnsafe, connector: conn,
		}},
		completion: newCompletionClient(statusServer(t, http.StatusNoContent).URL, "token", time.Second),
	}
	entry := connectorEntry(tenantID, uuid.New(), conn.typ)

	var running sync.WaitGroup
	dispatchEntry(context.Background(), d, &running, entry, noopLogger{})
	<-conn.started
	var duplicate sync.WaitGroup
	dispatchBatch(context.Background(), d, &duplicate, []valkeystream.Entry{entry}, noopLogger{})
	duplicate.Wait()
	heartbeatOnce(context.Background(), d, noopLogger{})

	assert.Equal(t, []string{entry.ID}, consumer.touchedIDs(), "the duplicate dropped the running entry from the heartbeat")
	close(conn.release)
	running.Wait()
}

// A run that has outlived its lease is hung: it stops being touched, so the
// entry becomes reclaimable and the task is retried rather than held forever.
func TestHeartbeat_StopsTouchingARunPastItsLease(t *testing.T) {
	tenantID := uuid.New()
	conn := &blockingConnector{countingConnector: countingConnector{typ: "blocking"}, started: make(chan struct{}, 1), release: make(chan struct{})}
	consumer := newFakeConsumer()
	var mu sync.Mutex
	clock := time.Unix(0, 0)
	d := &deps{
		consumer: consumer,
		dedup:    newFakeCacheStore(),
		pools: map[string]*typePool{conn.typ: {
			connectorType: conn.typ, sem: make(chan struct{}, 1), timeout: 5 * time.Second,
			retry: registry.RetryPolicyUnsafe, connector: conn,
		}},
		completion: newCompletionClient(statusServer(t, http.StatusNoContent).URL, "token", time.Second),
		now: func() time.Time {
			mu.Lock()
			defer mu.Unlock()
			return clock
		},
	}

	var wg sync.WaitGroup
	dispatchEntry(context.Background(), d, &wg, connectorEntry(tenantID, uuid.New(), conn.typ), noopLogger{})
	<-conn.started
	mu.Lock()
	clock = clock.Add(5*time.Second + executionLeaseMargin + time.Second)
	mu.Unlock()
	heartbeatOnce(context.Background(), d, noopLogger{})

	assert.Empty(t, consumer.touchedIDs(), "a run past its lease was still touched")
	close(conn.release)
	wg.Wait()
}
