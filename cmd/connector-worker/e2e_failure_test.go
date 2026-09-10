//go:build e2e

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	temporaladapter "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

// e2eStartInstance issues POST /instances against execSrv and returns the new
// instance's ID — the same request shape
// TestE2E_ConnectorWorker_DispatchAndComplete builds by hand, factored out
// since both failure-path tests below need it too.
func e2eStartInstance(t *testing.T, httpClient *http.Client, execSrvURL string, tenantID, versionID uuid.UUID, aliasVar string) uuid.UUID {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, execSrvURL+"/api/v1/instances", bytes.NewReader(mustMarshalPlanBody(t, map[string]any{
		"business_key":        "connector-e2e-" + uuid.NewString(),
		"workflow_version_id": versionID,
		"context_json":        map[string]any{"aliasVar": aliasVar},
	})))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-tenant-id", tenantID.String())
	req.Header.Set("x-user-id", uuid.NewString())
	req.Header.Set("x-tenant-roles", "tenant_admin")
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("POST /instances: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST /instances status = %d, want 201", resp.StatusCode)
	}
	var startResp struct {
		ID uuid.UUID `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&startResp); err != nil {
		t.Fatalf("decode start response: %v", err)
	}
	return startResp.ID
}

// TestE2E_ConnectorWorker_DispatchFailsInstanceFails drives a rest-call
// connector task whose alias resolves to a non-idempotent method (POST) —
// isRetryable's conditional policy (dispatch.go) makes this unretryable, so
// a single 500 must go straight to d.completion.Fail, hit the target exactly
// once, and fail the instance (a sequential single-department plan has no
// Parallel gateway to degrade into).
func TestE2E_ConnectorWorker_DispatchFailsInstanceFails(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	sdk := fixtures.NewTestTemporalServer(t)
	valkeyAddr := fixtures.NewTestValkey(t)

	tenantID := uuid.New()
	departmentIAMID := uuid.New()
	versionID := uuid.New()
	streamKey := "connector-tasks-e2e-fail-" + uuid.NewString()

	var hitCount int
	var mu sync.Mutex
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hitCount++
		mu.Unlock()
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(target.Close)

	aliasSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"version":1,"restCall":[{"alias":"submit","method":"POST","baseURL":%q,"pathTemplate":"/submit","timeout":5000000000}],"sqlQuery":[]}`, target.URL)
	}))
	t.Cleanup(aliasSrv.Close)

	definitions := &e2eDefinitionClient{
		versionID: versionID,
		workflow: &port.CompiledWorkflow{
			WorkflowID: uuid.New(), VersionID: versionID, Status: "PUBLISHED", IsValid: true,
			CompiledPlanJSON: mustMarshalPlan(t, connectorTaskPlan(departmentIAMID)),
		},
	}

	validator, err := eventbus.NewSchemaValidator()
	if err != nil {
		t.Fatalf("new schema validator: %v", err)
	}
	temporalDeps := &temporaladapter.Deps{
		Instances: postgres.NewInstanceRepo(pool), Tasks: postgres.NewTaskRepo(pool), Assignments: postgres.NewTaskAssignmentRepo(pool),
		Outbox: postgres.NewOutboxRepo(pool), Transactor: postgres.NewTransactor(pool), Validator: validator, Definitions: definitions,
	}
	e2eStartWorker(t, sdk, temporalDeps, "wf-connector-e2e-test-queue")

	router := e2eBuildRouter(t, pool, sdk, valkeyAddr, definitions, streamKey)
	execSrv := httptest.NewServer(router.Handler())
	t.Cleanup(execSrv.Close)

	cfg := &config.Config{
		ValkeyAddr: valkeyAddr, ValkeyDialTimeout: 5 * time.Second, ValkeyReadTimeout: 5 * time.Second, ValkeyWriteTimeout: 5 * time.Second,
		ConnectorStreamKey: streamKey, ConnectorStreamGroup: "connector-worker-e2e", ConnectorStreamBlockTimeout: time.Second,
		ConnectorStreamClaimMinIdle: 30 * time.Second, ConnectorStreamBatchSize: 10,
		DefinitionServiceInternalHTTPAddr: aliasSrv.URL, ConnectorAliasFetchTimeout: 5 * time.Second,
		InternalAPIToken: e2eInternalToken, OpenBaoAddr: "http://unused.invalid", OpenBaoMount: "secret", OpenBaoTimeout: 5 * time.Second,
		ExecutionServiceInternalAddr: execSrv.URL, ConnectorCompletionTimeout: 5 * time.Second,
		ConnectorPoolSizeRestCall: 2, ConnectorTimeoutRestCall: 10 * time.Second,
	}
	deps, cleanup, err := buildDeps(cfg)
	if err != nil {
		t.Fatalf("buildDeps: %v", err)
	}
	t.Cleanup(cleanup)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	go runDispatchLoop(ctx, deps, &wg, noopLogger{})
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	httpClient := execSrv.Client()
	instanceID := e2eStartInstance(t, httpClient, execSrv.URL, tenantID, versionID, "submit")

	detail := e2ePollInstance(t, httpClient, execSrv.URL, tenantID, instanceID, func(d e2eInstanceDetail) bool {
		return len(d.Tasks) > 0 && d.Tasks[0].Status == "READY"
	})
	task := detail.Tasks[0]

	e2ePostWorkflowTaskCreated(t, httpClient, execSrv.URL, tenantID, task, map[string]any{"endpointAlias": "submit"})

	e2ePollInstance(t, httpClient, execSrv.URL, tenantID, instanceID, func(d e2eInstanceDetail) bool {
		return d.Status == "FAILED"
	})

	mu.Lock()
	got := hitCount
	mu.Unlock()
	if got != 1 {
		t.Errorf("target hit %d times, want exactly 1 (POST is non-idempotent, isRetryable's conditional policy must not retry it)", got)
	}
}

// TestE2E_ConnectorWorker_RestCallRetriesIdempotentMethod drives the mirror
// case: alias resolves to GET (idempotent), isRetryable's conditional policy
// makes this retryable — the target fails twice then succeeds, and the
// instance must still complete normally via runWithRetry's own backoff loop.
func TestE2E_ConnectorWorker_RestCallRetriesIdempotentMethod(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	sdk := fixtures.NewTestTemporalServer(t)
	valkeyAddr := fixtures.NewTestValkey(t)

	tenantID := uuid.New()
	departmentIAMID := uuid.New()
	versionID := uuid.New()
	streamKey := "connector-tasks-e2e-retry-" + uuid.NewString()

	var hitCount int
	var mu sync.Mutex
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		hitCount++
		n := hitCount
		mu.Unlock()
		if n < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(target.Close)

	aliasSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"version":1,"restCall":[{"alias":"echo","method":"GET","baseURL":%q,"pathTemplate":"/echo","timeout":5000000000}],"sqlQuery":[]}`, target.URL)
	}))
	t.Cleanup(aliasSrv.Close)

	definitions := &e2eDefinitionClient{
		versionID: versionID,
		workflow: &port.CompiledWorkflow{
			WorkflowID: uuid.New(), VersionID: versionID, Status: "PUBLISHED", IsValid: true,
			CompiledPlanJSON: mustMarshalPlan(t, connectorTaskPlan(departmentIAMID)),
		},
	}

	validator, err := eventbus.NewSchemaValidator()
	if err != nil {
		t.Fatalf("new schema validator: %v", err)
	}
	temporalDeps := &temporaladapter.Deps{
		Instances: postgres.NewInstanceRepo(pool), Tasks: postgres.NewTaskRepo(pool), Assignments: postgres.NewTaskAssignmentRepo(pool),
		Outbox: postgres.NewOutboxRepo(pool), Transactor: postgres.NewTransactor(pool), Validator: validator, Definitions: definitions,
	}
	e2eStartWorker(t, sdk, temporalDeps, "wf-connector-e2e-test-queue")

	router := e2eBuildRouter(t, pool, sdk, valkeyAddr, definitions, streamKey)
	execSrv := httptest.NewServer(router.Handler())
	t.Cleanup(execSrv.Close)

	cfg := &config.Config{
		ValkeyAddr: valkeyAddr, ValkeyDialTimeout: 5 * time.Second, ValkeyReadTimeout: 5 * time.Second, ValkeyWriteTimeout: 5 * time.Second,
		ConnectorStreamKey: streamKey, ConnectorStreamGroup: "connector-worker-e2e", ConnectorStreamBlockTimeout: time.Second,
		ConnectorStreamClaimMinIdle: 30 * time.Second, ConnectorStreamBatchSize: 10,
		DefinitionServiceInternalHTTPAddr: aliasSrv.URL, ConnectorAliasFetchTimeout: 5 * time.Second,
		InternalAPIToken: e2eInternalToken, OpenBaoAddr: "http://unused.invalid", OpenBaoMount: "secret", OpenBaoTimeout: 5 * time.Second,
		ExecutionServiceInternalAddr: execSrv.URL, ConnectorCompletionTimeout: 5 * time.Second,
		ConnectorPoolSizeRestCall: 2, ConnectorTimeoutRestCall: 10 * time.Second,
	}
	deps, cleanup, err := buildDeps(cfg)
	if err != nil {
		t.Fatalf("buildDeps: %v", err)
	}
	t.Cleanup(cleanup)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	go runDispatchLoop(ctx, deps, &wg, noopLogger{})
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	httpClient := execSrv.Client()
	instanceID := e2eStartInstance(t, httpClient, execSrv.URL, tenantID, versionID, "echo")

	detail := e2ePollInstance(t, httpClient, execSrv.URL, tenantID, instanceID, func(d e2eInstanceDetail) bool {
		return len(d.Tasks) > 0 && d.Tasks[0].Status == "READY"
	})
	task := detail.Tasks[0]

	e2ePostWorkflowTaskCreated(t, httpClient, execSrv.URL, tenantID, task, map[string]any{"endpointAlias": "echo"})

	e2ePollInstance(t, httpClient, execSrv.URL, tenantID, instanceID, func(d e2eInstanceDetail) bool {
		return d.Status == "COMPLETED"
	})

	mu.Lock()
	got := hitCount
	mu.Unlock()
	if got != 3 {
		t.Errorf("target hit %d times, want exactly 3 (2 failures then a success, GET is idempotent so isRetryable's conditional policy must retry it)", got)
	}
}
