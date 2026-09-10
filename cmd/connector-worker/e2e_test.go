//go:build e2e

// This file exercises cmd/connector-worker's own composition root
// (buildDeps/runDispatchLoop) against real infra — the gap left by
// test/e2e's instantiate/dispatch/complete round trip, which never runs this
// binary's own HTTP-dispatch path (it registers Activities directly instead,
// per that package's own doc comment). It lives here rather than in
// test/e2e because buildDeps/runDispatchLoop are unexported package-main
// functions.
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
	"github.com/redis/go-redis/v9"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	sdkworkflow "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"

	httpadapter "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	temporaladapter "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporalclient"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkey"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkeystream"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

const e2eInternalToken = "e2e-connector-worker-test-token"

type e2eDefinitionClient struct {
	versionID uuid.UUID
	workflow  *port.CompiledWorkflow
}

func (f *e2eDefinitionClient) GetCompiledWorkflow(_ context.Context, _, versionID uuid.UUID) (*port.CompiledWorkflow, error) {
	if versionID != f.versionID {
		return nil, fmt.Errorf("no compiled workflow fixture for version %s", versionID)
	}
	return f.workflow, nil
}

type e2eEligibilityChecker struct{}

func (e2eEligibilityChecker) CheckEligibility(context.Context, uuid.UUID, uuid.UUID, string, uuid.UUID) (bool, error) {
	return true, nil
}

func (e2eEligibilityChecker) CheckEligibilityBatch(_ context.Context, reqs []port.EligibilityCheckRequest, _ uuid.UUID) ([]port.EligibilityResult, error) {
	results := make([]port.EligibilityResult, len(reqs))
	for i := range reqs {
		results[i] = port.EligibilityResult{Eligible: true}
	}
	return results, nil
}

// connectorTaskPlan is one department with a single rest-call-typed stage —
// resolvedInputs.endpointAlias comes from the instance's own context_json
// via the stage's IOMapping, exactly as a real compiled BPMN connector task
// would produce it (internal/adapter/outbound/temporal/task.go's
// resolveConnectorInputs).
func connectorTaskPlan(departmentIAMID uuid.UUID) *dsl.CompiledCollaboration {
	return &dsl.CompiledCollaboration{
		SchemaVersion: dsl.CurrentSchemaVersion,
		MainPlan:      "main",
		Plans: []*dsl.CompiledPlan{{
			Name:      "main",
			TaskQueue: "wf-connector-e2e-test-queue",
			Departments: []dsl.DepartmentDef{{
				ID:              "ops",
				Label:           "Ops",
				IAMDepartmentID: departmentIAMID.String(),
				Stages: []dsl.StageDef{{
					Type: "connector", NodeID: "dispatch", ConnectorType: "rest-call",
					IOMapping: &dsl.IOMapping{
						Inputs: []dsl.IOVar{{Source: "aliasVar", Target: "endpointAlias"}},
					},
				}},
			}},
			Execution: dsl.ExecutionPlan{
				Steps: []dsl.ExecutionStep{{Sequential: []string{"ops"}}},
			},
		}},
	}
}

func e2eStartWorker(t *testing.T, sdk client.Client, deps *temporaladapter.Deps, queueName string) {
	t.Helper()
	w := worker.New(sdk, queueName, worker.Options{})
	w.RegisterWorkflowWithOptions(wfengine.Execute, sdkworkflow.RegisterOptions{Name: port.WorkflowTypeExecute})
	w.RegisterActivityWithOptions(deps.GetCompiledPlan, activity.RegisterOptions{Name: port.ActivityGetCompiledPlan})
	w.RegisterActivityWithOptions(deps.CreateTask, activity.RegisterOptions{Name: port.ActivityCreateTask})
	w.RegisterActivityWithOptions(deps.UpdateInstanceNodes, activity.RegisterOptions{Name: port.ActivityUpdateInstanceNodes})
	w.RegisterActivityWithOptions(deps.ClaimAssignment, activity.RegisterOptions{Name: port.ActivityClaimAssignment})
	w.RegisterActivityWithOptions(deps.CompleteAssignment, activity.RegisterOptions{Name: port.ActivityCompleteAssignment})
	w.RegisterActivityWithOptions(deps.DeferTask, activity.RegisterOptions{Name: port.ActivityDeferTask})
	w.RegisterActivityWithOptions(deps.UpdateInstanceStatus, activity.RegisterOptions{Name: port.ActivityUpdateInstanceStatus})
	w.RegisterActivityWithOptions(deps.RecordForceRoute, activity.RegisterOptions{Name: port.ActivityRecordForceRoute})
	w.RegisterActivityWithOptions(deps.RecordSLAWarning, activity.RegisterOptions{Name: port.ActivityRecordSLAWarning})
	w.RegisterActivityWithOptions(deps.RecordSLABreach, activity.RegisterOptions{Name: port.ActivityRecordSLABreach})
	w.RegisterActivityWithOptions(deps.PauseInstance, activity.RegisterOptions{Name: port.ActivityPauseInstance})
	w.RegisterActivityWithOptions(deps.ResumeInstance, activity.RegisterOptions{Name: port.ActivityResumeInstance})
	w.RegisterActivityWithOptions(deps.CancelInstance, activity.RegisterOptions{Name: port.ActivityCancelInstance})
	w.RegisterActivityWithOptions(deps.ReassignAssignment, activity.RegisterOptions{Name: port.ActivityReassignAssignment})
	w.RegisterActivityWithOptions(deps.UpdateTaskStatus, activity.RegisterOptions{Name: port.ActivityUpdateTaskStatus})
	if err := w.Start(); err != nil {
		t.Fatalf("start worker: %v", err)
	}
	t.Cleanup(w.Stop)
}

// newValkeyClient opens a plain client against the test container — reused
// by e2eBuildRouter for both the router's own dedup Cache and the Stream
// producer; buildDeps dials its own separate client for the consumer side,
// same as production (cmd/server and cmd/connector-worker are separate
// processes there too).
func newValkeyClient(t *testing.T, addr string) *redis.Client {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = c.Close() })
	return c
}

type e2eAlwaysHealthy struct{}

func (e2eAlwaysHealthy) Ping(context.Context) error { return nil }
func (e2eAlwaysHealthy) Health(context.Context) httpadapter.DBHealth {
	return httpadapter.DBHealth{Healthy: true}
}

// e2eBuildRouter wires the real router with everything a connector-typed
// task's full round trip touches: instantiate/get (human-task path's own
// services), the connector-task completion endpoints, and the
// workflow-task-created internal-events endpoint that pushes onto the real
// Valkey Stream cmd/connector-worker consumes.
func e2eBuildRouter(t *testing.T, pool *pgcommon.Pool, sdk client.Client, valkeyAddr string, definitions *e2eDefinitionClient, streamKey string) *httpadapter.Router {
	t.Helper()

	instances := postgres.NewInstanceRepo(pool)
	tasks := postgres.NewTaskRepo(pool)
	assignments := postgres.NewTaskAssignmentRepo(pool)
	outboxRepo := postgres.NewOutboxRepo(pool)
	processedEvents := postgres.NewProcessedEventRepo(pool)
	transactor := postgres.NewTransactor(pool)

	validator, err := eventbus.NewSchemaValidator()
	if err != nil {
		t.Fatalf("new schema validator: %v", err)
	}
	temporal := temporalclient.New(sdk)
	eligibility := e2eEligibilityChecker{}

	valkeyClient := newValkeyClient(t, valkeyAddr)
	connectorEvents := valkeystream.NewEventPublisher(valkeystream.NewProducer(valkeyClient), streamKey)

	instanceService := &service.InstanceService{
		Instances: instances, Tasks: tasks, Assignments: assignments, Outbox: outboxRepo,
		Transactor: transactor, Temporal: temporal, Definitions: definitions, Eligibility: eligibility,
		Validator: validator,
	}
	taskService := &service.TaskService{
		Instances: instances, Tasks: tasks, Assignments: assignments,
		Temporal: temporal, Eligibility: eligibility, Definitions: definitions,
	}
	connectorTaskService := &service.ConnectorTaskService{
		Instances: instances, Tasks: tasks, Temporal: temporal, Cache: valkey.NewCache(valkeyClient),
	}

	h := handler.New(handler.Services{
		Tasks: taskService, Instances: instanceService, Eligibility: eligibility,
		ProcessedEvents: processedEvents,
		ConnectorTasks:  connectorTaskService,
		ConnectorEvents: connectorEvents,
	})
	return httpadapter.NewRouter(httpadapter.RouterConfig{
		GinConfig:        gincommon.Config{ServiceName: "connector-worker-e2e-test", BuildVersion: "test"},
		AppEnv:           "dev",
		InternalAPIToken: e2eInternalToken,
		Handler:          h,
		DB:               e2eAlwaysHealthy{}, Cache: e2eAlwaysHealthy{}, Temporal: e2eAlwaysHealthy{},
	})
}

// e2eTaskDetail is a narrow view of GET /instances/:id's response — just
// enough to drive a connector task through the Valkey Stream by hand.
type e2eTaskDetail struct {
	ID                 uuid.UUID `json:"id"`
	WorkflowInstanceID uuid.UUID `json:"workflow_instance_id"`
	NodeKey            string    `json:"node_key"`
	DepartmentID       uuid.UUID `json:"department_id"`
	Status             string    `json:"status"`
}

type e2eInstanceDetail struct {
	Status string          `json:"status"`
	Tasks  []e2eTaskDetail `json:"tasks"`
}

func e2ePollInstance(t *testing.T, httpClient *http.Client, baseURL string, tenantID, instanceID uuid.UUID, ready func(e2eInstanceDetail) bool) e2eInstanceDetail {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	var detail e2eInstanceDetail
	for {
		req, err := http.NewRequest(http.MethodGet, baseURL+"/api/v1/instances/"+instanceID.String(), nil)
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		req.Header.Set("x-tenant-id", tenantID.String())
		req.Header.Set("x-user-id", uuid.NewString())
		req.Header.Set("x-tenant-roles", "tenant_admin")
		resp, err := httpClient.Do(req)
		if err != nil {
			t.Fatalf("GET /instances/:id: %v", err)
		}
		if err := json.NewDecoder(resp.Body).Decode(&detail); err != nil {
			resp.Body.Close()
			t.Fatalf("decode instance detail: %v", err)
		}
		resp.Body.Close()
		if ready(detail) {
			return detail
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out polling instance %s; last status = %q, tasks = %+v", instanceID, detail.Status, detail.Tasks)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

// e2ePostWorkflowTaskCreated stands in for event_consumer's forward of a
// WorkflowTaskCreated event onto POST /api/v1/internal/events/workflow-task
// — everything upstream of that HTTP call (the outbox relay, SNS, the
// consumer's own routing) is exercised elsewhere; this test's own scope
// starts at the point a connector-typed task actually needs to reach the
// Stream cmd/connector-worker consumes.
func e2ePostWorkflowTaskCreated(t *testing.T, httpClient *http.Client, baseURL string, tenantID uuid.UUID, task e2eTaskDetail, resolvedInputs map[string]any) {
	t.Helper()
	body := map[string]any{
		"id":        uuid.NewString(),
		"type":      "WorkflowTaskCreated",
		"tenant_id": tenantID.String(),
		"time":      time.Now().Format(time.RFC3339),
		"data": map[string]any{
			"workflow_instance_id": task.WorkflowInstanceID.String(),
			"task_id":              task.ID.String(),
			"node_key":             task.NodeKey,
			"department_id":        task.DepartmentID.String(),
			"connector_type":       "rest-call",
			"resolved_inputs":      resolvedInputs,
			"output_mapping":       []any{},
		},
	}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal WorkflowTaskCreated event: %v", err)
	}
	req, err := http.NewRequest(http.MethodPost, baseURL+"/api/v1/internal/events/workflow-task", bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-internal-token", e2eInternalToken)
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("POST /internal/events/workflow-task: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST /internal/events/workflow-task status = %d, want 200", resp.StatusCode)
	}
}

// TestE2E_ConnectorWorker_DispatchAndComplete runs cmd/connector-worker's
// real composition root (buildDeps + runDispatchLoop) against a real Valkey
// Stream and the real completion HTTP endpoint — the gap left by
// test/e2e.TestE2E_InstantiateDispatchComplete, whose Activities never touch
// this binary at all. Target: a real rest-call dispatch against a local
// HTTP server, chosen because it needs no cloud SDK/credentials, unlike
// storage/send-email's real providers.
func TestE2E_ConnectorWorker_DispatchAndComplete(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	sdk := fixtures.NewTestTemporalServer(t)
	valkeyAddr := fixtures.NewTestValkey(t)

	tenantID := uuid.New()
	departmentIAMID := uuid.New()
	versionID := uuid.New()
	streamKey := "connector-tasks-e2e-" + uuid.NewString()

	// target is the real HTTP endpoint the rest-call connector dispatches
	// to, standing in for another platform service's internal API.
	var gotToken, gotDepartments string
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotToken = r.Header.Get("x-internal-token")
		gotDepartments = r.Header.Get("x-departments")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(target.Close)

	// aliasSrv stands in for definition_service's own
	// GET /api/v1/internal/connector-aliases (item 0 of this session's own
	// routing fix) — the one alias resolves to target above.
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

	// The real connector-worker composition root, pointed at real test
	// infra: real Valkey, the fake alias registry, and the real execution
	// router's completion endpoints.
	cfg := &config.Config{
		ValkeyAddr:                        valkeyAddr,
		ValkeyDialTimeout:                 5 * time.Second,
		ValkeyReadTimeout:                 5 * time.Second,
		ValkeyWriteTimeout:                5 * time.Second,
		ConnectorStreamKey:                streamKey,
		ConnectorStreamGroup:              "connector-worker-e2e",
		ConnectorStreamBlockTimeout:       time.Second,
		ConnectorStreamClaimMinIdle:       30 * time.Second,
		ConnectorStreamBatchSize:          10,
		DefinitionServiceInternalHTTPAddr: aliasSrv.URL,
		ConnectorAliasFetchTimeout:        5 * time.Second,
		InternalAPIToken:                  e2eInternalToken,
		OpenBaoAddr:                       "http://unused.invalid",
		OpenBaoMount:                      "secret",
		OpenBaoTimeout:                    5 * time.Second,
		ExecutionServiceInternalAddr:      execSrv.URL,
		ConnectorCompletionTimeout:        5 * time.Second,
		ConnectorPoolSizeRestCall:         2,
		ConnectorTimeoutRestCall:          10 * time.Second,
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
	var startResp struct {
		ID uuid.UUID `json:"id"`
	}
	startReq, err := http.NewRequest(http.MethodPost, execSrv.URL+"/api/v1/instances", bytes.NewReader(mustMarshalPlanBody(t, map[string]any{
		"business_key":        "connector-e2e-" + uuid.NewString(),
		"workflow_version_id": versionID,
		"context_json":        map[string]any{"aliasVar": "echo"},
	})))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	startReq.Header.Set("Content-Type", "application/json")
	startReq.Header.Set("x-tenant-id", tenantID.String())
	startReq.Header.Set("x-user-id", uuid.NewString())
	startReq.Header.Set("x-tenant-roles", "tenant_admin")
	startResp2, err := httpClient.Do(startReq)
	if err != nil {
		t.Fatalf("POST /instances: %v", err)
	}
	if startResp2.StatusCode != http.StatusCreated {
		t.Fatalf("POST /instances status = %d, want 201", startResp2.StatusCode)
	}
	if err := json.NewDecoder(startResp2.Body).Decode(&startResp); err != nil {
		startResp2.Body.Close()
		t.Fatalf("decode start response: %v", err)
	}
	startResp2.Body.Close()
	instanceID := startResp.ID

	detail := e2ePollInstance(t, httpClient, execSrv.URL, tenantID, instanceID, func(d e2eInstanceDetail) bool {
		return len(d.Tasks) > 0 && d.Tasks[0].Status == "READY"
	})
	task := detail.Tasks[0]

	// Simulate event_consumer's own forward of the real WorkflowTaskCreated
	// event onto the endpoint that pushes onto the real Valkey Stream.
	e2ePostWorkflowTaskCreated(t, httpClient, execSrv.URL, tenantID, task, map[string]any{"endpointAlias": "echo"})

	e2ePollInstance(t, httpClient, execSrv.URL, tenantID, instanceID, func(d e2eInstanceDetail) bool {
		return d.Status == "COMPLETED"
	})

	if gotToken != e2eInternalToken {
		t.Errorf("rest-call dispatch's x-internal-token = %q, want %q", gotToken, e2eInternalToken)
	}
	if gotDepartments != departmentIAMID.String() {
		t.Errorf("rest-call dispatch's x-departments = %q, want %q", gotDepartments, departmentIAMID.String())
	}
}

func mustMarshalPlan(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(b)
}

func mustMarshalPlanBody(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}
