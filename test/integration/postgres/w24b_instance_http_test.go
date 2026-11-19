//go:build integration

package postgres_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

type w24bDefinitions struct {
	resp *port.CompiledWorkflow
}

// GetCompiledWorkflow answers only for the published version and refuses any
// other, so a start that asks for the wrong version fails instead of passing.
func (d w24bDefinitions) GetCompiledWorkflow(_ context.Context, tenantID, versionID uuid.UUID) (*port.CompiledWorkflow, error) {
	if tenantID == uuid.Nil || versionID != d.resp.VersionID {
		return nil, fmt.Errorf("w24b definitions: unexpected tenant %s or version %s", tenantID, versionID)
	}
	return d.resp, nil
}

type w24bTemporal struct {
	port.TemporalClient
	mu     sync.Mutex
	starts []port.StartWorkflowInput
}

func (c *w24bTemporal) StartWorkflow(_ context.Context, in port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.starts = append(c.starts, in)
	return port.StartWorkflowOutput{TemporalWorkflowID: in.TemporalWorkflowID, TemporalRunID: "run-" + in.InstanceID.String()}, nil
}

func (c *w24bTemporal) startedQueues() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var queues []string
	for _, in := range c.starts {
		queues = append(queues, in.TaskQueue)
	}
	return queues
}

type w24bGrants struct {
	mu      sync.Mutex
	granted bool
	asked   []uuid.UUID
}

func (g *w24bGrants) HasFeature(_ context.Context, tenantID uuid.UUID, feature string) (bool, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if feature != "dedicated_workflow_queue" {
		return false, fmt.Errorf("w24b grants: unexpected feature %q", feature)
	}
	g.asked = append(g.asked, tenantID)
	return g.granted, nil
}

func (g *w24bGrants) askedFor(tenantID uuid.UUID) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, asked := range g.asked {
		if asked == tenantID {
			return true
		}
	}
	return false
}

func (g *w24bGrants) set(granted bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.granted = granted
}

type w24bLogger struct {
	mu       sync.Mutex
	warnings []string
}

func (l *w24bLogger) Debug(string, map[string]any) {}
func (l *w24bLogger) Info(string, map[string]any)  {}
func (l *w24bLogger) Error(string, map[string]any) {}
func (l *w24bLogger) Warn(msg string, _ map[string]any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warnings = append(l.warnings, msg)
}

func (l *w24bLogger) warned() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.warnings...)
}

type w24bProblem struct {
	Status int    `json:"status"`
	Code   string `json:"code"`
	Detail string `json:"detail"`
}

// w24bInstanceRig serves the start and event-list routes over the real
// repositories of one Postgres container.
type w24bInstanceRig struct {
	superPool *pgcommon.Pool
	instances *postgres.InstanceRepo
	queues    *postgres.ActiveTaskQueueRepo
	outbox    *postgres.OutboxRepo
	tx        *postgres.Transactor
	temporal  *w24bTemporal
	grants    *w24bGrants
	log       *w24bLogger
	router    *gin.Engine
	versionID uuid.UUID
}

func w24bNewInstanceRig(t *testing.T) *w24bInstanceRig {
	t.Helper()
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	validator, err := eventbus.NewSchemaValidator()
	require.NoError(t, err)
	plan, err := json.Marshal(dsl.CompiledCollaboration{
		SchemaVersion: dsl.CurrentSchemaVersion,
		MainPlan:      "main",
		Plans:         []*dsl.CompiledPlan{{Name: "main", TaskQueue: "wf-queue-default"}},
	})
	require.NoError(t, err)
	rig := &w24bInstanceRig{
		superPool: superPool,
		instances: postgres.NewInstanceRepo(appPool),
		queues:    postgres.NewActiveTaskQueueRepo(appPool),
		outbox:    postgres.NewOutboxRepo(appPool),
		tx:        postgres.NewTransactor(appPool),
		temporal:  &w24bTemporal{},
		grants:    &w24bGrants{},
		log:       &w24bLogger{},
		versionID: uuid.New(),
	}
	svc := &service.InstanceService{
		Instances:   rig.instances,
		Tasks:       postgres.NewTaskRepo(appPool),
		Assignments: postgres.NewTaskAssignmentRepo(appPool),
		Outbox:      rig.outbox,
		Transactor:  rig.tx,
		Temporal:    rig.temporal,
		Definitions: w24bDefinitions{resp: &port.CompiledWorkflow{
			WorkflowID: uuid.New(), VersionID: rig.versionID, Status: "PUBLISHED", IsValid: true, CompiledPlanJSON: string(plan),
		}},
		Queues:    rig.queues,
		Grants:    rig.grants,
		Validator: validator,
		Log:       rig.log,
	}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.UseRawPath = true
	for _, mw := range gincommon.ProtectedMiddlewares(gincommon.Config{ServiceName: "workflow-execution-service", BuildVersion: "test", Domain: observability.Domain, Environment: "test"}) {
		r.Use(mw)
	}
	h := handler.New(handler.Services{Instances: svc})
	r.POST("/api/v1/instances", h.StartInstance)
	r.GET("/api/v1/instances/:id/events", h.ListInstanceEvents)
	rig.router = r
	return rig
}

func (rig *w24bInstanceRig) do(method, path string, tenant uuid.UUID, body any) *httptest.ResponseRecorder {
	var reader *bytes.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			panic(err)
		}
		reader = bytes.NewReader(raw)
	} else {
		reader = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, reader)
	req.Header.Set("x-tenant-id", tenant.String())
	req.Header.Set("x-user-id", uuid.NewString())
	req.Header.Set("x-tenant-roles", "tenant_admin")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	rig.router.ServeHTTP(rec, req)
	return rec
}

func (rig *w24bInstanceRig) start(tenant uuid.UUID, subject, businessKey string) *httptest.ResponseRecorder {
	return rig.do(http.MethodPost, "/api/v1/instances", tenant, map[string]any{
		"subject_id": subject, "business_key": businessKey, "workflow_version_id": rig.versionID, "context_json": map[string]any{},
	})
}

type w24bStarted struct {
	ID                 uuid.UUID `json:"id"`
	TenantID           uuid.UUID `json:"tenant_id"`
	TemporalWorkflowID string    `json:"temporal_workflow_id"`
	Status             string    `json:"status"`
}

func w24bDecodeStarted(t *testing.T, rec *httptest.ResponseRecorder) w24bStarted {
	t.Helper()
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	var started w24bStarted
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &started))
	return started
}

func w24bDecodeProblem(t *testing.T, rec *httptest.ResponseRecorder) w24bProblem {
	t.Helper()
	var p w24bProblem
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &p), rec.Body.String())
	return p
}

func (rig *w24bInstanceRig) countInstances(t *testing.T, tenant uuid.UUID) (n int) {
	t.Helper()
	require.NoError(t, rig.superPool.WithConn(context.Background(), func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx, `SELECT count(*) FROM workflow_instance WHERE tenant_id = $1`, tenant).Scan(&n)
	}))
	return n
}

func w24bExecAsSuper(t *testing.T, pool *pgcommon.Pool, statements ...string) {
	t.Helper()
	for _, stmt := range statements {
		w8bSuperExec(t, pool, stmt)
	}
}

func TestStartInstance_SameSubjectAndKeyLiveInAnotherTenant_IsNotADuplicate(t *testing.T) {
	rig := w24bNewInstanceRig(t)
	ctx := context.Background()
	tenantA, tenantB := uuid.New(), uuid.New()

	first := w24bDecodeStarted(t, rig.start(tenantA, "SUBJ-1", "stage-1"))
	second := w24bDecodeStarted(t, rig.start(tenantB, "SUBJ-1", "stage-1"))

	assert.NotEqual(t, first.ID, second.ID, "the other tenant gets an instance of its own")
	assert.Equal(t, tenantA, first.TenantID)
	assert.Equal(t, tenantB, second.TenantID)
	assert.NotEqual(t, first.TemporalWorkflowID, second.TemporalWorkflowID)

	liveA, err := rig.instances.GetLiveByBusinessKey(ctx, tenantA, "SUBJ-1", "stage-1")
	require.NoError(t, err)
	liveB, err := rig.instances.GetLiveByBusinessKey(ctx, tenantB, "SUBJ-1", "stage-1")
	require.NoError(t, err)
	assert.Equal(t, first.ID, liveA.ID)
	assert.Equal(t, second.ID, liveB.ID)
	assert.Equal(t, 1, rig.countInstances(t, tenantA))
	assert.Equal(t, 1, rig.countInstances(t, tenantB))

	t.Run("the same tenant asking again with different input is still a duplicate", func(t *testing.T) {
		rec := rig.do(http.MethodPost, "/api/v1/instances", tenantA, map[string]any{
			"subject_id": "SUBJ-1", "business_key": "stage-1", "workflow_version_id": rig.versionID, "context_json": map[string]any{"changed": true},
		})
		require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
		assert.Equal(t, "DUPLICATE_BUSINESS_KEY", w24bDecodeProblem(t, rec).Code)
		assert.Equal(t, 1, rig.countInstances(t, tenantA))
	})
}

func TestStartInstance_PreviousInstanceEnded_StartsANewInstance(t *testing.T) {
	rig := w24bNewInstanceRig(t)
	ctx := context.Background()

	for _, ended := range []domain.InstanceStatus{
		domain.InstanceStatusCompleted, domain.InstanceStatusTerminated, domain.InstanceStatusFailed,
	} {
		t.Run(string(ended), func(t *testing.T) {
			tenant := uuid.New()
			previous := w24bDecodeStarted(t, rig.start(tenant, "SUBJ-1", "stage-1"))
			stored, err := rig.instances.GetByID(ctx, tenant, previous.ID)
			require.NoError(t, err)
			_, err = rig.instances.UpdateStatus(ctx, tenant, previous.ID, ended, stored.RecordVersion)
			require.NoError(t, err)

			again := w24bDecodeStarted(t, rig.start(tenant, "SUBJ-1", "stage-1"))

			assert.NotEqual(t, previous.ID, again.ID, "an ended instance does not hold its key")
			assert.Equal(t, "RUNNING", again.Status)
			live, err := rig.instances.GetLiveByBusinessKey(ctx, tenant, "SUBJ-1", "stage-1")
			require.NoError(t, err)
			assert.Equal(t, again.ID, live.ID, "the new instance is the live holder of the key")
			old, err := rig.instances.GetByID(ctx, tenant, previous.ID)
			require.NoError(t, err)
			assert.Equal(t, ended, old.Status, "the ended instance is left as it was")
			assert.Equal(t, 2, rig.countInstances(t, tenant))
		})
	}
}

func TestStartInstance_TheTaskQueueRegistryFailing_Gives500OnlyWhenTheStartDependsOnIt(t *testing.T) {
	rig := w24bNewInstanceRig(t)
	ctx := context.Background()
	const app = "execution_app"

	requireInternal := func(t *testing.T, rec *httptest.ResponseRecorder) {
		t.Helper()
		require.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
		problem := w24bDecodeProblem(t, rec)
		assert.Equal(t, "INTERNAL_ERROR", problem.Code)
		assert.Equal(t, "an unexpected error occurred", problem.Detail)
		for _, leak := range []string{"active_task_queues", "permission denied", "SQLSTATE", "tenant task queue"} {
			assert.NotContains(t, rec.Body.String(), leak, "the cause stays out of the answer")
		}
	}

	t.Run("a failing queue lookup gives 500 and no instance", func(t *testing.T) {
		tenant := uuid.New()
		rig.grants.set(true)
		w24bExecAsSuper(t, rig.superPool, "REVOKE SELECT ON active_task_queues FROM "+app)
		t.Cleanup(func() { w24bExecAsSuper(t, rig.superPool, "GRANT SELECT ON active_task_queues TO "+app) })

		requireInternal(t, rig.start(tenant, "SUBJ-1", "stage-1"))

		assert.Zero(t, rig.countInstances(t, tenant), "the start stopped before an instance was written")
		assert.Empty(t, rig.temporal.startedQueues())
	})

	t.Run("a failing registration of the tenant queue gives 500 and no instance", func(t *testing.T) {
		tenant := uuid.New()
		rig.grants.set(true)
		w24bExecAsSuper(t, rig.superPool, "REVOKE INSERT ON active_task_queues FROM "+app)
		t.Cleanup(func() { w24bExecAsSuper(t, rig.superPool, "GRANT INSERT ON active_task_queues TO "+app) })

		requireInternal(t, rig.start(tenant, "SUBJ-1", "stage-1"))

		assert.Zero(t, rig.countInstances(t, tenant))
		assert.Empty(t, rig.temporal.startedQueues())
		_, err := rig.queues.GetByQueueName(ctx, domain.TenantTaskQueueName(tenant))
		require.ErrorIs(t, err, domain.ErrNotFound, "nothing was registered")
	})

	t.Run("a failing stop of routing is only logged and the start answers 201 on the plan queue", func(t *testing.T) {
		tenant := uuid.New()
		name := domain.TenantTaskQueueName(tenant)
		_, err := rig.queues.Register(ctx, tenant, name)
		require.NoError(t, err)
		rig.grants.set(false)
		w24bExecAsSuper(t, rig.superPool, "REVOKE UPDATE ON active_task_queues FROM "+app)
		t.Cleanup(func() { w24bExecAsSuper(t, rig.superPool, "GRANT UPDATE ON active_task_queues TO "+app) })

		started := w24bDecodeStarted(t, rig.start(tenant, "SUBJ-1", "stage-1"))

		assert.Equal(t, "RUNNING", started.Status)
		assert.Equal(t, []string{"wf-queue-default"}, rig.temporal.startedQueues())
		assert.True(t, rig.grants.askedFor(tenant), "the grant is read for the starting tenant")
		warned := rig.log.warned()
		require.Len(t, warned, 1, "the failed stop is logged once")
		assert.Contains(t, warned[0], "stop routing")
	})
}

func TestListInstanceEvents_WhenTheEventQueryFails_Gives500WithTheGenericProblem(t *testing.T) {
	rig := w24bNewInstanceRig(t)
	ctx := context.Background()
	tenant := uuid.New()
	started := w24bDecodeStarted(t, rig.start(tenant, "SUBJ-1", "stage-1"))
	path := "/api/v1/instances/" + started.ID.String() + "/events"

	healthy := rig.do(http.MethodGet, path, tenant, nil)
	require.Equal(t, http.StatusOK, healthy.Code, healthy.Body.String())
	var listed struct {
		Items []json.RawMessage `json:"items"`
	}
	require.NoError(t, json.Unmarshal(healthy.Body.Bytes(), &listed))
	require.NotEmpty(t, listed.Items, "the start's own event is listed while the table exists")

	w8bDropTable(t, rig.superPool, "outbox_events")

	rec := rig.do(http.MethodGet, path, tenant, nil)
	require.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
	problem := w24bDecodeProblem(t, rec)
	assert.Equal(t, "INTERNAL_ERROR", problem.Code)
	assert.Equal(t, "an unexpected error occurred", problem.Detail)
	for _, leak := range []string{"outbox_events", "does not exist", "SQLSTATE"} {
		assert.NotContains(t, rec.Body.String(), leak, "the database's own message is not echoed")
	}

	_, err := rig.instances.GetByID(ctx, tenant, started.ID)
	require.NoError(t, err, "the failure is the event query's alone: the instance is still readable")
}
