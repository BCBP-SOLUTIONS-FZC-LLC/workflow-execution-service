package handler_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

var (
	errW10aTaskRead     = errors.New("w10a: task read failed: connection reset")
	errW10aSignalFailed = errors.New("w10a: signal refused by the workflow engine")
	errW10aCacheDelete  = errors.New("w10a: cache delete failed: valkey timeout")
	errW10aDefinitions  = errors.New("w10a: definitions connection refused")
)

const w10aUpstreamMetric = "workflow_execution_service_upstream_dependency_errors_total"

// w10aRead is one repository lookup: the tenant it was scoped to and the id it asked for.
type w10aRead struct {
	Tenant uuid.UUID
	ID     uuid.UUID
}

// w10aTasks is a tenant-scoped task store whose read can fail and whose row can vanish at the version bump.
// w10aBump is one version bump: the tenant, the task and the record version the caller expected.
type w10aBump struct {
	Tenant  uuid.UUID
	ID      uuid.UUID
	Version int64
}

type w10aTasks struct {
	port.TaskRepository
	rows         []*domain.Task
	getErr       error
	vanishOnBump bool
	reads        []w10aRead
	nodeReads    []w10aRead
	bumps        []w10aBump
}

func (r *w10aTasks) find(tenantID uuid.UUID, match func(*domain.Task) bool) (*domain.Task, error) {
	for _, t := range r.rows {
		if t.TenantID == tenantID && match(t) {
			return t, nil
		}
	}
	return nil, domain.ErrNotFound
}

func (r *w10aTasks) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Task, error) {
	r.reads = append(r.reads, w10aRead{tenantID, id})
	if r.getErr != nil {
		return nil, r.getErr
	}
	return r.find(tenantID, func(t *domain.Task) bool { return t.ID == id })
}

func (r *w10aTasks) GetByInstanceAndNode(_ context.Context, tenantID, instanceID uuid.UUID, nodeKey string) (*domain.Task, error) {
	r.nodeReads = append(r.nodeReads, w10aRead{tenantID, instanceID})
	return r.find(tenantID, func(t *domain.Task) bool { return t.WorkflowInstanceID == instanceID && t.NodeKey == nodeKey })
}

// BumpRecordVersion answers as the repository does for a row that is gone: not found.
func (r *w10aTasks) BumpRecordVersion(_ context.Context, tenantID, id uuid.UUID, recordVersion int64) (*domain.Task, error) {
	r.bumps = append(r.bumps, w10aBump{tenantID, id, recordVersion})
	if r.vanishOnBump {
		r.rows = nil
	}
	task, err := r.find(tenantID, func(t *domain.Task) bool { return t.ID == id })
	if err != nil {
		return nil, err
	}
	task.RecordVersion++
	return task, nil
}

type w10aAssignments struct {
	port.TaskAssignmentRepository
	rows  []*domain.TaskAssignment
	lists []w10aRead
}

func (r *w10aAssignments) ListActiveByTask(_ context.Context, tenantID, taskID uuid.UUID) ([]*domain.TaskAssignment, error) {
	r.lists = append(r.lists, w10aRead{tenantID, taskID})
	var out []*domain.TaskAssignment
	for _, a := range r.rows {
		if a.TenantID == tenantID && a.TaskID == taskID && a.IsActive {
			out = append(out, a)
		}
	}
	return out, nil
}

type w10aInstances struct {
	port.InstanceRepository
	rows  []*domain.Instance
	reads []w10aRead
}

func (r *w10aInstances) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	r.reads = append(r.reads, w10aRead{tenantID, id})
	for _, i := range r.rows {
		if i.TenantID == tenantID && i.ID == id {
			return i, nil
		}
	}
	return nil, domain.ErrNotFound
}

type w10aWorkflowCall struct {
	WorkflowID string
	Instance   uuid.UUID
	Name       string
	Payload    any
}

type w10aTemporal struct {
	port.TemporalClient
	signalErr error
	signals   []w10aWorkflowCall
	queries   []string
}

func (c *w10aTemporal) SignalWorkflow(_ context.Context, workflowID string, instanceID uuid.UUID, name string, payload any) error {
	c.signals = append(c.signals, w10aWorkflowCall{workflowID, instanceID, name, payload})
	return c.signalErr
}

func (c *w10aTemporal) QueryWorkflow(_ context.Context, _, queryType string, _ any) error {
	c.queries = append(c.queries, queryType)
	return nil
}

// w10aCache keeps what SetNX reserved and records every key it was asked to reserve or delete. A failing
// delete leaves the key in place, the way an unreachable Valkey does.
type w10aCache struct {
	port.CacheStore
	values  map[string]string
	reserve []string
	dels    []string
	delErr  error
}

func newW10aCache() *w10aCache { return &w10aCache{values: map[string]string{}} }

func (c *w10aCache) Get(_ context.Context, key string) (string, error) { return c.values[key], nil }

func (c *w10aCache) Set(_ context.Context, key, value string, _ time.Duration) error {
	c.values[key] = value
	return nil
}

func (c *w10aCache) SetNX(_ context.Context, key, value string, _ time.Duration) (bool, error) {
	c.reserve = append(c.reserve, key)
	if _, held := c.values[key]; held {
		return false, nil
	}
	c.values[key] = value
	return true, nil
}

func (c *w10aCache) Del(_ context.Context, keys ...string) error {
	c.dels = append(c.dels, keys...)
	if c.delErr != nil {
		return c.delErr
	}
	for _, k := range keys {
		delete(c.values, k)
	}
	return nil
}

type w10aDefinitions struct {
	resp  *port.CompiledWorkflow
	err   error
	calls []w10aRead
}

func (d *w10aDefinitions) GetCompiledWorkflow(_ context.Context, tenantID, versionID uuid.UUID) (*port.CompiledWorkflow, error) {
	d.calls = append(d.calls, w10aRead{tenantID, versionID})
	return d.resp, d.err
}

type w10aEligibility struct {
	port.EligibilityChecker
	calls []w10aRead
}

func (e *w10aEligibility) CheckEligibility(_ context.Context, tenantID, userID, _ uuid.UUID, _ string) (bool, error) {
	e.calls = append(e.calls, w10aRead{tenantID, userID})
	return true, nil
}

type w10aTx struct {
	runs int
	errs []error
}

func (t *w10aTx) RunInTx(ctx context.Context, fn func(context.Context) error) error {
	t.runs++
	err := fn(ctx)
	t.errs = append(t.errs, err)
	return err
}

func (t *w10aTx) RunInTxWithRetry(ctx context.Context, fn func(context.Context) error) error {
	return t.RunInTx(ctx, fn)
}

type w10aOverrides struct {
	port.AssigneeOverrideRepository
	creates int
}

func (o *w10aOverrides) Create(context.Context, *domain.AssigneeOverride) error {
	o.creates++
	return nil
}

type w10aSignalOutbox struct{ queued int }

func (o *w10aSignalOutbox) EnqueueSignal(context.Context, port.WorkflowSignal) error {
	o.queued++
	return nil
}

// w10aWorld is one tenant's running instance with one ready human task assigned to the calling user,
// plus every collaborator the task service uses.
type w10aWorld struct {
	tasks       *w10aTasks
	assignments *w10aAssignments
	instances   *w10aInstances
	temporal    *w10aTemporal
	cache       *w10aCache
	definitions *w10aDefinitions
	eligibility *w10aEligibility
	tx          *w10aTx
	overrides   *w10aOverrides
	outbox      *w10aSignalOutbox
	task        *domain.Task
}

const w10aTaskVersion = 3

func newW10aWorld() *w10aWorld {
	versionID := uuid.New()
	inst := &domain.Instance{
		ID: testInstID, TenantID: testTenantID, WorkflowVersionID: versionID,
		TemporalWorkflowID: "tenant:subject:key", Status: domain.InstanceStatusRunning,
	}
	task := &domain.Task{
		ID: testTaskID, TenantID: testTenantID, WorkflowInstanceID: testInstID, NodeKey: "finance/review",
		Status: domain.TaskStatusReady, AssigneeMode: "single", RecordVersion: w10aTaskVersion,
	}
	return &w10aWorld{
		tasks: &w10aTasks{rows: []*domain.Task{task}},
		assignments: &w10aAssignments{rows: []*domain.TaskAssignment{
			{ID: uuid.New(), TenantID: testTenantID, TaskID: testTaskID, UserID: testUserID, IsActive: true},
		}},
		instances:   &w10aInstances{rows: []*domain.Instance{inst}},
		temporal:    &w10aTemporal{},
		cache:       newW10aCache(),
		definitions: &w10aDefinitions{},
		eligibility: &w10aEligibility{},
		tx:          &w10aTx{},
		overrides:   &w10aOverrides{},
		outbox:      &w10aSignalOutbox{},
		task:        task,
	}
}

// router serves the real task service over the world's fakes; withPlanCheck adds the eligibility check
// that reads the compiled plan.
func (w *w10aWorld) router(withPlanCheck bool) *gin.Engine {
	svc := &service.TaskService{
		Tasks: w.tasks, Assignments: w.assignments, Instances: w.instances, Temporal: w.temporal,
		Cache: w.cache, Overrides: w.overrides, Transactor: w.tx, Signals: w.outbox,
	}
	if withPlanCheck {
		svc.Definitions = w.definitions
		svc.Eligibility = w.eligibility
	}
	return newRouter(handler.New(handler.Services{Tasks: svc}))
}

func (w *w10aWorld) requireNoWorkflowEffect(t *testing.T) {
	t.Helper()
	assert.Empty(t, w.temporal.signals, "no signal may reach the workflow")
	assert.Zero(t, w.outbox.queued, "no signal may be queued")
	assert.Zero(t, w.overrides.creates, "no override row may be stored")
}

func TestCompleteTask_TaskReadFails_AnswersForTheCauseAndReachesNothingElse(t *testing.T) {
	cases := []struct {
		name       string
		readErr    error
		taskID     uuid.UUID
		wantStatus int
		wantCode   string
	}{
		{name: "the database read fails", readErr: errW10aTaskRead, taskID: testTaskID, wantStatus: http.StatusInternalServerError, wantCode: "INTERNAL_ERROR"},
		{name: "the task id does not exist", taskID: uuid.New(), wantStatus: http.StatusNotFound, wantCode: "TASK_NOT_FOUND"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newW10aWorld()
			w.tasks.getErr = tc.readErr

			rec := do(w.router(false), req(http.MethodPost, hreTaskPath(tc.taskID.String(), "complete"), map[string]any{"record_version": w10aTaskVersion}))

			require.Equal(t, tc.wantStatus, rec.Code, rec.Body.String())
			var body problemBody
			decodeJSON(t, rec.Body, &body)
			assert.Equal(t, tc.wantCode, body.Code)
			assert.NotContains(t, rec.Body.String(), "connection reset", "the cause of a failed read stays out of the answer")
			assert.Equal(t, []w10aRead{{testTenantID, tc.taskID}}, w.tasks.reads, "the task is read once, in the caller's tenant")
			assert.Empty(t, w.assignments.lists, "the assignee check is never reached")
			assert.Empty(t, w.instances.reads, "the instance is never read")
			assert.Empty(t, w.cache.reserve, "no completion is reserved")
			w.requireNoWorkflowEffect(t)
		})
	}
}

func TestCompleteTask_SignalFailsAndTheReservationCannotBeReleased_AnswersWithTheSignalErrorAndHoldsTheKey(t *testing.T) {
	w := newW10aWorld()
	w.temporal.signalErr = errW10aSignalFailed
	w.cache.delErr = errW10aCacheDelete
	router := w.router(false)
	complete := func() *httptest.ResponseRecorder {
		return do(router, req(http.MethodPost, hreTaskPath(testTaskID.String(), "complete"), map[string]any{"record_version": w10aTaskVersion}))
	}

	first := complete()

	require.Equal(t, http.StatusInternalServerError, first.Code, first.Body.String())
	var body problemBody
	decodeJSON(t, first.Body, &body)
	assert.Equal(t, "INTERNAL_ERROR", body.Code)
	assert.NotContains(t, first.Body.String(), "valkey timeout", "the failed release is not what the caller is told")
	assert.NotContains(t, first.Body.String(), "refused by the workflow engine")
	require.Len(t, w.temporal.signals, 1, "the signal was attempted once")
	sent := w.temporal.signals[0]
	assert.Equal(t, "tenant:subject:key", sent.WorkflowID, "the signal goes to the instance's workflow")
	assert.Equal(t, testInstID, sent.Instance)
	assert.Equal(t, "stage-transition", sent.Name)
	rawPayload, err := json.Marshal(sent.Payload)
	require.NoError(t, err)
	var wire struct {
		UserID        string
		RecordVersion int64
	}
	require.NoError(t, json.Unmarshal(rawPayload, &wire))
	assert.Equal(t, testUserID.String(), wire.UserID, "the signal names the completing user")
	assert.EqualValues(t, w10aTaskVersion, wire.RecordVersion, "the signal carries the version the caller sent")
	require.Len(t, w.cache.reserve, 1)
	assert.Contains(t, w.cache.reserve[0], testTaskID.String(), "the reservation is for this task")
	assert.Equal(t, w.cache.reserve, w.cache.dels, "the release asked for the very key that was reserved")
	assert.Len(t, w.cache.values, 1, "the key stays because the delete failed")

	complete()

	assert.Len(t, w.cache.reserve, 2, "the retry tried to reserve the same task again")
	assert.Equal(t, w.cache.reserve[0], w.cache.reserve[1])
	assert.Len(t, w.temporal.signals, 1, "the retry sends no second signal")
	assert.Len(t, w.cache.dels, 1, "the retry releases nothing")
	assert.Len(t, w.cache.values, 1, "the key is still held")
}

func TestReassignTask_TaskReadFails_Returns500AndReachesNothingElse(t *testing.T) {
	w := newW10aWorld()
	w.tasks.getErr = errW10aTaskRead

	rec := do(w.router(false), asAdmin(req(http.MethodPost, hreTaskPath(testTaskID.String(), "reassign"), map[string]any{
		"new_user_id": uuid.New(), "record_version": w10aTaskVersion,
	})))

	require.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
	var body problemBody
	decodeJSON(t, rec.Body, &body)
	assert.Equal(t, "INTERNAL_ERROR", body.Code)
	assert.NotContains(t, rec.Body.String(), "connection reset")
	assert.Equal(t, []w10aRead{{testTenantID, testTaskID}}, w.tasks.reads)
	assert.Empty(t, w.assignments.lists, "the assignees are never listed")
	assert.Empty(t, w.instances.reads, "the instance is never read")
	w.requireNoWorkflowEffect(t)
}

func TestOverrideNodeAssignee_CompiledPlanCannotBeServed_AnswersForTheCauseAndStoresNothing(t *testing.T) {
	obstest.Init(t)
	cases := []struct {
		name         string
		definitions  func(d *w10aDefinitions, versionID uuid.UUID)
		wantStatus   int
		wantCode     string
		wantUpstream float64
	}{
		{
			name: "the Definition Service is unreachable",
			definitions: func(d *w10aDefinitions, _ uuid.UUID) {
				d.err = fmt.Errorf("%w: %w", port.ErrDefinitionUnavailable, errW10aDefinitions)
			},
			wantStatus: http.StatusServiceUnavailable, wantCode: "UPSTREAM_UNAVAILABLE", wantUpstream: 1,
		},
		{
			name: "the Definition Service rejects the version",
			definitions: func(d *w10aDefinitions, _ uuid.UUID) {
				d.err = fmt.Errorf("%w: status 502", port.ErrDefinitionRejected)
			},
			wantStatus: http.StatusServiceUnavailable, wantCode: "UPSTREAM_UNAVAILABLE", wantUpstream: 1,
		},
		{
			name: "the served plan is not a plan",
			definitions: func(d *w10aDefinitions, versionID uuid.UUID) {
				d.resp = &port.CompiledWorkflow{VersionID: versionID, Status: "PUBLISHED", IsValid: true, CompiledPlanJSON: "{not json"}
			},
			wantStatus: http.StatusInternalServerError, wantCode: "INTERNAL_ERROR", wantUpstream: 0,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newW10aWorld()
			versionID := w.instances.rows[0].WorkflowVersionID
			tc.definitions(w.definitions, versionID)
			before := obstest.Sum(t, w10aUpstreamMetric, map[string]string{"dependency": "definition_service"})
			newUser := uuid.New()

			rec := do(w.router(true), asAdmin(req(http.MethodPost, overridePath(testInstID, "finance%2Freview"), map[string]any{
				"new_user_id": newUser, "record_version": w10aTaskVersion,
			})))

			require.Equal(t, tc.wantStatus, rec.Code, rec.Body.String())
			var body problemBody
			decodeJSON(t, rec.Body, &body)
			assert.Equal(t, tc.wantCode, body.Code)
			assert.Equal(t, before+tc.wantUpstream, obstest.Sum(t, w10aUpstreamMetric, map[string]string{"dependency": "definition_service"}))
			assert.Equal(t, []w10aRead{{testTenantID, versionID}}, w.definitions.calls, "the plan is read for this tenant and the instance's version")
			assert.Empty(t, w.eligibility.calls, "the eligibility check never starts without a plan")
			assert.Zero(t, w.tx.runs, "nothing is stored, so no transaction opens")
			assert.Empty(t, w.tasks.bumps, "the task version is not bumped")
			w.requireNoWorkflowEffect(t)
			assert.EqualValues(t, w10aTaskVersion, w.task.RecordVersion)
		})
	}
}

func TestOverrideNodeAssignee_TaskDeletedBeforeTheVersionBump_Returns404AndStoresNothing(t *testing.T) {
	w := newW10aWorld()
	w.tasks.vanishOnBump = true

	rec := do(w.router(false), asAdmin(req(http.MethodPost, overridePath(testInstID, "finance%2Freview"), map[string]any{
		"new_user_id": uuid.New(), "record_version": w10aTaskVersion,
	})))

	require.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())
	var body problemBody
	decodeJSON(t, rec.Body, &body)
	assert.Equal(t, "TASK_NOT_FOUND", body.Code)
	assert.Equal(t, []w10aRead{{testTenantID, testInstID}}, w.tasks.nodeReads, "the task was found by instance and node before it vanished")
	assert.Equal(t, []w10aBump{{testTenantID, testTaskID, w10aTaskVersion}}, w.tasks.bumps, "the bump was attempted once, for this task, at the version the admin sent")
	assert.Equal(t, 1, w.tx.runs)
	require.Len(t, w.tx.errs, 1)
	require.ErrorIs(t, w.tx.errs[0], port.ErrTaskNotFound, "the transaction ended with the failure, so it rolls back")
	w.requireNoWorkflowEffect(t)
}
