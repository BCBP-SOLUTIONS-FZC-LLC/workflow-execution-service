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
)

var (
	errW18bDatabase = errors.New("w18b: connection to 10.9.8.7:5432 lost, password=hunter2")
	errW18bSignal   = errors.New("w18b: dial tcp 10.1.2.3:7233: connect refused")
)

type w18bProblem struct {
	Type     string `json:"type"`
	Title    string `json:"title"`
	Status   int    `json:"status"`
	Detail   string `json:"detail"`
	Instance string `json:"instance"`
	Code     string `json:"code"`
}

func w18bDecodeProblem(t *testing.T, w *httptest.ResponseRecorder, status int) w18bProblem {
	t.Helper()
	require.Equal(t, status, w.Code, w.Body.String())
	assert.Equal(t, "application/problem+json", w.Header().Get("Content-Type"))
	var p w18bProblem
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &p), w.Body.String())
	assert.Equal(t, status, p.Status)
	for _, secret := range []string{"hunter2", "10.9.8.7", "10.1.2.3", "w18b:"} {
		assert.NotContains(t, w.Body.String(), secret, "an internal error text never reaches the caller")
	}
	return p
}

type w18bRead struct{ tenantID, id uuid.UUID }

type w18bListCall struct {
	tenantID uuid.UUID
	filter   port.InstanceListFilter
	page     port.PageRequest
}

type w18bInstances struct {
	port.InstanceRepository
	rows      []*domain.Instance
	getErr    error
	listRows  []*domain.Instance
	listNext  *port.Cursor
	listErr   error
	gets      []w18bRead
	listCalls []w18bListCall
}

func (r *w18bInstances) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	r.gets = append(r.gets, w18bRead{tenantID, id})
	if r.getErr != nil {
		return nil, r.getErr
	}
	for _, inst := range r.rows {
		if inst.TenantID == tenantID && inst.ID == id {
			return inst, nil
		}
	}
	return nil, domain.ErrNotFound
}

func (r *w18bInstances) ListByTenant(_ context.Context, tenantID uuid.UUID, filter port.InstanceListFilter, page port.PageRequest) ([]*domain.Instance, *port.Cursor, error) {
	r.listCalls = append(r.listCalls, w18bListCall{tenantID, filter, page})
	if r.listErr != nil {
		return nil, nil, r.listErr
	}
	return r.listRows, r.listNext, nil
}

type w18bTaskPageCall struct {
	tenantID, instanceID uuid.UUID
	after                *port.Cursor
}

type w18bTasks struct {
	port.TaskRepository
	pages   [][]*domain.Task
	failAt  int
	err     error
	listing []w18bTaskPageCall
}

func (r *w18bTasks) ListByInstance(_ context.Context, tenantID, instanceID uuid.UUID, page port.PageRequest) ([]*domain.Task, *port.Cursor, error) {
	r.listing = append(r.listing, w18bTaskPageCall{tenantID, instanceID, page.After})
	call := len(r.listing)
	if r.err != nil && call == r.failAt {
		return nil, nil, r.err
	}
	if call > len(r.pages) {
		return nil, nil, nil
	}
	rows := r.pages[call-1]
	if call == len(r.pages) {
		return rows, nil, nil
	}
	return rows, &port.Cursor{CreatedAt: time.Unix(int64(call), 0).UTC(), ID: rows[len(rows)-1].ID}, nil
}

type w18bCountCall struct {
	tenantID uuid.UUID
	taskIDs  []uuid.UUID
}

type w18bAssignments struct {
	port.TaskAssignmentRepository
	err    error
	counts map[uuid.UUID]int
	calls  []w18bCountCall
}

func (r *w18bAssignments) CountActiveByTasks(_ context.Context, tenantID uuid.UUID, taskIDs []uuid.UUID) (map[uuid.UUID]int, error) {
	r.calls = append(r.calls, w18bCountCall{tenantID, append([]uuid.UUID(nil), taskIDs...)})
	if r.err != nil {
		return nil, r.err
	}
	return r.counts, nil
}

type w18bOutbox struct {
	port.OutboxRepository
	rows  []*domain.OutboxEventRecord
	next  *port.Cursor
	reads []w18bRead
}

func (o *w18bOutbox) ListByInstance(_ context.Context, tenantID, instanceID uuid.UUID, _ port.PageRequest) ([]*domain.OutboxEventRecord, *port.Cursor, error) {
	o.reads = append(o.reads, w18bRead{tenantID, instanceID})
	return o.rows, o.next, nil
}

type w18bSignalCall struct {
	workflowID string
	instanceID uuid.UUID
	name       string
	payload    []byte
}

type w18bTemporal struct {
	port.TemporalClient
	err     error
	signals []w18bSignalCall
}

func (c *w18bTemporal) SignalWorkflow(_ context.Context, workflowID string, instanceID uuid.UUID, name string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	c.signals = append(c.signals, w18bSignalCall{workflowID, instanceID, name, raw})
	return c.err
}

type w18bWorld struct {
	inst        *domain.Instance
	instances   *w18bInstances
	tasks       *w18bTasks
	assignments *w18bAssignments
	outbox      *w18bOutbox
	temporal    *w18bTemporal
}

func w18bNewWorld(t *testing.T, status domain.InstanceStatus) (*w18bWorld, *gin.Engine) {
	t.Helper()
	inst := &domain.Instance{
		ID: testInstID, TenantID: testTenantID, Status: status, RecordVersion: 3,
		TemporalWorkflowID: "tenant:subject:w18b", WorkflowVersionID: uuid.New(),
	}
	w := &w18bWorld{
		inst:        inst,
		instances:   &w18bInstances{rows: []*domain.Instance{inst}},
		tasks:       &w18bTasks{},
		assignments: &w18bAssignments{counts: map[uuid.UUID]int{}},
		outbox:      &w18bOutbox{},
		temporal:    &w18bTemporal{},
	}
	svc := &service.InstanceService{
		Instances: w.instances, Tasks: w.tasks, Assignments: w.assignments, Outbox: w.outbox, Temporal: w.temporal,
		Log: &w18bQuietLogger{},
	}
	return w, newRouter(handler.New(handler.Services{Instances: svc}))
}

type w18bQuietLogger struct{}

func (*w18bQuietLogger) Debug(string, map[string]any) {}
func (*w18bQuietLogger) Info(string, map[string]any)  {}
func (*w18bQuietLogger) Warn(string, map[string]any)  {}
func (*w18bQuietLogger) Error(string, map[string]any) {}

func w18bTask(instanceID uuid.UUID) *domain.Task {
	return &domain.Task{
		ID: uuid.New(), TenantID: testTenantID, WorkflowInstanceID: instanceID, NodeKey: "dept/" + uuid.NewString()[:8],
		DepartmentID: uuid.New(), Status: domain.TaskStatusReady, RecordVersion: 1,
	}
}

func w18bDecodeKeys(t *testing.T, w *httptest.ResponseRecorder) map[string]json.RawMessage {
	t.Helper()
	var keys map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &keys), w.Body.String())
	return keys
}

func TestListInstances_WhenTheServiceReturnsNoCursor_BodyHasNoNextCursorKey(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		rows       int
		next       *port.Cursor
		wantCursor bool
	}{
		{name: "some rows", rows: 2},
		{name: "no rows at all", rows: 0},
		{name: "control: a repository cursor is passed on", rows: 3, next: &port.Cursor{CreatedAt: time.Unix(1_700_000_000, 0).UTC(), ID: uuid.New()}, wantCursor: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			world, router := w18bNewWorld(t, domain.InstanceStatusRunning)
			var ids []uuid.UUID
			for i := 0; i < tc.rows; i++ {
				inst := &domain.Instance{ID: uuid.New(), TenantID: testTenantID, Status: domain.InstanceStatusRunning, StartedByUserID: testUserID}
				world.instances.listRows = append(world.instances.listRows, inst)
				ids = append(ids, inst.ID)
			}
			world.instances.listNext = tc.next

			w := do(router, asAdmin(req(http.MethodGet, instancesPath()+"?limit=5", nil)))

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			keys := w18bDecodeKeys(t, w)
			var items []struct {
				ID uuid.UUID `json:"id"`
			}
			require.NoError(t, json.Unmarshal(keys["items"], &items))
			gotIDs := make([]uuid.UUID, len(items))
			for i, it := range items {
				gotIDs[i] = it.ID
			}
			if tc.rows == 0 {
				assert.JSONEq(t, `[]`, string(keys["items"]), "an empty page is an empty list")
			} else {
				assert.Equal(t, ids, gotIDs)
			}
			_, hasCursor := keys["next_cursor"]
			assert.Equal(t, tc.wantCursor, hasCursor, w.Body.String())
			if tc.wantCursor {
				var encoded string
				require.NoError(t, json.Unmarshal(keys["next_cursor"], &encoded))
				pos, err := port.DecodeCursor(encoded)
				require.NoError(t, err)
				assert.Equal(t, tc.next.ID, pos.ID)
			}
			require.Len(t, world.instances.listCalls, 1)
			assert.Equal(t, testTenantID, world.instances.listCalls[0].tenantID)
			assert.Equal(t, 5, world.instances.listCalls[0].page.Limit)
		})
	}
}

func TestGetInstance_AReadFailsInTheDatabase_Returns500WithTheGenericProblemAndNoPartialDetail(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		setup func(w *w18bWorld, firstPage, secondPage []*domain.Task)
		// wantTaskPages is how many task pages the service asked for; wantCounts how many count reads.
		wantTaskPages int
		wantCounts    bool
	}{
		{
			name:          "the instance lookup fails",
			setup:         func(w *w18bWorld, _, _ []*domain.Task) { w.instances.getErr = errW18bDatabase },
			wantTaskPages: 0,
		},
		{
			name: "the first page of tasks fails",
			setup: func(w *w18bWorld, first, second []*domain.Task) {
				w.tasks.pages = [][]*domain.Task{first, second}
				w.tasks.failAt, w.tasks.err = 1, errW18bDatabase
			},
			wantTaskPages: 1,
		},
		{
			name: "a later page of tasks fails after the first was read",
			setup: func(w *w18bWorld, first, second []*domain.Task) {
				w.tasks.pages = [][]*domain.Task{first, second}
				w.tasks.failAt, w.tasks.err = 2, errW18bDatabase
			},
			wantTaskPages: 2,
		},
		{
			name: "the assignee count fails after every task page was read",
			setup: func(w *w18bWorld, first, second []*domain.Task) {
				w.tasks.pages = [][]*domain.Task{first, second}
				w.assignments.err = errW18bDatabase
			},
			wantTaskPages: 2,
			wantCounts:    true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			world, router := w18bNewWorld(t, domain.InstanceStatusRunning)
			first := []*domain.Task{w18bTask(testInstID), w18bTask(testInstID)}
			second := []*domain.Task{w18bTask(testInstID)}
			tc.setup(world, first, second)

			w := do(router, asAdmin(req(http.MethodGet, instancePath(testInstID), nil)))

			p := w18bDecodeProblem(t, w, http.StatusInternalServerError)
			assert.Equal(t, "INTERNAL_ERROR", p.Code)
			assert.Equal(t, "https://errors.bcbp.io/execution/internal-error", p.Type)
			assert.Equal(t, "an unexpected error occurred", p.Detail)
			assert.Equal(t, instancePath(testInstID), p.Instance)
			assert.NotContains(t, w.Body.String(), `"tasks"`, "no partial task list")
			assert.NotContains(t, w.Body.String(), first[0].ID.String())
			assert.Equal(t, []w18bRead{{testTenantID, testInstID}}, world.instances.gets)
			require.Len(t, world.tasks.listing, tc.wantTaskPages)
			for _, call := range world.tasks.listing {
				assert.Equal(t, testTenantID, call.tenantID)
				assert.Equal(t, testInstID, call.instanceID)
			}
			if tc.wantTaskPages == 2 {
				require.NotNil(t, world.tasks.listing[1].after, "the second page continues from the first page's cursor")
				assert.Equal(t, first[1].ID, world.tasks.listing[1].after.ID)
			}
			if tc.wantCounts {
				require.Len(t, world.assignments.calls, 1, "one count read for the whole instance")
				assert.Equal(t, testTenantID, world.assignments.calls[0].tenantID)
				assert.Equal(t, []uuid.UUID{first[0].ID, first[1].ID, second[0].ID}, world.assignments.calls[0].taskIDs)
			} else {
				assert.Empty(t, world.assignments.calls)
			}
		})
	}
}

func TestListInstanceEvents_ALookupFailsInTheDatabase_Returns500AndReadsNoEvent(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		admin     bool
		setup     func(w *w18bWorld)
		wantGets  int
		wantLists int
	}{
		{
			name: "the instance lookup fails", admin: true,
			setup:    func(w *w18bWorld) { w.instances.getErr = errW18bDatabase },
			wantGets: 1,
		},
		{
			name: "the instance lookup fails for a non-admin, so no visibility lookup follows", admin: false,
			setup:    func(w *w18bWorld) { w.instances.getErr = errW18bDatabase },
			wantGets: 1,
		},
		{
			name: "the visibility lookup of a non-admin fails", admin: false,
			setup:     func(w *w18bWorld) { w.instances.listErr = errW18bDatabase },
			wantGets:  1,
			wantLists: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			world, router := w18bNewWorld(t, domain.InstanceStatusRunning)
			tc.setup(world)
			r := req(http.MethodGet, instanceEventsPath(testInstID), nil)
			if tc.admin {
				r = asAdmin(r)
			}

			w := do(router, r)

			p := w18bDecodeProblem(t, w, http.StatusInternalServerError)
			assert.Equal(t, "INTERNAL_ERROR", p.Code)
			assert.Equal(t, "https://errors.bcbp.io/execution/internal-error", p.Type)
			assert.Equal(t, "an unexpected error occurred", p.Detail)
			assert.NotContains(t, w.Body.String(), "INSTANCE_NOT_FOUND", "a failed lookup is not read as not found")
			assert.Len(t, world.instances.gets, tc.wantGets)
			assert.Len(t, world.instances.listCalls, tc.wantLists)
			assert.Empty(t, world.outbox.reads, "no event is read")
		})
	}
}

func TestListInstanceEvents_ANonAdminWithNoTaskInTheInstance_Returns403WithTheProblemBody(t *testing.T) {
	t.Parallel()

	world, router := w18bNewWorld(t, domain.InstanceStatusRunning)
	deptID := uuid.New()
	r := req(http.MethodGet, instanceEventsPath(testInstID), nil)
	r.Header.Set("x-departments", deptID.String()+":member")

	w := do(router, r)

	p := w18bDecodeProblem(t, w, http.StatusForbidden)
	assert.Equal(t, "NOT_AUTHORIZED_FOR_RESOURCE", p.Code)
	assert.Equal(t, "https://errors.bcbp.io/execution/not-authorized-for-resource", p.Type)
	assert.Equal(t, instanceEventsPath(testInstID), p.Instance)
	assert.Equal(t, []w18bRead{{testTenantID, testInstID}}, world.instances.gets)
	require.Len(t, world.instances.listCalls, 1)
	call := world.instances.listCalls[0]
	assert.Equal(t, testTenantID, call.tenantID)
	require.NotNil(t, call.filter.InstanceID)
	assert.Equal(t, testInstID, *call.filter.InstanceID)
	require.NotNil(t, call.filter.Scope)
	assert.Equal(t, testUserID, call.filter.Scope.CallerUserID)
	assert.Equal(t, []uuid.UUID{deptID}, call.filter.Scope.DepartmentIDs)
	assert.Empty(t, world.outbox.reads, "no event is read for a refused caller")
}

func TestListInstanceEvents_WhenTheServiceReturnsNoCursor_BodyHasNoNextCursorKey(t *testing.T) {
	t.Parallel()

	rec := func(n int) *domain.OutboxEventRecord {
		return &domain.OutboxEventRecord{
			ID: uuid.New(), EventType: domain.EventWorkflowInstanceStarted,
			Payload: json.RawMessage(fmt.Sprintf(`{"tenant_id":"t","data":{"n":%d}}`, n)), CreatedAt: time.Unix(int64(1_700_000_000+n), 0).UTC(),
		}
	}
	more := &port.Cursor{CreatedAt: time.Unix(1_700_000_100, 0).UTC(), ID: uuid.New()}
	cases := []struct {
		name       string
		rows       []*domain.OutboxEventRecord
		next       *port.Cursor
		wantCursor bool
	}{
		{name: "an instance with no events", rows: nil},
		{name: "the final items of the last page", rows: []*domain.OutboxEventRecord{rec(1), rec(2)}},
		{name: "control: a page with more behind it carries the cursor", rows: []*domain.OutboxEventRecord{rec(1)}, next: more, wantCursor: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			world, router := w18bNewWorld(t, domain.InstanceStatusRunning)
			world.outbox.rows, world.outbox.next = tc.rows, tc.next

			w := do(router, asAdmin(req(http.MethodGet, instanceEventsPath(testInstID), nil)))

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			keys := w18bDecodeKeys(t, w)
			var items []struct {
				ID      uuid.UUID       `json:"id"`
				Payload json.RawMessage `json:"payload_json"`
			}
			require.NoError(t, json.Unmarshal(keys["items"], &items))
			require.Len(t, items, len(tc.rows))
			if len(tc.rows) == 0 {
				assert.JSONEq(t, `[]`, string(keys["items"]))
			}
			for i, row := range tc.rows {
				assert.Equal(t, row.ID, items[i].ID)
			}
			_, hasCursor := keys["next_cursor"]
			assert.Equal(t, tc.wantCursor, hasCursor, w.Body.String())
			assert.Equal(t, []w18bRead{{testTenantID, testInstID}}, world.outbox.reads)
		})
	}
}

func TestResumeInstance_TheWorkflowSignalFails_Returns503WithAFixedDetailAndNoCause(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		cause string
	}{
		{"the workflow engine is unavailable", "transport is closing"},
		{"the workflow engine is out of resources", "namespace rps quota exceeded"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			world, router := w18bNewWorld(t, domain.InstanceStatusPaused)
			world.temporal.err = fmt.Errorf("signal workflow: %w: %w (%s)", port.ErrTemporalUnavailable, errW18bSignal, tc.cause)

			w := do(router, asAdmin(req(http.MethodPost, instancePath(testInstID)+"/resume", map[string]any{"record_version": 3})))

			p := w18bDecodeProblem(t, w, http.StatusServiceUnavailable)
			assert.Equal(t, "UPSTREAM_UNAVAILABLE", p.Code)
			assert.Equal(t, "the workflow engine could not take the request; retry it", p.Detail)
			assert.NotContains(t, w.Body.String(), tc.cause)
			assert.Equal(t, []w18bRead{{testTenantID, testInstID}}, world.instances.gets)
			require.Len(t, world.temporal.signals, 1, "the signal was attempted once")
			sent := world.temporal.signals[0]
			assert.Equal(t, "tenant:subject:w18b", sent.workflowID)
			assert.Equal(t, testInstID, sent.instanceID)
			assert.Equal(t, port.SignalInstanceResume, sent.name)
			var wire struct {
				AdminUserID   string
				Initiator     string
				RecordVersion int64
			}
			require.NoError(t, json.Unmarshal(sent.payload, &wire))
			assert.Equal(t, testUserID.String(), wire.AdminUserID)
			assert.Equal(t, "admin", wire.Initiator)
			assert.Equal(t, int64(3), wire.RecordVersion)
		})
	}
}
