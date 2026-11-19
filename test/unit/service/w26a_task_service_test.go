package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

var (
	errW26aCacheRead  = errors.New("w26a: valkey read timed out")
	errW26aCacheWrite = errors.New("w26a: valkey write timed out")
)

type w26aLookup struct{ tenant, id uuid.UUID }

// w26aTasks is a tenant-scoped task store: a row of another tenant is as absent as an unknown id.
type w26aTasks struct {
	port.TaskRepository
	rows  []*domain.Task
	reads []w26aLookup
}

func (r *w26aTasks) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Task, error) {
	r.reads = append(r.reads, w26aLookup{tenantID, id})
	for _, t := range r.rows {
		if t.ID == id && t.TenantID == tenantID {
			return t, nil
		}
	}
	return nil, domain.ErrNotFound
}

type w26aActivePage struct {
	tenant, user uuid.UUID
	page         port.PageRequest
}

// w26aAssignments lists active assignments in insertion order, and answers the user-paged read from a script.
type w26aAssignments struct {
	port.TaskAssignmentRepository
	rows      []*domain.TaskAssignment
	lists     []w26aLookup
	counts    []w26aLookup
	pageRows  []port.ActiveUserTaskRow
	pageNext  *port.Cursor
	userPages []w26aActivePage
}

func (r *w26aAssignments) ListActiveByTask(_ context.Context, tenantID, taskID uuid.UUID) ([]*domain.TaskAssignment, error) {
	r.lists = append(r.lists, w26aLookup{tenantID, taskID})
	var out []*domain.TaskAssignment
	for _, a := range r.rows {
		if a.TenantID == tenantID && a.TaskID == taskID && a.IsActive {
			out = append(out, a)
		}
	}
	return out, nil
}

func (r *w26aAssignments) CountActiveByTasks(_ context.Context, tenantID uuid.UUID, taskIDs []uuid.UUID) (map[uuid.UUID]int, error) {
	for _, id := range taskIDs {
		r.counts = append(r.counts, w26aLookup{tenantID, id})
	}
	out := map[uuid.UUID]int{}
	for _, a := range r.rows {
		if a.TenantID == tenantID && a.IsActive {
			out[a.TaskID]++
		}
	}
	return out, nil
}

func (r *w26aAssignments) ListActiveByUserPaginated(_ context.Context, tenantID, userID uuid.UUID, page port.PageRequest) ([]port.ActiveUserTaskRow, *port.Cursor, error) {
	r.userPages = append(r.userPages, w26aActivePage{tenantID, userID, page})
	return r.pageRows, r.pageNext, nil
}

type w26aInstances struct {
	port.InstanceRepository
	rows  []*domain.Instance
	reads []w26aLookup
}

func (r *w26aInstances) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	r.reads = append(r.reads, w26aLookup{tenantID, id})
	for _, i := range r.rows {
		if i.ID == id && i.TenantID == tenantID {
			return i, nil
		}
	}
	return nil, domain.ErrNotFound
}

type w26aSignal struct {
	workflowID string
	instanceID uuid.UUID
	name       string
	payload    any
}

type w26aTemporal struct {
	port.TemporalClient
	signals []w26aSignal
}

func (c *w26aTemporal) SignalWorkflow(_ context.Context, workflowID string, instanceID uuid.UUID, name string, payload any) error {
	c.signals = append(c.signals, w26aSignal{workflowID, instanceID, name, payload})
	return nil
}

type w26aCacheSet struct{ key, value string }

// w26aCache is a map-backed cache whose reservation can be refused and whose read and write can fail.
type w26aCache struct {
	port.CacheStore
	values      map[string]string
	refuseSetNX bool
	getErr      error
	setErr      error
	setNXKeys   []string
	gets        []string
	sets        []w26aCacheSet
	dels        []string
}

func (c *w26aCache) SetNX(_ context.Context, key, value string, _ time.Duration) (bool, error) {
	c.setNXKeys = append(c.setNXKeys, key)
	if _, held := c.values[key]; held || c.refuseSetNX {
		return false, nil
	}
	c.values[key] = value
	return true, nil
}

func (c *w26aCache) Get(_ context.Context, key string) (string, error) {
	c.gets = append(c.gets, key)
	if c.getErr != nil {
		return "", c.getErr
	}
	return c.values[key], nil
}

func (c *w26aCache) Set(_ context.Context, key, value string, _ time.Duration) error {
	c.sets = append(c.sets, w26aCacheSet{key, value})
	if c.setErr != nil {
		return c.setErr
	}
	c.values[key] = value
	return nil
}

func (c *w26aCache) Del(_ context.Context, keys ...string) error {
	c.dels = append(c.dels, keys...)
	for _, k := range keys {
		delete(c.values, k)
	}
	return nil
}

type w26aWarn struct {
	msg    string
	fields map[string]any
}

type w26aLog struct{ warns []w26aWarn }

func (l *w26aLog) Debug(string, map[string]any) {}
func (l *w26aLog) Info(string, map[string]any)  {}
func (l *w26aLog) Error(string, map[string]any) {}
func (l *w26aLog) Warn(msg string, fields map[string]any) {
	l.warns = append(l.warns, w26aWarn{msg, fields})
}

// w26aWorld is one tenant's running instance with one ready single-assignee task held by one user.
type w26aWorld struct {
	svc         *service.TaskService
	tasks       *w26aTasks
	assignments *w26aAssignments
	instances   *w26aInstances
	temporal    *w26aTemporal
	log         *w26aLog
	tenant      uuid.UUID
	instance    *domain.Instance
	task        *domain.Task
	user        uuid.UUID
}

const w26aTaskVersion = 3

func w26aNewWorld() *w26aWorld {
	w := &w26aWorld{tenant: uuid.New(), user: uuid.New(), log: &w26aLog{}}
	w.instance = &domain.Instance{
		ID: uuid.New(), TenantID: w.tenant, TemporalWorkflowID: "tenant:subject:key", Status: domain.InstanceStatusRunning,
	}
	w.task = &domain.Task{
		ID: uuid.New(), TenantID: w.tenant, WorkflowInstanceID: w.instance.ID, DepartmentID: uuid.New(),
		NodeKey: "finance/review", Status: domain.TaskStatusReady, AssigneeMode: "single", RecordVersion: w26aTaskVersion,
	}
	w.tasks = &w26aTasks{rows: []*domain.Task{w.task}}
	w.assignments = &w26aAssignments{rows: []*domain.TaskAssignment{w26aAssignment(w.tenant, w.task.ID, w.user)}}
	w.instances = &w26aInstances{rows: []*domain.Instance{w.instance}}
	w.temporal = &w26aTemporal{}
	w.svc = &service.TaskService{
		Instances: w.instances, Tasks: w.tasks, Assignments: w.assignments, Temporal: w.temporal, Log: w.log,
	}
	return w
}

func w26aAssignment(tenant, taskID, userID uuid.UUID) *domain.TaskAssignment {
	return &domain.TaskAssignment{ID: uuid.New(), TenantID: tenant, TaskID: taskID, UserID: userID, IsActive: true}
}

func TestTaskService_Get_SeveralActiveAssignees_ReturnsEveryActiveAssignmentToAnyOfThem(t *testing.T) {
	cases := []struct {
		name   string
		caller int // index into the assignees, in the order the repository lists them
	}{
		{name: "the first assignee listed", caller: 0},
		{name: "a middle assignee", caller: 1},
		{name: "the last assignee listed", caller: 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := w26aNewWorld()
			w.task.AssigneeMode = "all"
			users := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
			w.assignments.rows = nil
			var active []uuid.UUID
			for _, u := range users {
				a := w26aAssignment(w.tenant, w.task.ID, u)
				w.assignments.rows = append(w.assignments.rows, a)
				active = append(active, a.ID)
			}
			vacated := w26aAssignment(w.tenant, w.task.ID, uuid.New())
			vacated.IsActive = false
			w.assignments.rows = append(w.assignments.rows, vacated)

			got, assignments, err := w.svc.Get(context.Background(), w.tenant, w.task.ID, port.ReadScope{CallerUserID: users[tc.caller]})

			require.NoError(t, err)
			assert.Equal(t, w.task.ID, got.ID)
			assert.Equal(t, 3, got.AssigneeCount)
			gotIDs := make([]uuid.UUID, len(assignments))
			for i, a := range assignments {
				gotIDs[i] = a.ID
			}
			assert.Equal(t, active, gotIDs, "every active assignment is returned, the vacated one is not")
			assert.Equal(t, []w26aLookup{{w.tenant, w.task.ID}}, w.assignments.lists, "assignments are read once, in the caller's tenant")
		})
	}
}

func TestTaskService_Get_ACallerWhoseAssignmentWasVacated_IsNotInScope(t *testing.T) {
	w := w26aNewWorld()
	w.assignments.rows[0].IsActive = false

	_, _, err := w.svc.Get(context.Background(), w.tenant, w.task.ID, port.ReadScope{CallerUserID: w.user})

	require.ErrorIs(t, err, port.ErrNotAuthorizedForRead)
}

func TestTaskService_Get_ATaskInATerminalStatus_IsReadWithItsStatusAndCompletionTime(t *testing.T) {
	completedAt := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	cases := []struct {
		name  string
		scope func(w *w26aWorld) port.ReadScope
	}{
		{name: "an active assignee", scope: func(w *w26aWorld) port.ReadScope { return port.ReadScope{CallerUserID: w.user} }},
		{name: "an admin who is not an assignee", scope: func(*w26aWorld) port.ReadScope {
			return port.ReadScope{CallerUserID: uuid.New(), IsAdmin: true}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := w26aNewWorld()
			w.task.Status = domain.TaskStatusCompleted
			w.task.CompletedAt = &completedAt

			got, assignments, err := w.svc.Get(context.Background(), w.tenant, w.task.ID, tc.scope(w))

			require.NoError(t, err)
			assert.Equal(t, port.TaskStatusCompleted, got.Status)
			require.NotNil(t, got.CompletedAt)
			assert.True(t, completedAt.Equal(*got.CompletedAt))
			assert.Equal(t, []w26aLookup{{w.tenant, w.task.ID}}, w.tasks.reads)
			require.Len(t, assignments, 1)
			assert.Equal(t, w.user, assignments[0].UserID)
		})
	}
}

func TestTaskService_ActiveByUser_TranslatesTheRequestCursorAndTheNextCursor(t *testing.T) {
	requestPos := port.CursorPosition{CreatedAt: time.Date(2026, 5, 6, 7, 8, 9, 0, time.UTC), ID: uuid.New()}
	nextPos := port.Cursor{CreatedAt: time.Date(2026, 5, 6, 7, 9, 0, 0, time.UTC), ID: uuid.New()}
	cases := []struct {
		name      string
		page      port.Page
		next      *port.Cursor
		wantAfter *port.Cursor
		wantNext  bool
	}{
		{name: "first page with no further page", page: port.Page{Limit: 10}},
		{name: "first page with a further page", page: port.Page{Limit: 10}, next: &nextPos, wantNext: true},
		{
			name:      "a later page with a further page",
			page:      port.Page{Limit: 7, Cursor: &requestPos},
			next:      &nextPos,
			wantAfter: &port.Cursor{CreatedAt: requestPos.CreatedAt, ID: requestPos.ID},
			wantNext:  true,
		},
		{
			name:      "the last page",
			page:      port.Page{Limit: 7, Cursor: &requestPos},
			wantAfter: &port.Cursor{CreatedAt: requestPos.CreatedAt, ID: requestPos.ID},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := w26aNewWorld()
			userID := uuid.New()
			row := port.ActiveUserTaskRow{TaskID: uuid.New(), UserID: userID}
			w.assignments.pageRows = []port.ActiveUserTaskRow{row}
			w.assignments.pageNext = tc.next

			res, err := w.svc.ActiveByUser(context.Background(), w.tenant, userID, tc.page)

			require.NoError(t, err)
			require.Len(t, w.assignments.userPages, 1)
			call := w.assignments.userPages[0]
			assert.Equal(t, w.tenant, call.tenant)
			assert.Equal(t, userID, call.user)
			assert.Equal(t, tc.page.Limit, call.page.Limit)
			assert.Equal(t, tc.wantAfter, call.page.After)
			require.Len(t, res.Items, 1)
			assert.Equal(t, row.TaskID, res.Items[0].TaskID)
			if !tc.wantNext {
				assert.Empty(t, res.NextCursor, "no further page means no cursor")
				return
			}
			decoded, err := port.DecodeCursor(res.NextCursor)
			require.NoError(t, err)
			assert.True(t, nextPos.CreatedAt.Equal(decoded.CreatedAt))
			assert.Equal(t, nextPos.ID, decoded.ID)
		})
	}
}

func w26aWarnWithError(log *w26aLog, msg string, cause error) bool {
	for _, w := range log.warns {
		if w.msg == msg && w.fields["error"] == cause.Error() {
			return true
		}
	}
	return false
}

func TestTaskService_Complete_ReservationRefusedAndTheKeyReadFails_SignalsAndAnswersTheTask(t *testing.T) {
	w := w26aNewWorld()
	cache := &w26aCache{values: map[string]string{}, refuseSetNX: true, getErr: errW26aCacheRead}
	w.svc.Cache = cache
	key := "task-complete:" + w.task.ID.String()

	got, err := w.svc.Complete(context.Background(), w.tenant, w.task.ID, w.user, json.RawMessage(`{"ok":true}`), w26aTaskVersion)

	require.NoError(t, err, "a cache outage must not block the completion")
	assert.Equal(t, w.task.ID, got.ID)
	assert.Equal(t, []string{key}, cache.setNXKeys, "the reservation was tried for this task")
	assert.Equal(t, []string{key}, cache.gets, "the held key was read for this task")
	require.Len(t, w.temporal.signals, 1, "the completion goes ahead")
	sent := w.temporal.signals[0]
	assert.Equal(t, w.instance.TemporalWorkflowID, sent.workflowID)
	assert.Equal(t, w.instance.ID, sent.instanceID)
	assert.Equal(t, "stage-transition", sent.name)
	raw, err := json.Marshal(sent.payload)
	require.NoError(t, err)
	var wire struct {
		UserID        string
		RecordVersion int64
	}
	require.NoError(t, json.Unmarshal(raw, &wire))
	assert.Equal(t, w.user.String(), wire.UserID)
	assert.EqualValues(t, w26aTaskVersion, wire.RecordVersion)
	assert.True(t, w26aWarnWithError(w.log, "task completion dedupe read failed, proceeding fail-open", errW26aCacheRead), "the failed read is logged as a warning")
}

func TestTaskService_Complete_MarkingTheKeyDeliveredFails_AnswersUnchangedKeepsTheReservationAndWarns(t *testing.T) {
	w := w26aNewWorld()
	cache := &w26aCache{values: map[string]string{}, setErr: errW26aCacheWrite}
	w.svc.Cache = cache
	key := "task-complete:" + w.task.ID.String()

	got, err := w.svc.Complete(context.Background(), w.tenant, w.task.ID, w.user, json.RawMessage(`{}`), w26aTaskVersion)

	require.NoError(t, err, "the signal went out, so the answer is unchanged")
	assert.Equal(t, w.task.ID, got.ID)
	require.Len(t, w.temporal.signals, 1)
	assert.Equal(t, []w26aCacheSet{{key, "delivered"}}, cache.sets, "the delivered mark was attempted for this task")
	assert.Equal(t, "reserved", cache.values[key], "the reservation stays until it expires")
	assert.Empty(t, cache.dels, "the reservation is not released after a sent signal")
	assert.True(t, w26aWarnWithError(w.log, "task completion dedupe could not be marked delivered", errW26aCacheWrite))

	_, err = w.svc.Complete(context.Background(), w.tenant, w.task.ID, w.user, json.RawMessage(`{}`), w26aTaskVersion)

	require.ErrorIs(t, err, port.ErrCallbackInFlight, "until the reservation expires a repeat is told to retry later")
	assert.Len(t, w.temporal.signals, 1, "the repeat sends no second signal")
}
