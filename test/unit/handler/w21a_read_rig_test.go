package handler_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

const (
	w21aBadRequestType = "https://errors.bcbp.io/execution/bad-request"
	w21aUpstreamType   = "https://errors.bcbp.io/execution/upstream-unavailable"
)

type w21aParam struct {
	Name   string `json:"name"`
	Reason string `json:"reason"`
}

type w21aProblem struct {
	Type          string      `json:"type"`
	Status        int         `json:"status"`
	Detail        string      `json:"detail"`
	Code          string      `json:"code"`
	InvalidParams []w21aParam `json:"invalid_params"`
}

type w21aLookup struct{ tenantID, id uuid.UUID }

type w21aInstanceList struct {
	tenantID uuid.UUID
	filter   port.InstanceListFilter
	page     port.PageRequest
}

type w21aTaskList struct {
	tenantID uuid.UUID
	filter   port.TaskListFilter
	page     port.PageRequest
}

type w21aCount struct {
	tenantID uuid.UUID
	taskIDs  []uuid.UUID
}

// w21aInstanceRepo answers reads from rows and records the tenant and ids of every call.
type w21aInstanceRepo struct {
	port.InstanceRepository
	rows  []*domain.Instance
	next  *port.Cursor
	gets  []w21aLookup
	lists []w21aInstanceList
}

func (r *w21aInstanceRepo) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	r.gets = append(r.gets, w21aLookup{tenantID, id})
	for _, row := range r.rows {
		if row.TenantID == tenantID && row.ID == id {
			return row, nil
		}
	}
	return nil, domain.ErrNotFound
}

func (r *w21aInstanceRepo) ListByTenant(_ context.Context, tenantID uuid.UUID, filter port.InstanceListFilter, page port.PageRequest) ([]*domain.Instance, *port.Cursor, error) {
	r.lists = append(r.lists, w21aInstanceList{tenantID, filter, page})
	return r.rows, r.next, nil
}

type w21aTaskRepo struct {
	port.TaskRepository
	rows         []*domain.Task
	next         *port.Cursor
	gets         []w21aLookup
	lists        []w21aTaskList
	byInstance   []w21aLookup
	byInstancePg []port.PageRequest
}

func (r *w21aTaskRepo) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Task, error) {
	r.gets = append(r.gets, w21aLookup{tenantID, id})
	for _, row := range r.rows {
		if row.TenantID == tenantID && row.ID == id {
			return row, nil
		}
	}
	return nil, domain.ErrNotFound
}

func (r *w21aTaskRepo) ListByTenant(_ context.Context, tenantID uuid.UUID, filter port.TaskListFilter, page port.PageRequest) ([]*domain.Task, *port.Cursor, error) {
	r.lists = append(r.lists, w21aTaskList{tenantID, filter, page})
	return r.rows, r.next, nil
}

func (r *w21aTaskRepo) ListByInstance(_ context.Context, tenantID, instanceID uuid.UUID, page port.PageRequest) ([]*domain.Task, *port.Cursor, error) {
	r.byInstance = append(r.byInstance, w21aLookup{tenantID, instanceID})
	r.byInstancePg = append(r.byInstancePg, page)
	return r.rows, nil, nil
}

type w21aAssignmentRepo struct {
	port.TaskAssignmentRepository
	active  []*domain.TaskAssignment
	counts  map[uuid.UUID]int
	lookups []w21aLookup
	tallies []w21aCount
}

func (r *w21aAssignmentRepo) ListActiveByTask(_ context.Context, tenantID, taskID uuid.UUID) ([]*domain.TaskAssignment, error) {
	r.lookups = append(r.lookups, w21aLookup{tenantID, taskID})
	return r.active, nil
}

func (r *w21aAssignmentRepo) CountActiveByTasks(_ context.Context, tenantID uuid.UUID, taskIDs []uuid.UUID) (map[uuid.UUID]int, error) {
	r.tallies = append(r.tallies, w21aCount{tenantID, taskIDs})
	return r.counts, nil
}

type w21aEventList struct {
	tenantID, instanceID uuid.UUID
	page                 port.PageRequest
}

type w21aOutbox struct {
	port.OutboxRepository
	rows  []*domain.OutboxEventRecord
	next  *port.Cursor
	lists []w21aEventList
}

func (o *w21aOutbox) ListByInstance(_ context.Context, tenantID, instanceID uuid.UUID, page port.PageRequest) ([]*domain.OutboxEventRecord, *port.Cursor, error) {
	o.lists = append(o.lists, w21aEventList{tenantID, instanceID, page})
	return o.rows, o.next, nil
}

// w21aRig puts the real instance and task services, over recording repositories, behind the public route table.
type w21aRig struct {
	instances   *w21aInstanceRepo
	tasks       *w21aTaskRepo
	assignments *w21aAssignmentRepo
	outbox      *w21aOutbox
	router      http.Handler
}

func w21aNewRig() *w21aRig {
	rig := &w21aRig{
		instances:   &w21aInstanceRepo{},
		tasks:       &w21aTaskRepo{},
		assignments: &w21aAssignmentRepo{},
		outbox:      &w21aOutbox{},
	}
	rig.router = newRouter(handler.New(handler.Services{
		Instances: &service.InstanceService{Instances: rig.instances, Tasks: rig.tasks, Assignments: rig.assignments, Outbox: rig.outbox},
		Tasks:     &service.TaskService{Tasks: rig.tasks, Assignments: rig.assignments},
	}))
	return rig
}

// get sends a GET as the standard caller; admin adds the tenant_admin role and headers are extra request headers.
func (r *w21aRig) get(target string, admin bool, headers map[string]string) *httptest.ResponseRecorder {
	return w21aServe(r.router, http.MethodGet, target, "", admin, headers)
}

func w21aServe(router http.Handler, method, target, body string, admin bool, headers map[string]string) *httptest.ResponseRecorder {
	hr := httptest.NewRequest(method, target, strings.NewReader(body))
	hr.Header.Set("Content-Type", "application/json")
	hr.Header.Set("x-tenant-id", testTenantID.String())
	hr.Header.Set("x-user-id", testUserID.String())
	if admin {
		hr.Header.Set("x-tenant-roles", "tenant_admin")
	}
	for k, v := range headers {
		hr.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, hr)
	return w
}

func w21aDecodeProblem(t *testing.T, w *httptest.ResponseRecorder) w21aProblem {
	t.Helper()
	assert.Equal(t, "application/problem+json", strings.Split(w.Header().Get("Content-Type"), ";")[0])
	var p w21aProblem
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &p), w.Body.String())
	assert.Equal(t, w.Code, p.Status)
	return p
}

func w21aObject(t *testing.T, raw []byte) map[string]json.RawMessage {
	t.Helper()
	var m map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &m), string(raw))
	return m
}

func w21aItems(t *testing.T, body map[string]json.RawMessage) []map[string]json.RawMessage {
	t.Helper()
	var items []map[string]json.RawMessage
	require.Contains(t, body, "items")
	require.NoError(t, json.Unmarshal(body["items"], &items), string(body["items"]))
	return items
}

func w21aKeys(m map[string]json.RawMessage) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// w21aWith returns the sorted union of base and extra, for comparing against w21aKeys.
func w21aWith(base []string, extra ...string) []string {
	out := append(append([]string{}, base...), extra...)
	sort.Strings(out)
	return out
}

func w21aString(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var s string
	require.NoError(t, json.Unmarshal(raw, &s), string(raw))
	return s
}

func w21aNumber(t *testing.T, raw json.RawMessage) int64 {
	t.Helper()
	var n int64
	require.NoError(t, json.Unmarshal(raw, &n), string(raw))
	return n
}

func w21aTime(t *testing.T, raw json.RawMessage) time.Time {
	t.Helper()
	var ts time.Time
	require.NoError(t, json.Unmarshal(raw, &ts), string(raw))
	return ts
}
