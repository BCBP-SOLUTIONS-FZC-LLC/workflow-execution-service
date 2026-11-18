package service_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
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

// w15aCache records every reservation attempt; the routes under test run
// without an Idempotency-Key, so none must arrive.
type w15aCache struct {
	setNXKeys []string
}

func (c *w15aCache) Get(context.Context, string) (string, error)              { return "", nil }
func (c *w15aCache) Set(context.Context, string, string, time.Duration) error { return nil }
func (c *w15aCache) Del(context.Context, ...string) error                     { return nil }
func (c *w15aCache) Ping(context.Context) error                               { return nil }
func (c *w15aCache) SetNX(_ context.Context, key, _ string, _ time.Duration) (bool, error) {
	c.setNXKeys = append(c.setNXKeys, key)
	return true, nil
}

var _ port.CacheStore = (*w15aCache)(nil)

// w15aTaskReads fails the read of one task with err, as a database call that
// ran out of time would.
type w15aTaskReads struct {
	*fakeTaskRepo
	err   error
	reads []w15aTaskRead
}

type w15aTaskRead struct{ tenant, task uuid.UUID }

func (r *w15aTaskReads) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Task, error) {
	r.reads = append(r.reads, w15aTaskRead{tenantID, id})
	if r.err != nil {
		return nil, r.err
	}
	return r.fakeTaskRepo.GetByID(ctx, tenantID, id)
}

func w15aDelegateRoutes(svc *service.WorkflowClient, cache port.CacheStore) *gin.Engine {
	gin.SetMode(gin.TestMode)
	h := handler.New(handler.Services{WorkflowClient: svc, Cache: cache, IdempotencyTTL: time.Hour})
	r := gin.New()
	rg := r.Group("/api/v1/internal/workflows")
	rg.POST("/reassign-delegate", h.Idempotent(h.ReassignDelegate))
	rg.POST("/cancel-by-delegate", h.Idempotent(h.CancelByDelegate))
	rg.GET("/delegate-impact", h.DelegateImpact)
	return r
}

func w15aServe(t *testing.T, r *gin.Engine, method, target string, body map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	var rd *strings.Reader
	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(t, err)
		rd = strings.NewReader(string(b))
	} else {
		rd = strings.NewReader("")
	}
	req := httptest.NewRequest(method, target, rd)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

// w15aHoldDelegated gives the delegate one active, delegation-tagged assignment
// on a new task of a new instance.
func w15aHoldDelegated(instances *fakeInstanceRepo, tasks *fakeTaskRepo, assignments *fakeAssignmentRepo, tenant, delegate uuid.UUID) (*domain.Task, *domain.TaskAssignment) {
	inst := &domain.Instance{ID: uuid.New(), TenantID: tenant, TemporalWorkflowID: tenant.String() + ":" + uuid.NewString()}
	instances.byID[inst.ID] = inst
	task := &domain.Task{ID: uuid.New(), TenantID: tenant, WorkflowInstanceID: inst.ID, RecordVersion: 1}
	tasks.byID[task.ID] = task
	a := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenant, TaskID: task.ID, UserID: delegate, IsActive: true, Reason: "delegation:" + uuid.NewString()}
	assignments.add(a)
	return task, a
}

// w15aListedAssignments records the tenant and user of every active-assignment read.
type w15aListedAssignments struct {
	*fakeAssignmentRepo
	listed []w15aListCall
}

type w15aListCall struct{ tenant, user uuid.UUID }

func (r *w15aListedAssignments) ListActiveByUser(ctx context.Context, tenantID, userID uuid.UUID) ([]*domain.TaskAssignment, error) {
	r.listed = append(r.listed, w15aListCall{tenantID, userID})
	return r.fakeAssignmentRepo.ListActiveByUser(ctx, tenantID, userID)
}

func w15aAssignmentIDs(assignments *fakeAssignmentRepo) []uuid.UUID {
	ids := make([]uuid.UUID, 0, len(assignments.byID))
	for id := range assignments.byID {
		ids = append(ids, id)
	}
	return ids
}

func w15aActiveHolders(assignments *fakeAssignmentRepo, taskID uuid.UUID) []uuid.UUID {
	var holders []uuid.UUID
	for _, a := range assignments.byID {
		if a.TaskID == taskID && a.IsActive {
			holders = append(holders, a.UserID)
		}
	}
	return holders
}

func TestReassignDelegate_RepeatedWithoutKeyAfterFullSuccess_ReassignsNothingMore(t *testing.T) {
	svc, instances, tasks, assignments, outbox := newWorkflowClientHarness()
	tenant, oldDelegate, newDelegate := uuid.New(), uuid.New(), uuid.New()
	task, held := w15aHoldDelegated(instances, tasks, assignments, tenant, oldDelegate)
	listed := &w15aListedAssignments{fakeAssignmentRepo: assignments}
	svc.Assignments = listed
	cache := &w15aCache{}
	router := w15aDelegateRoutes(svc, cache)
	body := map[string]any{
		"tenant_id": tenant.String(), "old_delegate_id": oldDelegate.String(), "new_delegate_id": newDelegate.String(),
	}

	first := w15aServe(t, router, http.MethodPost, "/api/v1/internal/workflows/reassign-delegate", body)
	require.Equal(t, http.StatusOK, first.Code, first.Body.String())
	assert.JSONEq(t, `{"reassigned":1}`, first.Body.String())
	require.False(t, assignments.byID[held.ID].IsActive, "the first call must vacate the old delegate's assignment")
	assert.Equal(t, []w15aListCall{{tenant, oldDelegate}}, listed.listed, "the tenant and old delegate reach the assignment read")
	require.Len(t, outbox.signals, 1)
	assert.Equal(t, task.WorkflowInstanceID, outbox.signals[0].InstanceID)
	assert.Equal(t, port.SignalInstanceReassign, outbox.signals[0].SignalName)
	assert.Equal(t, []uuid.UUID{tenant}, outbox.signalTenants, "the signal is recorded for the caller's tenant")
	assignmentIDs := w15aAssignmentIDs(assignments)
	signals := slices.Clone(outbox.signals)
	enqueued := slices.Clone(outbox.enqueued)

	second := w15aServe(t, router, http.MethodPost, "/api/v1/internal/workflows/reassign-delegate", body)

	require.Equal(t, http.StatusOK, second.Code, second.Body.String())
	assert.JSONEq(t, `{"reassigned":0}`, second.Body.String())
	assert.Equal(t, []uuid.UUID{newDelegate}, w15aActiveHolders(assignments, task.ID), "the new delegate stays the only holder")
	assert.ElementsMatch(t, assignmentIDs, w15aAssignmentIDs(assignments), "the repeat must insert no assignment")
	assert.Equal(t, signals, outbox.signals, "the repeat must record no second signal")
	assert.Equal(t, enqueued, outbox.enqueued, "the repeat must record no second event")
	assert.Equal(t, []uuid.UUID{tenant}, outbox.signalTenants)
	assert.Equal(t, []w15aListCall{{tenant, oldDelegate}, {tenant, oldDelegate}}, listed.listed, "the repeat reads the same tenant and delegate")
	assert.Empty(t, cache.setNXKeys, "without an Idempotency-Key nothing is reserved or replayed")
}

func TestCancelByDelegate_RepeatedWithoutKeyAfterFullSuccess_CancelsNothingMore(t *testing.T) {
	svc, instances, tasks, assignments, outbox := newWorkflowClientHarness()
	tenant, delegate := uuid.New(), uuid.New()
	task, held := w15aHoldDelegated(instances, tasks, assignments, tenant, delegate)
	listed := &w15aListedAssignments{fakeAssignmentRepo: assignments}
	svc.Assignments = listed
	cache := &w15aCache{}
	router := w15aDelegateRoutes(svc, cache)
	body := map[string]any{"tenant_id": tenant.String(), "delegate_user_id": delegate.String()}

	first := w15aServe(t, router, http.MethodPost, "/api/v1/internal/workflows/cancel-by-delegate", body)
	require.Equal(t, http.StatusOK, first.Code, first.Body.String())
	assert.JSONEq(t, `{"cancelled":1}`, first.Body.String())
	require.False(t, assignments.byID[held.ID].IsActive, "the first call must vacate the delegate's assignment")
	assert.Equal(t, []w15aListCall{{tenant, delegate}}, listed.listed, "the tenant and delegate reach the assignment read")
	require.Len(t, outbox.signals, 1, "the task left with no assignee is signalled once")
	assert.Equal(t, task.WorkflowInstanceID, outbox.signals[0].InstanceID)
	assert.Equal(t, port.SignalInstanceTaskVacated, outbox.signals[0].SignalName)
	assert.Equal(t, []uuid.UUID{tenant}, outbox.signalTenants, "the signal is recorded for the caller's tenant")
	assignmentIDs := w15aAssignmentIDs(assignments)
	signals := slices.Clone(outbox.signals)
	enqueued := slices.Clone(outbox.enqueued)

	second := w15aServe(t, router, http.MethodPost, "/api/v1/internal/workflows/cancel-by-delegate", body)

	require.Equal(t, http.StatusOK, second.Code, second.Body.String())
	assert.JSONEq(t, `{"cancelled":0}`, second.Body.String())
	assert.Empty(t, w15aActiveHolders(assignments, task.ID))
	assert.ElementsMatch(t, assignmentIDs, w15aAssignmentIDs(assignments), "the repeat must insert no assignment")
	assert.Equal(t, signals, outbox.signals, "the repeat must record no second signal")
	assert.Equal(t, enqueued, outbox.enqueued, "the repeat must record no second event")
	assert.Equal(t, []uuid.UUID{tenant}, outbox.signalTenants)
	assert.Equal(t, []w15aListCall{{tenant, delegate}, {tenant, delegate}}, listed.listed, "the repeat reads the same tenant and delegate")
	assert.Empty(t, cache.setNXKeys, "without an Idempotency-Key nothing is reserved or replayed")
}

func TestDelegateImpact_ReadTimesOut_Returns503RequestTimedOut(t *testing.T) {
	cases := []struct {
		name        string
		failList    bool
		failTaskGet bool
	}{
		{"assignment read", true, false},
		{"task read", false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, instances, rawTasks, assignments, _ := newWorkflowClientHarness()
			tenant, delegate := uuid.New(), uuid.New()
			heldTask, _ := w15aHoldDelegated(instances, rawTasks, assignments, tenant, delegate)
			tasks := &w15aTaskReads{fakeTaskRepo: rawTasks}
			svc.Tasks = tasks
			if tc.failList {
				assignments.listActiveByUserErr = fmt.Errorf("list active assignments: %w", context.DeadlineExceeded)
			}
			if tc.failTaskGet {
				tasks.err = fmt.Errorf("read task: %w", context.DeadlineExceeded)
			}
			router := w15aDelegateRoutes(svc, nil)

			w := w15aServe(t, router, http.MethodGet,
				"/api/v1/internal/workflows/delegate-impact?tenant_id="+tenant.String()+"&delegate_user_id="+delegate.String(), nil)

			require.Equal(t, http.StatusServiceUnavailable, w.Code, w.Body.String())
			assert.Equal(t, "application/problem+json", w.Header().Get("Content-Type"))
			var p struct {
				Code   string `json:"code"`
				Detail string `json:"detail"`
			}
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &p))
			assert.Equal(t, "UPSTREAM_UNAVAILABLE", p.Code)
			assert.Equal(t, "request timed out", p.Detail)
			if tc.failList {
				assert.Empty(t, tasks.reads, "no task is read once the assignment read failed")
			} else {
				assert.Equal(t, []w15aTaskRead{{tenant, heldTask.ID}}, tasks.reads, "the task read is the one that failed")
			}
		})
	}
}
