package handler_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// hrdRow is one stored row of one tenant, ordered by (created, id).
type hrdRow struct {
	tenantID uuid.UUID
	id       uuid.UUID
	created  time.Time
}

func (r hrdRow) position() port.CursorPosition {
	return port.CursorPosition{CreatedAt: r.created, ID: r.id}
}

// hrdKeysetStore answers like a store: only the asked tenant's rows, in order,
// strictly after the cursor position, at most limit of them.
type hrdKeysetStore struct {
	rows  []hrdRow
	calls []hrdKeysetCall
}

type hrdKeysetCall struct {
	tenantID   uuid.UUID
	instanceID uuid.UUID
	cursor     *port.CursorPosition
	limit      int
}

// page records the call; instanceID is uuid.Nil for the reads that have none.
func (s *hrdKeysetStore) page(tenantID, instanceID uuid.UUID, page port.Page) ([]hrdRow, string) {
	s.calls = append(s.calls, hrdKeysetCall{tenantID: tenantID, instanceID: instanceID, cursor: page.Cursor, limit: page.Limit})
	var own []hrdRow
	for _, r := range s.rows {
		if r.tenantID != tenantID {
			continue
		}
		if page.Cursor != nil && !hrdAfter(r, *page.Cursor) {
			continue
		}
		own = append(own, r)
	}
	sort.Slice(own, func(i, j int) bool { return hrdBefore(own[i], own[j]) })
	if len(own) <= page.Limit {
		return own, ""
	}
	own = own[:page.Limit]
	return own, port.EncodeCursor(own[len(own)-1].position())
}

func hrdAfter(r hrdRow, pos port.CursorPosition) bool {
	if !r.created.Equal(pos.CreatedAt) {
		return r.created.After(pos.CreatedAt)
	}
	return r.id.String() > pos.ID.String()
}

func hrdBefore(a, b hrdRow) bool {
	if !a.created.Equal(b.created) {
		return a.created.Before(b.created)
	}
	return a.id.String() < b.id.String()
}

type hrdListedItem struct {
	ID       uuid.UUID `json:"id"`
	TenantID uuid.UUID `json:"tenant_id"`
}

type hrdListedPage struct {
	Items      []hrdListedItem `json:"items"`
	NextCursor string          `json:"next_cursor"`
}

func hrdTenantRequest(target string, tenantID uuid.UUID) *http.Request {
	r := httptest.NewRequest(http.MethodGet, target, nil)
	r.Header.Set("x-tenant-id", tenantID.String())
	r.Header.Set("x-user-id", testUserID.String())
	return r
}

func hrdDecodePage(t *testing.T, w *httptest.ResponseRecorder) hrdListedPage {
	t.Helper()
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var page hrdListedPage
	decodeJSON(t, w.Body, &page)
	return page
}

func hrdIDs(page hrdListedPage) []uuid.UUID {
	ids := make([]uuid.UUID, len(page.Items))
	for i, it := range page.Items {
		ids[i] = it.ID
	}
	return ids
}

// TestListRoutes_CursorFromAnotherTenant_ReturnsOnlyTheCallersRowsAfterThePosition
// covers the three cursor-paged reads: the cursor holds a position only, so a
// cursor issued to tenant A is accepted from tenant B and scopes by B's header.
func TestListRoutes_CursorFromAnotherTenant_ReturnsOnlyTheCallersRowsAfterThePosition(t *testing.T) {
	tenantA, tenantB := uuid.New(), uuid.New()
	base := time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)
	instanceID := uuid.New()
	// A's first page ends at minute 1. B has a row before that position and two
	// after it.
	a := []hrdRow{
		{tenantA, uuid.New(), base},
		{tenantA, uuid.New(), base.Add(1 * time.Minute)},
		{tenantA, uuid.New(), base.Add(2 * time.Minute)},
	}
	b := []hrdRow{
		{tenantB, uuid.New(), base.Add(30 * time.Second)},
		{tenantB, uuid.New(), base.Add(3 * time.Minute)},
		{tenantB, uuid.New(), base.Add(4 * time.Minute)},
	}
	rows := append(append([]hrdRow{}, a...), b...)

	store := &hrdKeysetStore{rows: rows}
	tasks := &fakeTaskService{list: func(_ context.Context, tenantID uuid.UUID, _ port.ReadScope, _ port.TaskFilter, page port.Page) (port.PageResult[*port.Task], error) {
		got, next := store.page(tenantID, uuid.Nil, page)
		items := make([]*port.Task, len(got))
		for i, r := range got {
			items[i] = &port.Task{ID: r.id, TenantID: r.tenantID, CreatedAt: r.created}
		}
		return port.PageResult[*port.Task]{Items: items, NextCursor: next}, nil
	}}
	instances := &fakeInstanceService{
		list: func(_ context.Context, tenantID uuid.UUID, _ port.ReadScope, _ port.InstanceFilter, page port.Page) (port.PageResult[*port.Instance], error) {
			got, next := store.page(tenantID, uuid.Nil, page)
			items := make([]*port.Instance, len(got))
			for i, r := range got {
				items[i] = &port.Instance{ID: r.id, TenantID: r.tenantID, CreatedAt: r.created}
			}
			return port.PageResult[*port.Instance]{Items: items, NextCursor: next}, nil
		},
		listEvents: func(_ context.Context, tenantID, eventsInstanceID uuid.UUID, _ port.ReadScope, page port.Page) (port.PageResult[*port.WorkflowEvent], error) {
			got, next := store.page(tenantID, eventsInstanceID, page)
			items := make([]*port.WorkflowEvent, len(got))
			for i, r := range got {
				items[i] = &port.WorkflowEvent{ID: r.id, TenantID: r.tenantID, OccurredAt: r.created}
			}
			return port.PageResult[*port.WorkflowEvent]{Items: items, NextCursor: next}, nil
		},
	}
	router := newRouter(handler.New(handler.Services{Tasks: tasks, Instances: instances, Eligibility: &fakeEligibilityChecker{}}))

	routes := []struct {
		name, path   string
		wantInstance uuid.UUID
	}{
		{"tasks", "/api/v1/tasks", uuid.Nil},
		{"instances", "/api/v1/instances", uuid.Nil},
		{"instance events", instanceEventsPath(instanceID), instanceID},
	}
	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			store.calls = nil

			first := hrdDecodePage(t, do(router, hrdTenantRequest(route.path+"?limit=2", tenantA)))
			require.Equal(t, []uuid.UUID{a[0].id, a[1].id}, hrdIDs(first))
			require.NotEmpty(t, first.NextCursor, "tenant A has a further page")
			position := a[1].position()

			second := hrdDecodePage(t, do(router, hrdTenantRequest(route.path+"?limit=10&cursor="+url.QueryEscape(first.NextCursor), tenantB)))

			assert.Equal(t, []uuid.UUID{b[1].id, b[2].id}, hrdIDs(second), "only tenant B's rows after the position")
			for _, it := range second.Items {
				assert.Equal(t, tenantB, it.TenantID)
			}
			require.Len(t, store.calls, 2)
			assert.Equal(t, tenantB, store.calls[1].tenantID, "the caller's tenant scopes the read, not the cursor")
			assert.Equal(t, route.wantInstance, store.calls[0].instanceID)
			assert.Equal(t, route.wantInstance, store.calls[1].instanceID, "the instance in the path scopes the event read")
			require.NotNil(t, store.calls[1].cursor)
			assert.True(t, position.CreatedAt.Equal(store.calls[1].cursor.CreatedAt))
			assert.Equal(t, position.ID, store.calls[1].cursor.ID)
		})
	}
}
