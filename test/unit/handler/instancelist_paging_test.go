package handler_test

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func hhbListInstancesRecording(gotPage *port.Page, called *bool) *fakeInstanceService {
	return &fakeInstanceService{
		list: func(_ context.Context, _ uuid.UUID, _ port.ReadScope, _ port.InstanceFilter, page port.Page) (port.PageResult[*port.Instance], error) {
			*called = true
			*gotPage = page
			return port.PageResult[*port.Instance]{}, nil
		},
	}
}

func TestListInstances_LimitAtAndAboveTheMaximum_IsClampedToOneHundred(t *testing.T) {
	for _, limit := range []string{"100", "101", "100000"} {
		t.Run("limit="+limit, func(t *testing.T) {
			var gotPage port.Page
			called := false
			router := newRouter(newInstanceHandler(hhbListInstancesRecording(&gotPage, &called)))

			w := do(router, req(http.MethodGet, instancesPath()+"?limit="+limit, nil))

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			require.True(t, called)
			assert.Equal(t, 100, gotPage.Limit)
		})
	}
}

func TestListInstances_LimitBelowTheMaximum_IsPassedThrough(t *testing.T) {
	var gotPage port.Page
	called := false
	router := newRouter(newInstanceHandler(hhbListInstancesRecording(&gotPage, &called)))

	w := do(router, req(http.MethodGet, instancesPath()+"?limit=99", nil))

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.True(t, called)
	assert.Equal(t, 99, gotPage.Limit)
}

func TestListInstances_LimitOne_AsksForOneItemAndReturnsTheNextCursor(t *testing.T) {
	now := time.Now().UTC()
	var gotPage port.Page
	fake := &fakeInstanceService{
		list: func(_ context.Context, tenantID uuid.UUID, _ port.ReadScope, _ port.InstanceFilter, page port.Page) (port.PageResult[*port.Instance], error) {
			gotPage = page
			return port.PageResult[*port.Instance]{
				Items:      []*port.Instance{{ID: testInstID, TenantID: tenantID, Status: port.InstanceStatusRunning, StartedByUserID: testUserID, StartedAt: &now, CreatedAt: now, UpdatedAt: now}},
				NextCursor: "more-after-this",
			}, nil
		},
	}
	router := newRouter(newInstanceHandler(fake))

	w := do(router, req(http.MethodGet, instancesPath()+"?limit=1", nil))

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.Equal(t, 1, gotPage.Limit)
	var body struct {
		Items []struct {
			ID uuid.UUID `json:"id"`
		} `json:"items"`
		NextCursor string `json:"next_cursor"`
	}
	decodeJSON(t, w.Body, &body)
	require.Len(t, body.Items, 1)
	assert.Equal(t, testInstID, body.Items[0].ID)
	assert.Equal(t, "more-after-this", body.NextCursor)
}

func TestListInstances_NoLimit_UsesTheDefaultPageSizeOfTwentyFive(t *testing.T) {
	var gotPage port.Page
	called := false
	router := newRouter(newInstanceHandler(hhbListInstancesRecording(&gotPage, &called)))

	w := do(router, req(http.MethodGet, instancesPath(), nil))

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.True(t, called)
	assert.Equal(t, 25, gotPage.Limit)
	assert.Nil(t, gotPage.Cursor)
}

func TestListInstances_NonPositiveOrNonIntegerLimit_Returns400(t *testing.T) {
	for _, limit := range []string{"0", "-1", "abc", "10.5"} {
		t.Run("limit="+limit, func(t *testing.T) {
			var gotPage port.Page
			called := false
			router := newRouter(newInstanceHandler(hhbListInstancesRecording(&gotPage, &called)))

			w := do(router, req(http.MethodGet, instancesPath()+"?limit="+url.QueryEscape(limit), nil))

			require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			p := hhbDecodeProblem(t, w)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.Equal(t, hhbBadRequestType, p.Type)
			assert.Equal(t, "limit must be a positive integer", p.Detail)
			assert.False(t, called)
		})
	}
}

func TestListInstances_InvalidStartedBefore_Returns400(t *testing.T) {
	for _, value := range []string{"not-a-date", "2026-07-01", "2026-07-01 10:00:00"} {
		t.Run(value, func(t *testing.T) {
			var gotPage port.Page
			called := false
			router := newRouter(newInstanceHandler(hhbListInstancesRecording(&gotPage, &called)))

			w := do(router, req(http.MethodGet, instancesPath()+"?started_before="+url.QueryEscape(value), nil))

			require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			p := hhbDecodeProblem(t, w)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.Equal(t, hhbBadRequestType, p.Type)
			assert.Equal(t, "invalid started_before query parameter", p.Detail)
			assert.False(t, called)
		})
	}
}

func TestListInstances_MalformedDepartmentsHeaderEntries_AreDroppedAndValidOnesScope(t *testing.T) {
	validDept := uuid.New()
	header := strings.Join([]string{
		"no-colon-at-all",
		"not-a-uuid:approver",
		":approver",
		"",
		fmt.Sprintf("%s:approver", validDept),
	}, ",")
	var gotTenant uuid.UUID
	var gotScope port.ReadScope
	fake := &fakeInstanceService{
		list: func(_ context.Context, tenantID uuid.UUID, scope port.ReadScope, _ port.InstanceFilter, _ port.Page) (port.PageResult[*port.Instance], error) {
			gotTenant, gotScope = tenantID, scope
			return port.PageResult[*port.Instance]{}, nil
		},
	}
	router := newRouter(newInstanceHandler(fake))

	r := req(http.MethodGet, instancesPath(), nil)
	r.Header.Set("x-departments", header)
	w := do(router, r)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.Equal(t, testTenantID, gotTenant)
	assert.Equal(t, testUserID, gotScope.CallerUserID)
	assert.False(t, gotScope.IsAdmin)
	assert.Equal(t, []port.DepartmentRole{{DepartmentID: validDept, Role: "approver"}}, gotScope.Departments)
}
