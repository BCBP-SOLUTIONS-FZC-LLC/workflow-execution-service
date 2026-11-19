package handler_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
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

// The active-by-user route runs the real task service behind the real handler; the assignment repository is the
// only fake. It answers the rows of the asked tenant and user only, so a call with the wrong ids finds nothing.

const w26bActivePath = "/api/v1/workflows/active-by-user"

type w26bActiveRead struct {
	tenant, user uuid.UUID
	limit        int
	after        *port.Cursor
}

type w26bActiveRepo struct {
	port.TaskAssignmentRepository
	rows  map[w26bLookup][]port.ActiveUserTaskRow
	err   error
	reads []w26bActiveRead
}

func (r *w26bActiveRepo) ListActiveByUserPaginated(_ context.Context, tenantID, userID uuid.UUID, page port.PageRequest) ([]port.ActiveUserTaskRow, *port.Cursor, error) {
	r.reads = append(r.reads, w26bActiveRead{tenantID, userID, page.Limit, page.After})
	if r.err != nil {
		return nil, nil, r.err
	}
	return r.rows[w26bLookup{tenantID, userID}], nil, nil
}

type w26bActiveWorld struct {
	repo   *w26bActiveRepo
	target uuid.UUID
	rows   []port.ActiveUserTaskRow
}

func w26bNewActiveWorld() *w26bActiveWorld {
	w := &w26bActiveWorld{repo: &w26bActiveRepo{rows: map[w26bLookup][]port.ActiveUserTaskRow{}}, target: uuid.New()}
	now := time.Date(2026, 11, 18, 9, 0, 0, 0, time.UTC)
	w.rows = []port.ActiveUserTaskRow{
		{
			TaskID: uuid.New(), WorkflowInstanceID: uuid.New(), NodeKey: "finance/review", UserID: w.target, DepartmentID: uuid.New(),
			Status: domain.TaskStatusReady, RecordVersion: 3, CreatedAt: now,
		},
		{
			TaskID: uuid.New(), WorkflowInstanceID: uuid.New(), NodeKey: "legal/approve", UserID: w.target, DepartmentID: uuid.New(),
			Status: domain.TaskStatusInProgress, RecordVersion: 7, CreatedAt: now.Add(time.Minute),
		},
	}
	w.repo.rows[w26bLookup{testTenantID, w.target}] = w.rows
	other := w.rows[0]
	other.TaskID, other.UserID = uuid.New(), uuid.New()
	w.repo.rows[w26bLookup{uuid.New(), w.target}] = []port.ActiveUserTaskRow{other}
	return w
}

func (w *w26bActiveWorld) get(roles, query string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodGet, w26bActivePath+"?"+query, nil)
	r.Header.Set("x-tenant-id", testTenantID.String())
	r.Header.Set("x-user-id", testUserID.String())
	if roles != "" {
		r.Header.Set("x-tenant-roles", roles)
	}
	return do(newRouter(handler.New(handler.Services{Tasks: &service.TaskService{Assignments: w.repo}})), r)
}

func TestListActiveByUser_AdminRolesGet200AndOthersGet403(t *testing.T) {
	cases := []struct {
		name       string
		roles      string
		wantStatus int
	}{
		{name: "tenant_owner alone", roles: "tenant_owner", wantStatus: http.StatusOK},
		{name: "tenant_admin", roles: "tenant_admin", wantStatus: http.StatusOK},
		{name: "a role that is neither", roles: "tenant_member", wantStatus: http.StatusForbidden},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			world := w26bNewActiveWorld()

			rec := world.get(tc.roles, "user_id="+world.target.String())

			require.Equal(t, tc.wantStatus, rec.Code, rec.Body.String())
			if tc.wantStatus == http.StatusOK {
				assert.Equal(t, []w26bActiveRead{{testTenantID, world.target, 25, nil}}, world.repo.reads)
				var body struct {
					Items []map[string]any `json:"items"`
				}
				decodeJSON(t, rec.Body, &body)
				assert.Len(t, body.Items, 2)
				return
			}
			assert.Equal(t, "FORBIDDEN", w26bProblemOf(t, rec, http.StatusForbidden).Code)
			assert.Empty(t, world.repo.reads, "a caller without an admin role must not reach the repository")
		})
	}
}

func TestListActiveByUser_LimitParameter_SetsThePageSize(t *testing.T) {
	cases := []struct {
		name      string
		query     string
		wantLimit int
	}{
		{name: "no limit", query: "", wantLimit: 25},
		{name: "limit of 1", query: "&limit=1", wantLimit: 1},
		{name: "limit of 100", query: "&limit=100", wantLimit: 100},
		{name: "limit of 101", query: "&limit=101", wantLimit: 100},
		{name: "limit of 100000", query: "&limit=100000", wantLimit: 100},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			world := w26bNewActiveWorld()

			rec := world.get("tenant_admin", "user_id="+world.target.String()+tc.query)

			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			assert.Equal(t, []w26bActiveRead{{testTenantID, world.target, tc.wantLimit, nil}}, world.repo.reads,
				"the repository is asked for the clamped page size, in the caller's tenant")
		})
	}
}

func TestListActiveByUser_UserWithAssignments_ListsEveryFieldOfEachItem(t *testing.T) {
	world := w26bNewActiveWorld()

	rec := world.get("tenant_admin", "user_id="+world.target.String())

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, []w26bActiveRead{{testTenantID, world.target, 25, nil}}, world.repo.reads)
	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw), rec.Body.String())
	assert.NotContains(t, raw, "next_cursor", "a last page carries no cursor")
	var body struct {
		Items []struct {
			TaskID             uuid.UUID `json:"task_id"`
			WorkflowInstanceID uuid.UUID `json:"workflow_instance_id"`
			NodeKey            string    `json:"node_key"`
			UserID             uuid.UUID `json:"user_id"`
			DepartmentID       uuid.UUID `json:"department_id"`
			Status             string    `json:"status"`
			RecordVersion      int64     `json:"record_version"`
		} `json:"items"`
	}
	require.NoError(t, json.Unmarshal(raw["items"], &body.Items))
	require.Len(t, body.Items, 2)
	for i, want := range world.rows {
		got := body.Items[i]
		assert.Equal(t, want.TaskID, got.TaskID)
		assert.Equal(t, want.WorkflowInstanceID, got.WorkflowInstanceID)
		assert.Equal(t, want.NodeKey, got.NodeKey)
		assert.Equal(t, world.target, got.UserID)
		assert.Equal(t, want.DepartmentID, got.DepartmentID)
		assert.Equal(t, string(want.Status), got.Status)
		assert.Equal(t, want.RecordVersion, got.RecordVersion)
	}
	assert.Equal(t, "READY", body.Items[0].Status)
	assert.Equal(t, "IN_PROGRESS", body.Items[1].Status)
	var itemKeys []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw["items"], &itemKeys))
	assert.Len(t, itemKeys[0], 7, "an item carries exactly the seven documented fields")
}

func TestListActiveByUser_UserWithNoAssignments_Returns200WithAnEmptyList(t *testing.T) {
	cases := []struct {
		name string
		user uuid.UUID
	}{
		{name: "a user with no active assignments", user: uuid.New()},
		{name: "a user id nobody holds", user: uuid.MustParse("99999999-9999-4999-8999-999999999999")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			world := w26bNewActiveWorld()

			rec := world.get("tenant_admin", "user_id="+tc.user.String())

			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			assert.JSONEq(t, `{"items":[]}`, rec.Body.String(), "an empty list, not null, and no cursor")
			assert.Equal(t, []w26bActiveRead{{testTenantID, tc.user, 25, nil}}, world.repo.reads)
		})
	}
}

func TestListActiveByUser_TheReadFails_AnswersWithoutInternalText(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
		wantType   string
		wantDetail string
		leak       string
	}{
		{
			name: "a database error", err: errors.New("w26b: connection to db-host-7 refused"), wantStatus: http.StatusInternalServerError,
			wantCode: "INTERNAL_ERROR", wantType: "internal-error", wantDetail: "an unexpected error occurred", leak: "db-host-7",
		},
		{
			name: "the read ran past its deadline", err: context.DeadlineExceeded, wantStatus: http.StatusServiceUnavailable,
			wantCode: "UPSTREAM_UNAVAILABLE", wantType: "upstream-unavailable", wantDetail: "request timed out", leak: "deadline exceeded",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			world := w26bNewActiveWorld()
			world.repo.err = tc.err

			rec := world.get("tenant_admin", "user_id="+world.target.String())

			p := w26bProblemOf(t, rec, tc.wantStatus)
			assert.Equal(t, tc.wantCode, p.Code)
			assert.Equal(t, w26bProblemBase+tc.wantType, p.Type)
			assert.Equal(t, tc.wantDetail, p.Detail)
			assert.NotContains(t, rec.Body.String(), tc.leak, "the error text of the cause stays out of the response")
			assert.Equal(t, []w26bActiveRead{{testTenantID, world.target, 25, nil}}, world.repo.reads)
		})
	}
}

func TestListActiveByUser_UserIDIsMissingOrNotAUUID_Returns400(t *testing.T) {
	cases := []struct {
		name  string
		query string
	}{
		{name: "missing", query: ""},
		{name: "empty", query: "user_id="},
		{name: "not a uuid", query: "user_id=not-a-uuid"},
		{name: "a uuid with a character cut off", query: "user_id=" + uuid.NewString()[1:]},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			world := w26bNewActiveWorld()

			rec := world.get("tenant_admin", tc.query)

			p := w26bProblemOf(t, rec, http.StatusBadRequest)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.Equal(t, w26bProblemBase+"bad-request", p.Type)
			assert.Equal(t, "invalid user_id query parameter", p.Detail)
			assert.NotContains(t, rec.Body.String(), "invalid UUID", "the parser text stays out of the response")
			assert.Empty(t, world.repo.reads, "a refused request must not reach the repository")
		})
	}
}

func TestListActiveByUser_LimitIsNotAPositiveInteger_Returns400(t *testing.T) {
	for _, limit := range []string{"abc", "1.5", "-5", "0", "+", "1e2", "999999999999999999999999"} {
		t.Run("limit "+limit, func(t *testing.T) {
			world := w26bNewActiveWorld()

			rec := world.get("tenant_admin", "user_id="+world.target.String()+"&limit="+url.QueryEscape(limit))

			p := w26bProblemOf(t, rec, http.StatusBadRequest)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.Equal(t, w26bProblemBase+"bad-request", p.Type)
			assert.Equal(t, "limit must be a positive integer", p.Detail)
			assert.Empty(t, world.repo.reads, "a refused request must not reach the repository")
		})
	}
}

func TestListActiveByUser_CursorCannotBeDecoded_Returns400(t *testing.T) {
	b64 := func(s string) string { return base64.URLEncoding.EncodeToString([]byte(s)) }
	cases := []struct {
		name   string
		cursor string
	}{
		{name: "not base64", cursor: "not-a-valid-cursor"},
		{name: "base64 of text that is not JSON", cursor: b64("not json")},
		{name: "JSON with no fields", cursor: b64(`{}`)},
		{name: "JSON with no id", cursor: b64(`{"created_at":"2026-11-18T09:00:00Z"}`)},
		{name: "JSON with no created_at", cursor: b64(`{"id":"` + uuid.NewString() + `"}`)},
		{name: "JSON with an all-zero id", cursor: b64(`{"created_at":"2026-11-18T09:00:00Z","id":"` + uuid.Nil.String() + `"}`)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			world := w26bNewActiveWorld()

			rec := world.get("tenant_admin", "user_id="+world.target.String()+"&cursor="+url.QueryEscape(tc.cursor))

			p := w26bProblemOf(t, rec, http.StatusBadRequest)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.Equal(t, w26bProblemBase+"bad-request", p.Type)
			assert.Equal(t, "invalid cursor", p.Detail)
			assert.Empty(t, world.repo.reads, "a refused request must not reach the repository")
		})
	}

	t.Run("a cursor the service issued is accepted and reaches the repository", func(t *testing.T) {
		world := w26bNewActiveWorld()
		at := time.Date(2026, 11, 18, 9, 0, 0, 0, time.UTC)
		id := uuid.New()
		cursor := port.EncodeCursor(port.CursorPosition{CreatedAt: at, ID: id})

		rec := world.get("tenant_admin", "user_id="+world.target.String()+"&limit="+strconv.Itoa(2)+"&cursor="+url.QueryEscape(cursor))

		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		assert.Equal(t, []w26bActiveRead{{testTenantID, world.target, 2, &port.Cursor{CreatedAt: at, ID: id}}}, world.repo.reads)
	})
}
