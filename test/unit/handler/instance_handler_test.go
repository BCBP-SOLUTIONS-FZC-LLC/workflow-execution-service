package handler_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	outboundgrpc "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/grpc"
	adapterhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func instancesPath() string { return "/api/v1/instances" }

func instancePath(id uuid.UUID) string { return "/api/v1/instances/" + id.String() }

func instanceEventsPath(id uuid.UUID) string { return instancePath(id) + "/events" }

func asAdmin(r *http.Request) *http.Request {
	r.Header.Set("x-tenant-roles", "tenant_admin")
	return r
}

type problemBody struct {
	Code          string `json:"code"`
	InvalidParams []struct {
		Name   string `json:"name"`
		Reason string `json:"reason"`
	} `json:"invalid_params"`
}

// --- StartInstance ---

func TestStartInstance_Success(t *testing.T) {
	versionID := uuid.New()
	workflowID := uuid.New()
	now := time.Now().UTC().Truncate(time.Second)
	var gotInput port.StartInstanceInput
	fake := &fakeInstanceService{
		start: func(_ context.Context, in port.StartInstanceInput) (*port.Instance, error) {
			gotInput = in
			return &port.Instance{
				ID: testInstID, TenantID: testTenantID, WorkflowID: workflowID, WorkflowVersionID: versionID,
				BusinessKey: "TND-001", TemporalWorkflowID: "wf-1", Status: port.InstanceStatusRunning,
				CurrentNodeKeys: []string{"intake"}, StartedByUserID: testUserID, StartedAt: &now,
				RecordVersion: 1, CreatedAt: now, UpdatedAt: now,
			}, nil
		},
	}
	router := newRouter(newInstanceHandler(fake))

	overrideUser := uuid.New()
	w := do(router, req(http.MethodPost, instancesPath(), map[string]any{
		"business_key": "TND-001", "workflow_version_id": versionID,
		"override_map": map[string]string{"review": overrideUser.String()},
	}))

	require.Equal(t, http.StatusCreated, w.Code)
	assert.Equal(t, testTenantID, gotInput.TenantID)
	assert.Equal(t, testUserID, gotInput.StartedByUserID)
	assert.Equal(t, versionID, gotInput.WorkflowVersionID)
	assert.Equal(t, "TND-001", gotInput.BusinessKey)
	assert.Equal(t, overrideUser, gotInput.OverrideMap["review"])

	var body struct {
		ID            uuid.UUID `json:"id"`
		Status        string    `json:"status"`
		RecordVersion int64     `json:"record_version"`
	}
	decodeJSON(t, w.Body, &body)
	assert.Equal(t, testInstID, body.ID)
	assert.Equal(t, "RUNNING", body.Status)
	assert.Equal(t, int64(1), body.RecordVersion)
}

func TestStartInstance_MissingRequiredFields(t *testing.T) {
	router := newRouter(newInstanceHandler(&fakeInstanceService{}))
	w := do(router, req(http.MethodPost, instancesPath(), map[string]any{}))
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestStartInstance_MalformedOverrideMapValue(t *testing.T) {
	fake := &fakeInstanceService{
		start: func(context.Context, port.StartInstanceInput) (*port.Instance, error) {
			t.Fatal("must not call the service on a binding failure")
			return nil, nil
		},
	}
	router := newRouter(newInstanceHandler(fake))
	w := do(router, req(http.MethodPost, instancesPath(), map[string]any{
		"business_key": "TND-001", "workflow_version_id": uuid.New(),
		"override_map": map[string]string{"review": "not-a-uuid"},
	}))
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// TestStartInstance_ContextJSONNotAnObject is the regression test for the
// unvalidated-ContextJSON finding: json.RawMessage's own bind only requires
// context_json be some valid JSON value — a bare string or number passes —
// not specifically an object. This must be rejected at the HTTP boundary,
// not left to fail later inside a live workflow's first connector stage.
func TestStartInstance_ContextJSONNotAnObject(t *testing.T) {
	fake := &fakeInstanceService{
		start: func(context.Context, port.StartInstanceInput) (*port.Instance, error) {
			t.Fatal("must not call the service when context_json isn't an object")
			return nil, nil
		},
	}
	router := newRouter(newInstanceHandler(fake))

	cases := []struct {
		name        string
		contextJSON string
	}{
		{"bare string", `"just a string"`},
		{"bare number", `42`},
		{"array", `["a","b"]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(`{"business_key":"TND-001","workflow_version_id":"` + uuid.New().String() + `","context_json":` + tc.contextJSON + `}`)
			w := do(router, req(http.MethodPost, instancesPath(), json.RawMessage(body)))
			assert.Equal(t, http.StatusBadRequest, w.Code)
		})
	}
}

func TestStartInstance_ContextJSONObject_Accepted(t *testing.T) {
	var gotInput port.StartInstanceInput
	fake := &fakeInstanceService{
		start: func(_ context.Context, in port.StartInstanceInput) (*port.Instance, error) {
			gotInput = in
			return &port.Instance{ID: testInstID, TenantID: testTenantID, RecordVersion: 1}, nil
		},
	}
	router := newRouter(newInstanceHandler(fake))

	w := do(router, req(http.MethodPost, instancesPath(), map[string]any{
		"business_key": "TND-001", "workflow_version_id": uuid.New(),
		"context_json": map[string]any{"applicant_email": "a@example.com"},
	}))
	require.Equal(t, http.StatusCreated, w.Code)
	assert.Contains(t, string(gotInput.ContextJSON), "applicant_email")
}

func TestStartInstance_ErrorCodes(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		status int
		code   string
	}{
		{"DuplicateBusinessKey", port.ErrDuplicateBusinessKey, http.StatusConflict, "DUPLICATE_BUSINESS_KEY"},
		{"TenantNotActive", port.ErrTenantNotActive, http.StatusConflict, "TENANT_NOT_ACTIVE"},
		{"VersionNotPublished", port.ErrVersionNotPublished, http.StatusConflict, "VERSION_NOT_PUBLISHED"},
		{"VersionInvalid", port.ErrVersionInvalid, http.StatusConflict, "VERSION_INVALID"},
		{"OverrideMapInvalid", port.ErrOverrideMapInvalid, http.StatusUnprocessableEntity, "OVERRIDE_MAP_INVALID"},
		{"AssigneeIneligible", port.ErrAssigneeIneligible, http.StatusUnprocessableEntity, "ASSIGNEE_INELIGIBLE"},
		{"UpstreamUnavailable", adapterhttp.ErrUpstreamUnavailable, http.StatusServiceUnavailable, "UPSTREAM_UNAVAILABLE"},
		{"DefinitionServiceUnavailable", outboundgrpc.ErrUpstreamUnavailable, http.StatusServiceUnavailable, "UPSTREAM_UNAVAILABLE"},
		{"DefinitionServiceRejected", outboundgrpc.ErrUpstreamRejected, http.StatusServiceUnavailable, "UPSTREAM_UNAVAILABLE"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeInstanceService{
				start: func(context.Context, port.StartInstanceInput) (*port.Instance, error) {
					return nil, tc.err
				},
			}
			router := newRouter(newInstanceHandler(fake))
			w := do(router, req(http.MethodPost, instancesPath(), map[string]any{
				"business_key": "TND-001", "workflow_version_id": uuid.New(),
			}))
			require.Equal(t, tc.status, w.Code)
			var body problemBody
			decodeJSON(t, w.Body, &body)
			assert.Equal(t, tc.code, body.Code)
		})
	}
}

func TestStartInstance_AssigneeIneligible_PopulatesInvalidParams(t *testing.T) {
	fake := &fakeInstanceService{
		start: func(context.Context, port.StartInstanceInput) (*port.Instance, error) {
			return nil, &port.AssigneeIneligibleError{Nodes: []string{"finance/review", "legal/review"}}
		},
	}
	router := newRouter(newInstanceHandler(fake))
	w := do(router, req(http.MethodPost, instancesPath(), map[string]any{
		"business_key": "TND-001", "workflow_version_id": uuid.New(),
	}))

	require.Equal(t, http.StatusUnprocessableEntity, w.Code)
	var body problemBody
	decodeJSON(t, w.Body, &body)
	assert.Equal(t, "ASSIGNEE_INELIGIBLE", body.Code)
	require.Len(t, body.InvalidParams, 2)
	assert.Equal(t, "finance/review", body.InvalidParams[0].Name)
	assert.Equal(t, "legal/review", body.InvalidParams[1].Name)
}

func TestStartInstance_IdempotencyReplay_SameBody(t *testing.T) {
	calls := 0
	fake := &fakeInstanceService{
		start: func(context.Context, port.StartInstanceInput) (*port.Instance, error) {
			calls++
			now := time.Now().UTC()
			return &port.Instance{ID: testInstID, Status: port.InstanceStatusRunning, StartedAt: &now, CreatedAt: now, UpdatedAt: now, RecordVersion: 1}, nil
		},
	}
	router := newRouter(newInstanceHandlerWithCache(fake, newFakeCacheStore()))

	body := map[string]any{"business_key": "TND-100", "workflow_version_id": uuid.New()}
	r1 := req(http.MethodPost, instancesPath(), body)
	r1.Header.Set("Idempotency-Key", "key-1")
	w1 := do(router, r1)
	require.Equal(t, http.StatusCreated, w1.Code)

	r2 := req(http.MethodPost, instancesPath(), body)
	r2.Header.Set("Idempotency-Key", "key-1")
	w2 := do(router, r2)
	require.Equal(t, http.StatusCreated, w2.Code)
	assert.Equal(t, w1.Body.String(), w2.Body.String())
	assert.Equal(t, 1, calls, "second request must be served from cache, not re-invoke the service")
}

func TestStartInstance_IdempotencyReplay_DifferentBody(t *testing.T) {
	fake := &fakeInstanceService{
		start: func(context.Context, port.StartInstanceInput) (*port.Instance, error) {
			now := time.Now().UTC()
			return &port.Instance{ID: testInstID, Status: port.InstanceStatusRunning, StartedAt: &now, CreatedAt: now, UpdatedAt: now, RecordVersion: 1}, nil
		},
	}
	router := newRouter(newInstanceHandlerWithCache(fake, newFakeCacheStore()))

	versionID := uuid.New()
	r1 := req(http.MethodPost, instancesPath(), map[string]any{"business_key": "TND-100", "workflow_version_id": versionID})
	r1.Header.Set("Idempotency-Key", "key-2")
	w1 := do(router, r1)
	require.Equal(t, http.StatusCreated, w1.Code)

	r2 := req(http.MethodPost, instancesPath(), map[string]any{"business_key": "TND-200", "workflow_version_id": versionID})
	r2.Header.Set("Idempotency-Key", "key-2")
	w2 := do(router, r2)
	require.Equal(t, http.StatusConflict, w2.Code)
	var body problemBody
	decodeJSON(t, w2.Body, &body)
	assert.Equal(t, "IDEMPOTENCY_KEY_REPLAY", body.Code)
}

func TestStartInstance_IdempotencyKey_DoesNotCrossTenants(t *testing.T) {
	calls := 0
	fake := &fakeInstanceService{
		start: func(context.Context, port.StartInstanceInput) (*port.Instance, error) {
			calls++
			now := time.Now().UTC()
			return &port.Instance{ID: testInstID, Status: port.InstanceStatusRunning, StartedAt: &now, CreatedAt: now, UpdatedAt: now, RecordVersion: 1}, nil
		},
	}
	router := newRouter(newInstanceHandlerWithCache(fake, newFakeCacheStore()))

	// Neither business_key nor workflow_version_id carries tenant_id, so two
	// tenants can legitimately submit an identical body.
	body := map[string]any{"business_key": "TND-100", "workflow_version_id": uuid.New()}

	r1 := req(http.MethodPost, instancesPath(), body)
	r1.Header.Set("Idempotency-Key", "shared-key")
	w1 := do(router, r1)
	require.Equal(t, http.StatusCreated, w1.Code)

	r2 := req(http.MethodPost, instancesPath(), body)
	r2.Header.Set("Idempotency-Key", "shared-key")
	r2.Header.Set("x-tenant-id", uuid.New().String())
	w2 := do(router, r2)
	require.Equal(t, http.StatusCreated, w2.Code)
	assert.Equal(t, 2, calls, "two different tenants reusing the same Idempotency-Key and body must not collide on one cached response")
}

// --- ListInstances ---

func TestListInstances_FiltersAndPaging(t *testing.T) {
	versionID := uuid.New()
	after := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	before := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	now := time.Now().UTC()
	var gotFilter port.InstanceFilter
	var gotScope port.ReadScope
	fake := &fakeInstanceService{
		list: func(_ context.Context, tenantID uuid.UUID, scope port.ReadScope, filter port.InstanceFilter, page port.Page) (port.PageResult[*port.Instance], error) {
			gotFilter, gotScope = filter, scope
			return port.PageResult[*port.Instance]{
				Items:      []*port.Instance{{ID: testInstID, TenantID: tenantID, Status: port.InstanceStatusRunning, StartedByUserID: testUserID, StartedAt: &now, CreatedAt: now, UpdatedAt: now}},
				NextCursor: "next",
			}, nil
		},
	}
	router := newRouter(newInstanceHandler(fake))

	url := instancesPath() + "?status=RUNNING&workflow_version_id=" + versionID.String() +
		"&started_after=" + after.Format(time.RFC3339) + "&started_before=" + before.Format(time.RFC3339)
	w := do(router, asAdmin(req(http.MethodGet, url, nil)))

	require.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, gotFilter.Status)
	assert.Equal(t, port.InstanceStatusRunning, *gotFilter.Status)
	require.NotNil(t, gotFilter.WorkflowVersionID)
	assert.Equal(t, versionID, *gotFilter.WorkflowVersionID)
	require.NotNil(t, gotFilter.StartedAfter)
	assert.True(t, after.Equal(*gotFilter.StartedAfter))
	require.NotNil(t, gotFilter.StartedBefore)
	assert.True(t, before.Equal(*gotFilter.StartedBefore))
	assert.True(t, gotScope.IsAdmin)

	var body struct {
		Items []struct {
			ID uuid.UUID `json:"id"`
		} `json:"items"`
		NextCursor string `json:"next_cursor"`
	}
	decodeJSON(t, w.Body, &body)
	require.Len(t, body.Items, 1)
	assert.Equal(t, testInstID, body.Items[0].ID)
	assert.Equal(t, "next", body.NextCursor)
}

func TestListInstances_InvalidWorkflowVersionID(t *testing.T) {
	router := newRouter(newInstanceHandler(&fakeInstanceService{}))
	w := do(router, req(http.MethodGet, instancesPath()+"?workflow_version_id=not-a-uuid", nil))
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestListInstances_InvalidStartedAfter(t *testing.T) {
	router := newRouter(newInstanceHandler(&fakeInstanceService{}))
	w := do(router, req(http.MethodGet, instancesPath()+"?started_after=not-a-date", nil))
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestListInstances_InvalidCursor(t *testing.T) {
	router := newRouter(newInstanceHandler(&fakeInstanceService{}))
	w := do(router, req(http.MethodGet, instancesPath()+"?cursor=not-a-valid-cursor", nil))
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// --- GetInstance ---

func TestGetInstance_Success(t *testing.T) {
	now := time.Now().UTC()
	deptID := uuid.New()
	fake := &fakeInstanceService{
		get: func(_ context.Context, tenantID, instanceID uuid.UUID, _ port.ReadScope) (*port.Instance, []*port.Task, error) {
			assert.Equal(t, testTenantID, tenantID)
			assert.Equal(t, testInstID, instanceID)
			return &port.Instance{
					ID: testInstID, TenantID: tenantID, Status: port.InstanceStatusRunning,
					StartedByUserID: testUserID, StartedAt: &now, CreatedAt: now, UpdatedAt: now,
					ContextJSON: json.RawMessage(`{"a":1}`),
					OverrideMap: json.RawMessage(`{"review":"` + uuid.New().String() + `"}`),
				}, []*port.Task{{
					ID: testTaskID, WorkflowInstanceID: testInstID, TenantID: tenantID, NodeKey: "review",
					TaskType: "userTask", DepartmentID: deptID, Status: port.TaskStatusReady, RecordVersion: 1,
					AssigneeMode: "single", AssigneeCount: 1, CreatedAt: now,
				}}, nil
		},
	}
	router := newRouter(newInstanceHandler(fake))

	w := do(router, req(http.MethodGet, instancePath(testInstID), nil))

	require.Equal(t, http.StatusOK, w.Code)
	var body struct {
		HasContext  bool            `json:"has_context"`
		OverrideMap json.RawMessage `json:"override_map"`
		Tasks       []struct {
			ID uuid.UUID `json:"id"`
		} `json:"tasks"`
	}
	decodeJSON(t, w.Body, &body)
	assert.True(t, body.HasContext)
	assert.NotEqual(t, "null", string(body.OverrideMap))
	require.Len(t, body.Tasks, 1)
	assert.Equal(t, testTaskID, body.Tasks[0].ID)
}

func TestGetInstance_NoContextNoOverride(t *testing.T) {
	now := time.Now().UTC()
	fake := &fakeInstanceService{
		get: func(context.Context, uuid.UUID, uuid.UUID, port.ReadScope) (*port.Instance, []*port.Task, error) {
			return &port.Instance{ID: testInstID, StartedAt: &now, CreatedAt: now, UpdatedAt: now}, nil, nil
		},
	}
	router := newRouter(newInstanceHandler(fake))
	w := do(router, req(http.MethodGet, instancePath(testInstID), nil))
	require.Equal(t, http.StatusOK, w.Code)
	var body struct {
		HasContext  bool            `json:"has_context"`
		OverrideMap json.RawMessage `json:"override_map"`
	}
	decodeJSON(t, w.Body, &body)
	assert.False(t, body.HasContext)
	assert.Equal(t, "null", string(body.OverrideMap))
}

func TestGetInstance_NotFound(t *testing.T) {
	fake := &fakeInstanceService{
		get: func(context.Context, uuid.UUID, uuid.UUID, port.ReadScope) (*port.Instance, []*port.Task, error) {
			return nil, nil, port.ErrInstanceNotFound
		},
	}
	router := newRouter(newInstanceHandler(fake))
	w := do(router, req(http.MethodGet, instancePath(testInstID), nil))
	require.Equal(t, http.StatusNotFound, w.Code)
	var body problemBody
	decodeJSON(t, w.Body, &body)
	assert.Equal(t, "INSTANCE_NOT_FOUND", body.Code)
}

func TestGetInstance_NotAuthorizedForResource(t *testing.T) {
	fake := &fakeInstanceService{
		get: func(context.Context, uuid.UUID, uuid.UUID, port.ReadScope) (*port.Instance, []*port.Task, error) {
			return nil, nil, port.ErrNotAuthorizedForRead
		},
	}
	router := newRouter(newInstanceHandler(fake))
	w := do(router, req(http.MethodGet, instancePath(testInstID), nil))
	require.Equal(t, http.StatusForbidden, w.Code)
	var body problemBody
	decodeJSON(t, w.Body, &body)
	assert.Equal(t, "NOT_AUTHORIZED_FOR_RESOURCE", body.Code)
}

func TestGetInstance_InvalidID(t *testing.T) {
	router := newRouter(newInstanceHandler(&fakeInstanceService{}))
	w := do(router, req(http.MethodGet, "/api/v1/instances/not-a-uuid", nil))
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// --- Lifecycle signals: shared not-admin/unauthenticated coverage ---

func TestInstances_Unauthenticated(t *testing.T) {
	router := newRouter(newInstanceHandler(&fakeInstanceService{}))
	r := req(http.MethodGet, instancesPath(), nil)
	r.Header.Del("x-tenant-id")
	r.Header.Del("x-user-id")
	w := do(router, r)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

// --- PauseInstance ---

func TestPauseInstance_Success(t *testing.T) {
	var got struct {
		reason        string
		recordVersion int64
	}
	fake := &fakeInstanceService{
		pause: func(_ context.Context, tenantID, instanceID, actorUserID uuid.UUID, reason string, recordVersion int64) error {
			assert.Equal(t, testTenantID, tenantID)
			assert.Equal(t, testInstID, instanceID)
			assert.Equal(t, testUserID, actorUserID)
			got.reason, got.recordVersion = reason, recordVersion
			return nil
		},
	}
	router := newRouter(newInstanceHandler(fake))
	w := do(router, asAdmin(req(http.MethodPost, instancePath(testInstID)+"/pause", map[string]any{"record_version": 3, "reason": "maintenance"})))

	require.Equal(t, http.StatusAccepted, w.Code)
	assert.Equal(t, "maintenance", got.reason)
	assert.Equal(t, int64(3), got.recordVersion)
	var body struct {
		Message string `json:"message"`
	}
	decodeJSON(t, w.Body, &body)
	assert.Equal(t, "signal accepted", body.Message)
}

func TestPauseInstance_NotAdmin_Forbidden(t *testing.T) {
	fake := &fakeInstanceService{
		pause: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string, int64) error {
			t.Fatal("must not call the service when the caller is not admin")
			return nil
		},
	}
	router := newRouter(newInstanceHandler(fake))
	w := do(router, req(http.MethodPost, instancePath(testInstID)+"/pause", map[string]any{"record_version": 3}))
	require.Equal(t, http.StatusForbidden, w.Code)
	var body problemBody
	decodeJSON(t, w.Body, &body)
	assert.Equal(t, "FORBIDDEN", body.Code)
}

func TestPauseInstance_MissingRecordVersion(t *testing.T) {
	router := newRouter(newInstanceHandler(&fakeInstanceService{}))
	w := do(router, asAdmin(req(http.MethodPost, instancePath(testInstID)+"/pause", map[string]any{})))
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestPauseInstance_ReasonTooLong(t *testing.T) {
	fake := &fakeInstanceService{
		pause: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string, int64) error {
			t.Fatal("must not call the service when reason exceeds the 500-char limit")
			return nil
		},
	}
	router := newRouter(newInstanceHandler(fake))
	w := do(router, asAdmin(req(http.MethodPost, instancePath(testInstID)+"/pause", map[string]any{
		"record_version": 1, "reason": strings.Repeat("a", 501),
	})))
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestPauseInstance_ErrorCodes(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		status int
		code   string
	}{
		{"NotFound", port.ErrInstanceNotFound, http.StatusNotFound, "INSTANCE_NOT_FOUND"},
		{"InvalidInstanceState", port.ErrInvalidInstanceState, http.StatusConflict, "INVALID_INSTANCE_STATE"},
		{"RecordVersionConflict", port.ErrRecordVersionConflict, http.StatusConflict, "RECORD_VERSION_CONFLICT"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeInstanceService{pause: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string, int64) error { return tc.err }}
			router := newRouter(newInstanceHandler(fake))
			w := do(router, asAdmin(req(http.MethodPost, instancePath(testInstID)+"/pause", map[string]any{"record_version": 3})))
			require.Equal(t, tc.status, w.Code)
			var body problemBody
			decodeJSON(t, w.Body, &body)
			assert.Equal(t, tc.code, body.Code)
		})
	}
}

// --- ResumeInstance ---

func TestResumeInstance_Success(t *testing.T) {
	fake := &fakeInstanceService{resume: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, int64) error { return nil }}
	router := newRouter(newInstanceHandler(fake))
	w := do(router, asAdmin(req(http.MethodPost, instancePath(testInstID)+"/resume", map[string]any{"record_version": 3})))
	assert.Equal(t, http.StatusAccepted, w.Code)
}

func TestResumeInstance_NotAdmin_Forbidden(t *testing.T) {
	router := newRouter(newInstanceHandler(&fakeInstanceService{}))
	w := do(router, req(http.MethodPost, instancePath(testInstID)+"/resume", map[string]any{"record_version": 3}))
	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestResumeInstance_InvalidInstanceState(t *testing.T) {
	fake := &fakeInstanceService{resume: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, int64) error {
		return port.ErrInvalidInstanceState
	}}
	router := newRouter(newInstanceHandler(fake))
	w := do(router, asAdmin(req(http.MethodPost, instancePath(testInstID)+"/resume", map[string]any{"record_version": 3})))
	require.Equal(t, http.StatusConflict, w.Code)
	var body problemBody
	decodeJSON(t, w.Body, &body)
	assert.Equal(t, "INVALID_INSTANCE_STATE", body.Code)
}

// --- CancelInstance ---

func TestCancelInstance_Success(t *testing.T) {
	var gotReason string
	fake := &fakeInstanceService{
		cancel: func(_ context.Context, tenantID, instanceID, actorUserID uuid.UUID, reason string, recordVersion int64) error {
			gotReason = reason
			return nil
		},
	}
	router := newRouter(newInstanceHandler(fake))
	w := do(router, asAdmin(req(http.MethodPost, instancePath(testInstID)+"/cancel", map[string]any{"reason": "duplicate submission", "record_version": 2})))
	require.Equal(t, http.StatusAccepted, w.Code)
	assert.Equal(t, "duplicate submission", gotReason)
}

func TestCancelInstance_NotAdmin_Forbidden(t *testing.T) {
	router := newRouter(newInstanceHandler(&fakeInstanceService{}))
	w := do(router, req(http.MethodPost, instancePath(testInstID)+"/cancel", map[string]any{"reason": "x", "record_version": 2}))
	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestCancelInstance_MissingReason(t *testing.T) {
	router := newRouter(newInstanceHandler(&fakeInstanceService{}))
	w := do(router, asAdmin(req(http.MethodPost, instancePath(testInstID)+"/cancel", map[string]any{"record_version": 2})))
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestCancelInstance_AlreadyTerminal(t *testing.T) {
	fake := &fakeInstanceService{cancel: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string, int64) error {
		return port.ErrInstanceAlreadyTerminal
	}}
	router := newRouter(newInstanceHandler(fake))
	w := do(router, asAdmin(req(http.MethodPost, instancePath(testInstID)+"/cancel", map[string]any{"reason": "x", "record_version": 2})))
	require.Equal(t, http.StatusConflict, w.Code)
	var body problemBody
	decodeJSON(t, w.Body, &body)
	assert.Equal(t, "INSTANCE_ALREADY_TERMINAL", body.Code)
}

// --- TerminateInstance ---

func TestTerminateInstance_Success(t *testing.T) {
	var gotReason string
	fake := &fakeInstanceService{
		terminate: func(_ context.Context, tenantID, instanceID, actorUserID uuid.UUID, reason string) error {
			gotReason = reason
			return nil
		},
	}
	router := newRouter(newInstanceHandler(fake))
	r := req(http.MethodPost, instancePath(testInstID)+"/terminate", map[string]any{"reason": "fraud detected"})
	r.Header.Set("x-tenant-roles", "tenant_owner")
	w := do(router, r)
	require.Equal(t, http.StatusAccepted, w.Code)
	assert.Equal(t, "fraud detected", gotReason)
}

func TestTerminateInstance_NotAdmin_Forbidden(t *testing.T) {
	router := newRouter(newInstanceHandler(&fakeInstanceService{}))
	w := do(router, req(http.MethodPost, instancePath(testInstID)+"/terminate", map[string]any{"reason": "x"}))
	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestTerminateInstance_MissingReason(t *testing.T) {
	router := newRouter(newInstanceHandler(&fakeInstanceService{}))
	w := do(router, asAdmin(req(http.MethodPost, instancePath(testInstID)+"/terminate", map[string]any{})))
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestTerminateInstance_AlreadyTerminal(t *testing.T) {
	fake := &fakeInstanceService{terminate: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string) error {
		return port.ErrInstanceAlreadyTerminal
	}}
	router := newRouter(newInstanceHandler(fake))
	w := do(router, asAdmin(req(http.MethodPost, instancePath(testInstID)+"/terminate", map[string]any{"reason": "x"})))
	require.Equal(t, http.StatusConflict, w.Code)
	var body problemBody
	decodeJSON(t, w.Body, &body)
	assert.Equal(t, "INSTANCE_ALREADY_TERMINAL", body.Code)
}

// --- ForceForwardInstance ---

func TestForceForwardInstance_Success(t *testing.T) {
	var gotTarget string
	fake := &fakeInstanceService{
		forceForward: func(_ context.Context, tenantID, instanceID, actorUserID uuid.UUID, targetNodeKey string, recordVersion int64) error {
			gotTarget = targetNodeKey
			return nil
		},
	}
	router := newRouter(newInstanceHandler(fake))
	w := do(router, asAdmin(req(http.MethodPost, instancePath(testInstID)+"/force-forward", map[string]any{"target_node_key": "settlement", "record_version": 2})))
	require.Equal(t, http.StatusAccepted, w.Code)
	assert.Equal(t, "settlement", gotTarget)
}

func TestForceForwardInstance_NotAdmin_Forbidden(t *testing.T) {
	router := newRouter(newInstanceHandler(&fakeInstanceService{}))
	w := do(router, req(http.MethodPost, instancePath(testInstID)+"/force-forward", map[string]any{"target_node_key": "settlement", "record_version": 2}))
	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestForceForwardInstance_TargetNodeNotFound(t *testing.T) {
	fake := &fakeInstanceService{forceForward: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string, int64) error {
		return port.ErrTargetNodeNotFound
	}}
	router := newRouter(newInstanceHandler(fake))
	w := do(router, asAdmin(req(http.MethodPost, instancePath(testInstID)+"/force-forward", map[string]any{"target_node_key": "nope", "record_version": 2})))
	require.Equal(t, http.StatusConflict, w.Code)
	var body problemBody
	decodeJSON(t, w.Body, &body)
	assert.Equal(t, "TARGET_NODE_NOT_FOUND", body.Code)
}

func TestForceForwardInstance_InvalidInstanceState(t *testing.T) {
	fake := &fakeInstanceService{forceForward: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string, int64) error {
		return port.ErrInvalidInstanceState
	}}
	router := newRouter(newInstanceHandler(fake))
	w := do(router, asAdmin(req(http.MethodPost, instancePath(testInstID)+"/force-forward", map[string]any{"target_node_key": "settlement", "record_version": 2})))
	require.Equal(t, http.StatusConflict, w.Code)
	var body problemBody
	decodeJSON(t, w.Body, &body)
	assert.Equal(t, "INVALID_INSTANCE_STATE", body.Code)
}

func TestForceForwardInstance_MissingTargetNodeKey(t *testing.T) {
	router := newRouter(newInstanceHandler(&fakeInstanceService{}))
	w := do(router, asAdmin(req(http.MethodPost, instancePath(testInstID)+"/force-forward", map[string]any{"record_version": 2})))
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

// --- ForceBackInstance ---

func TestForceBackInstance_Success(t *testing.T) {
	fake := &fakeInstanceService{forceBack: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, int64) error { return nil }}
	router := newRouter(newInstanceHandler(fake))
	w := do(router, asAdmin(req(http.MethodPost, instancePath(testInstID)+"/force-back", map[string]any{"record_version": 2})))
	assert.Equal(t, http.StatusAccepted, w.Code)
}

func TestForceBackInstance_NotAdmin_Forbidden(t *testing.T) {
	router := newRouter(newInstanceHandler(&fakeInstanceService{}))
	w := do(router, req(http.MethodPost, instancePath(testInstID)+"/force-back", map[string]any{"record_version": 2}))
	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestForceBackInstance_NoSavedBranch(t *testing.T) {
	fake := &fakeInstanceService{forceBack: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, int64) error {
		return port.ErrForceBackNoSavedBranch
	}}
	router := newRouter(newInstanceHandler(fake))
	w := do(router, asAdmin(req(http.MethodPost, instancePath(testInstID)+"/force-back", map[string]any{"record_version": 2})))
	require.Equal(t, http.StatusConflict, w.Code)
	var body problemBody
	decodeJSON(t, w.Body, &body)
	assert.Equal(t, "FORCE_BACK_NO_SAVED_BRANCH", body.Code)
}

// --- ListInstanceEvents ---

func TestListInstanceEvents_Success(t *testing.T) {
	now := time.Now().UTC()
	fake := &fakeInstanceService{
		listEvents: func(_ context.Context, tenantID, instanceID uuid.UUID, _ port.ReadScope, _ port.Page) (port.PageResult[*port.WorkflowEvent], error) {
			return port.PageResult[*port.WorkflowEvent]{
				Items: []*port.WorkflowEvent{{
					ID: uuid.New(), WorkflowInstanceID: instanceID, TenantID: tenantID,
					EventType: port.EventInstanceStarted, OccurredAt: now, PayloadJSON: json.RawMessage(`{}`),
				}},
				NextCursor: "cur",
			}, nil
		},
	}
	router := newRouter(newInstanceHandler(fake))
	w := do(router, req(http.MethodGet, instanceEventsPath(testInstID), nil))

	require.Equal(t, http.StatusOK, w.Code)
	var body struct {
		Items []struct {
			EventType string `json:"event_type"`
		} `json:"items"`
		NextCursor string `json:"next_cursor"`
	}
	decodeJSON(t, w.Body, &body)
	require.Len(t, body.Items, 1)
	assert.Equal(t, "INSTANCE_STARTED", body.Items[0].EventType)
	assert.Equal(t, "cur", body.NextCursor)
}

func TestListInstanceEvents_NotAuthorizedForResource(t *testing.T) {
	fake := &fakeInstanceService{
		listEvents: func(context.Context, uuid.UUID, uuid.UUID, port.ReadScope, port.Page) (port.PageResult[*port.WorkflowEvent], error) {
			return port.PageResult[*port.WorkflowEvent]{}, port.ErrNotAuthorizedForRead
		},
	}
	router := newRouter(newInstanceHandler(fake))
	w := do(router, req(http.MethodGet, instanceEventsPath(testInstID), nil))
	assert.Equal(t, http.StatusForbidden, w.Code)
}

func TestListInstanceEvents_NotFound(t *testing.T) {
	fake := &fakeInstanceService{
		listEvents: func(context.Context, uuid.UUID, uuid.UUID, port.ReadScope, port.Page) (port.PageResult[*port.WorkflowEvent], error) {
			return port.PageResult[*port.WorkflowEvent]{}, port.ErrInstanceNotFound
		},
	}
	router := newRouter(newInstanceHandler(fake))
	w := do(router, req(http.MethodGet, instanceEventsPath(testInstID), nil))
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestListInstanceEvents_InvalidLimit(t *testing.T) {
	router := newRouter(newInstanceHandler(&fakeInstanceService{}))
	w := do(router, req(http.MethodGet, instanceEventsPath(testInstID)+"?limit=0", nil))
	assert.Equal(t, http.StatusBadRequest, w.Code)
}
