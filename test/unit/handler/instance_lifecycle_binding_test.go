package handler_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type hhcParam struct {
	Name   string `json:"name"`
	Reason string `json:"reason"`
}

type hhcProblem struct {
	Code          string     `json:"code"`
	Status        int        `json:"status"`
	Detail        string     `json:"detail"`
	InvalidParams []hhcParam `json:"invalid_params"`
}

type hhcLifecycleCall struct {
	op            string
	tenantID      uuid.UUID
	instanceID    uuid.UUID
	actorUserID   uuid.UUID
	reason        string
	targetNodeKey string
	recordVersion int64
}

type hhcLifecycleRecorder struct {
	calls []hhcLifecycleCall
}

// hhcLifecycleFake records every lifecycle call it receives, with the ids the
// handler resolved from the request.
func hhcLifecycleFake() (*fakeInstanceService, *hhcLifecycleRecorder) {
	rec := &hhcLifecycleRecorder{}
	fake := &fakeInstanceService{
		pause: func(_ context.Context, tenantID, instanceID, actorUserID uuid.UUID, reason string, recordVersion int64) error {
			rec.calls = append(rec.calls, hhcLifecycleCall{op: "pause", tenantID: tenantID, instanceID: instanceID, actorUserID: actorUserID, reason: reason, recordVersion: recordVersion})
			return nil
		},
		resume: func(_ context.Context, tenantID, instanceID, actorUserID uuid.UUID, recordVersion int64) error {
			rec.calls = append(rec.calls, hhcLifecycleCall{op: "resume", tenantID: tenantID, instanceID: instanceID, actorUserID: actorUserID, recordVersion: recordVersion})
			return nil
		},
		cancel: func(_ context.Context, tenantID, instanceID, actorUserID uuid.UUID, reason string, recordVersion int64) error {
			rec.calls = append(rec.calls, hhcLifecycleCall{op: "cancel", tenantID: tenantID, instanceID: instanceID, actorUserID: actorUserID, reason: reason, recordVersion: recordVersion})
			return nil
		},
		terminate: func(_ context.Context, tenantID, instanceID, actorUserID uuid.UUID, reason string) error {
			rec.calls = append(rec.calls, hhcLifecycleCall{op: "terminate", tenantID: tenantID, instanceID: instanceID, actorUserID: actorUserID, reason: reason})
			return nil
		},
		forceBack: func(_ context.Context, tenantID, instanceID, actorUserID uuid.UUID, targetNodeKey string, recordVersion int64) error {
			rec.calls = append(rec.calls, hhcLifecycleCall{op: "force-back", tenantID: tenantID, instanceID: instanceID, actorUserID: actorUserID, targetNodeKey: targetNodeKey, recordVersion: recordVersion})
			return nil
		},
	}
	return fake, rec
}

func hhcRawRequest(method, path, rawBody string) *http.Request {
	r := httptest.NewRequest(method, path, strings.NewReader(rawBody))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("x-tenant-id", testTenantID.String())
	r.Header.Set("x-user-id", testUserID.String())
	r.Header.Set("x-tenant-roles", "tenant_admin")
	return r
}

func hhcDecodeProblem(t *testing.T, w *httptest.ResponseRecorder) hhcProblem {
	t.Helper()
	assert.Equal(t, "application/problem+json", strings.Split(w.Header().Get("Content-Type"), ";")[0])
	var prob hhcProblem
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &prob), "body: %s", w.Body.String())
	assert.Equal(t, w.Code, prob.Status)
	return prob
}

// hhcAssertNoInternalText fails when a decoder, validator or runtime message
// reached the response body.
func hhcAssertNoInternalText(t *testing.T, w *httptest.ResponseRecorder) {
	t.Helper()
	body := w.Body.String()
	for _, leak := range []string{"Go struct", "int64", "unexpected end", "EOF", "invalid character", "Key:", "Field validation", "goroutine", "panic", "invalid UUID"} {
		assert.NotContains(t, body, leak)
	}
}

func hhcLifecyclePath(id string, op string) string {
	return "/api/v1/instances/" + id + "/" + op
}

func TestInstanceLifecycle_NonUUIDInstanceID_Returns400(t *testing.T) {
	cases := []struct {
		name string
		op   string
		body string
	}{
		{name: "pause", op: "pause", body: `{"record_version":1}`},
		{name: "resume", op: "resume", body: `{"record_version":1}`},
		{name: "cancel", op: "cancel", body: `{"record_version":1,"reason":"duplicate"}`},
		{name: "force-back", op: "force-back", body: `{"record_version":1}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake, rec := hhcLifecycleFake()
			router := newRouter(newInstanceHandler(fake))

			w := do(router, hhcRawRequest(http.MethodPost, hhcLifecyclePath("not-a-uuid", tc.op), tc.body))

			require.Equal(t, http.StatusBadRequest, w.Code)
			prob := hhcDecodeProblem(t, w)
			assert.Equal(t, "BAD_REQUEST", prob.Code)
			assert.Equal(t, "invalid id path parameter", prob.Detail)
			assert.Empty(t, prob.InvalidParams)
			assert.Empty(t, rec.calls, "a refused request must not reach the service")
			hhcAssertNoInternalText(t, w)
		})
	}
}

func TestInstanceLifecycle_MalformedBody_Returns400(t *testing.T) {
	cases := []struct {
		name       string
		op         string
		body       string
		wantDetail string
		wantParams []hhcParam
	}{
		{name: "resume empty body", op: "resume", body: ``, wantDetail: "request body is not valid JSON"},
		{name: "resume invalid JSON", op: "resume", body: `{"record_version":`, wantDetail: "request body is not valid JSON"},
		{name: "resume string record_version", op: "resume", body: `{"record_version":"1"}`,
			wantDetail: "request body has a field of the wrong type", wantParams: []hhcParam{{Name: "record_version", Reason: "has the wrong type"}}},
		{name: "resume trailing data", op: "resume", body: `{"record_version":1} {"record_version":2}`,
			wantDetail: "request body has data after its JSON value"},
		{name: "cancel empty body", op: "cancel", body: ``, wantDetail: "request body is not valid JSON"},
		{name: "cancel invalid JSON", op: "cancel", body: `not json`, wantDetail: "request body is not valid JSON"},
		{name: "cancel string record_version", op: "cancel", body: `{"reason":"duplicate","record_version":"1"}`,
			wantDetail: "request body has a field of the wrong type", wantParams: []hhcParam{{Name: "record_version", Reason: "has the wrong type"}}},
		{name: "cancel numeric reason", op: "cancel", body: `{"reason":42,"record_version":1}`,
			wantDetail: "request body has a field of the wrong type", wantParams: []hhcParam{{Name: "reason", Reason: "has the wrong type"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake, rec := hhcLifecycleFake()
			router := newRouter(newInstanceHandler(fake))

			w := do(router, hhcRawRequest(http.MethodPost, hhcLifecyclePath(testInstID.String(), tc.op), tc.body))

			require.Equal(t, http.StatusBadRequest, w.Code)
			prob := hhcDecodeProblem(t, w)
			assert.Equal(t, "BAD_REQUEST", prob.Code)
			assert.Equal(t, tc.wantDetail, prob.Detail)
			assert.Equal(t, tc.wantParams, prob.InvalidParams)
			assert.Empty(t, rec.calls, "a refused request must not reach the service")
			hhcAssertNoInternalText(t, w)
		})
	}
}

func TestInstanceLifecycle_MissingRecordVersion_NamesTheField(t *testing.T) {
	cases := []struct {
		name string
		op   string
		body string
	}{
		{name: "resume absent", op: "resume", body: `{}`},
		{name: "resume zero", op: "resume", body: `{"record_version":0}`},
		{name: "force-back absent", op: "force-back", body: `{"target_node_key":"billing/prep"}`},
		{name: "force-back zero", op: "force-back", body: `{"record_version":0}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake, rec := hhcLifecycleFake()
			router := newRouter(newInstanceHandler(fake))

			w := do(router, hhcRawRequest(http.MethodPost, hhcLifecyclePath(testInstID.String(), tc.op), tc.body))

			require.Equal(t, http.StatusBadRequest, w.Code)
			prob := hhcDecodeProblem(t, w)
			assert.Equal(t, "BAD_REQUEST", prob.Code)
			require.Len(t, prob.InvalidParams, 1)
			assert.Equal(t, "record_version", prob.InvalidParams[0].Name)
			if strings.HasSuffix(tc.name, "absent") {
				assert.Equal(t, "is required", prob.InvalidParams[0].Reason)
			}
			assert.Empty(t, rec.calls, "a refused request must not reach the service")
			hhcAssertNoInternalText(t, w)
		})
	}
}

func TestInstanceLifecycle_CancelReasonLimit(t *testing.T) {
	t.Run("500 characters is accepted", func(t *testing.T) {
		fake, rec := hhcLifecycleFake()
		router := newRouter(newInstanceHandler(fake))
		reason := strings.Repeat("r", 500)

		w := do(router, asAdmin(req(http.MethodPost, instancePath(testInstID)+"/cancel", map[string]any{"reason": reason, "record_version": 6})))

		require.Equal(t, http.StatusAccepted, w.Code)
		require.Len(t, rec.calls, 1)
		assert.Equal(t, hhcLifecycleCall{op: "cancel", tenantID: testTenantID, instanceID: testInstID, actorUserID: testUserID, reason: reason, recordVersion: 6}, rec.calls[0])
	})

	t.Run("501 characters is refused", func(t *testing.T) {
		fake, rec := hhcLifecycleFake()
		router := newRouter(newInstanceHandler(fake))

		w := do(router, asAdmin(req(http.MethodPost, instancePath(testInstID)+"/cancel", map[string]any{"reason": strings.Repeat("r", 501), "record_version": 6})))

		require.Equal(t, http.StatusBadRequest, w.Code)
		prob := hhcDecodeProblem(t, w)
		assert.Equal(t, "BAD_REQUEST", prob.Code)
		assert.Equal(t, []hhcParam{{Name: "reason", Reason: "is not valid"}}, prob.InvalidParams)
		assert.Empty(t, rec.calls, "a refused request must not reach the service")
		hhcAssertNoInternalText(t, w)
	})
}

func TestTerminateInstance_ReasonOfExactlyTheLimit_Returns200(t *testing.T) {
	fake, rec := hhcLifecycleFake()
	router := newRouter(newInstanceHandler(fake))
	reason := strings.Repeat("t", 500)

	w := do(router, asAdmin(req(http.MethodPost, instancePath(testInstID)+"/terminate", map[string]any{"reason": reason})))

	require.Equal(t, http.StatusOK, w.Code)
	var body struct {
		Message string `json:"message"`
	}
	decodeJSON(t, w.Body, &body)
	assert.Equal(t, "action completed", body.Message)
	require.Len(t, rec.calls, 1)
	assert.Equal(t, hhcLifecycleCall{op: "terminate", tenantID: testTenantID, instanceID: testInstID, actorUserID: testUserID, reason: reason}, rec.calls[0])
}

func TestForceBackInstance_TargetNodeKeyLimit(t *testing.T) {
	t.Run("255 characters is accepted", func(t *testing.T) {
		fake, rec := hhcLifecycleFake()
		router := newRouter(newInstanceHandler(fake))
		key := strings.Repeat("n", 255)

		w := do(router, asAdmin(req(http.MethodPost, instancePath(testInstID)+"/force-back", map[string]any{"target_node_key": key, "record_version": 2})))

		require.Equal(t, http.StatusAccepted, w.Code)
		require.Len(t, rec.calls, 1)
		assert.Equal(t, hhcLifecycleCall{op: "force-back", tenantID: testTenantID, instanceID: testInstID, actorUserID: testUserID, targetNodeKey: key, recordVersion: 2}, rec.calls[0])
	})

	t.Run("256 characters is refused", func(t *testing.T) {
		fake, rec := hhcLifecycleFake()
		router := newRouter(newInstanceHandler(fake))

		w := do(router, asAdmin(req(http.MethodPost, instancePath(testInstID)+"/force-back", map[string]any{"target_node_key": strings.Repeat("n", 256), "record_version": 2})))

		require.Equal(t, http.StatusBadRequest, w.Code)
		prob := hhcDecodeProblem(t, w)
		assert.Equal(t, "BAD_REQUEST", prob.Code)
		assert.Equal(t, []hhcParam{{Name: "target_node_key", Reason: "is not valid"}}, prob.InvalidParams)
		assert.Empty(t, rec.calls, "a refused request must not reach the service")
		hhcAssertNoInternalText(t, w)
	})
}

func TestForceBackInstance_UnusableCallerIdentity_Returns500(t *testing.T) {
	cases := []struct {
		name       string
		header     string
		bad        string
		bare       bool
		wantDetail string
	}{
		{name: "malformed tenant id", header: "x-tenant-id", bad: "not-a-uuid", wantDetail: "malformed tenant id in request context"},
		{name: "malformed user id", header: "x-user-id", bad: "not-a-uuid", wantDetail: "malformed user id in request context"},
		{name: "no request context", bare: true, wantDetail: "missing request context"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake, rec := hhcLifecycleFake()
			h := newInstanceHandler(fake)
			router := newRouter(h)
			if tc.bare {
				gin.SetMode(gin.TestMode)
				router = gin.New()
				router.POST("/api/v1/instances/:id/force-back", h.ForceBackInstance)
			}
			r := asAdmin(req(http.MethodPost, instancePath(testInstID)+"/force-back", map[string]any{"record_version": 2}))
			if tc.header != "" {
				r.Header.Set(tc.header, tc.bad)
			}

			w := do(router, r)

			require.Equal(t, http.StatusInternalServerError, w.Code)
			prob := hhcDecodeProblem(t, w)
			assert.Equal(t, "INTERNAL_ERROR", prob.Code)
			assert.Equal(t, tc.wantDetail, prob.Detail)
			assert.Empty(t, rec.calls, "an unusable identity must not reach the service")
			assert.NotContains(t, w.Body.String(), "not-a-uuid")
			hhcAssertNoInternalText(t, w)
		})
	}
}
