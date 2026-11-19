package http_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

const (
	w27aRejectMetric = "workflow_execution_service_internal_auth_rejected_total"
	w27aNotFoundType = "https://errors.bcbp.io/execution/not-found"
	w27aNoRouteText  = "no route matches this method and path"
)

func w27aRejected(t *testing.T) float64 {
	t.Helper()
	return obstest.Sum(t, w27aRejectMetric, map[string]string{"reason": "missing_user"}) +
		obstest.Sum(t, w27aRejectMetric, map[string]string{"reason": "missing_role"})
}

func w27aAssertNotFoundProblem(t *testing.T, w *httptest.ResponseRecorder, body map[string]any, path string) {
	t.Helper()
	assert.Equal(t, "application/problem+json", w.Header().Get("Content-Type"))
	assert.Equal(t, w27aNotFoundType, body["type"])
	assert.Equal(t, "Not Found", body["title"])
	assert.EqualValues(t, http.StatusNotFound, body["status"])
	assert.Equal(t, w27aNoRouteText, body["detail"])
	assert.Equal(t, path, body["instance"])
	assert.Equal(t, "NOT_FOUND", body["code"])
}

func w27aAssertMethodNotAllowedProblem(t *testing.T, w *httptest.ResponseRecorder, body map[string]any, path string) {
	t.Helper()
	assert.Equal(t, "application/problem+json", w.Header().Get("Content-Type"))
	assert.Equal(t, "https://errors.bcbp.io/execution/method-not-allowed", body["type"])
	assert.Equal(t, "Method Not Allowed", body["title"])
	assert.EqualValues(t, http.StatusMethodNotAllowed, body["status"])
	assert.Equal(t, "the method is not allowed for this path", body["detail"])
	assert.Equal(t, path, body["instance"])
	assert.Equal(t, "METHOD_NOT_ALLOWED", body["code"])
}

func TestRouter_UnroutedPath_AnswersTheCompleteNotFoundProblemWhateverTheCaller(t *testing.T) {
	cases := []struct {
		name    string
		headers map[string]string
	}{
		{name: "no identity", headers: nil},
		{name: "public identity", headers: w27aPublicHeaders()},
		{name: "system caller", headers: w27aSystemHeaders()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w27aNewRig(t, "test")
			const path = "/api/v1/does-not-exist"

			w := rig.send(w27aRequest{method: http.MethodGet, path: path, headers: tc.headers})

			require.Equal(t, http.StatusNotFound, w.Code, w.Body.String())
			w27aAssertNotFoundProblem(t, w, w27aBody(t, w), path)
			assert.Empty(t, rig.recorded())
		})
	}
}

func TestRouter_KnownPathWithAnotherMethod_Answers405WithTheAllowedMethods(t *testing.T) {
	obstest.Init(t)
	cases := []struct {
		name      string
		method    string
		path      string
		wantAllow string
	}{
		{name: "get on a post route", method: http.MethodGet, path: "/api/v1/tasks/" + w27aTask + "/claim", wantAllow: "POST"},
		{name: "post on a get route", method: http.MethodPost, path: "/api/v1/tasks", wantAllow: "GET"},
		{name: "delete on a task", method: http.MethodDelete, path: "/api/v1/tasks/" + w27aTask, wantAllow: "GET"},
		{name: "get on an internal post route", method: http.MethodGet, path: "/api/v1/internal/events/tenant", wantAllow: "POST"},
		{name: "post on an internal get route", method: http.MethodPost, path: "/api/v1/internal/workflows/delegate-impact", wantAllow: "GET"},
		{name: "put on an instance action", method: http.MethodPut, path: "/api/v1/instances/" + w27aInstance + "/pause", wantAllow: "POST"},
	}
	identities := []struct {
		name    string
		headers map[string]string
	}{
		{name: "no identity", headers: nil},
		{name: "public identity", headers: w27aAdminHeaders()},
		{name: "system caller", headers: w27aSystemHeaders()},
	}
	for _, tc := range cases {
		for _, who := range identities {
			t.Run(tc.name+"/"+who.name, func(t *testing.T) {
				rig := w27aNewRig(t, "test")
				rejectedBefore := w27aRejected(t)

				w := rig.send(w27aRequest{method: tc.method, path: tc.path, headers: who.headers, body: `{}`})

				require.Equal(t, http.StatusMethodNotAllowed, w.Code, "405, not 404 and not 401: "+w.Body.String())
				w27aAssertMethodNotAllowedProblem(t, w, w27aBody(t, w), tc.path)
				assert.Equal(t, tc.wantAllow, w.Header().Get("Allow"))
				assert.Empty(t, rig.recorded(), "no handler ran")
				assert.Zero(t, rig.cache.claims(), "no idempotency claim")
				assert.Equal(t, rejectedBefore, w27aRejected(t), "no identity check ran, so nothing is counted as an internal auth rejection")
			})
		}
	}
}

func TestRouter_StaticAndParameterSegmentsAtTheSameDepth_ReachTheirOwnHandlers(t *testing.T) {
	cases := []struct {
		name       string
		path       string
		headers    map[string]string
		wantCall   w27aCall
		wantFields map[string]any
	}{
		{
			name:       "static delegate-impact beside the {id} segment",
			path:       "/api/v1/internal/workflows/delegate-impact?tenant_id=" + w27aTenant + "&delegate_user_id=" + w27aDelegate,
			headers:    w27aSystemHeaders(),
			wantCall:   w27aCall{"delegate impact", uuid.MustParse(w27aTenant), uuid.Nil, uuid.MustParse(w27aDelegate)},
			wantFields: map[string]any{"active_workflows": float64(2), "active_assignments": float64(5)},
		},
		{
			name:       "{id} segment before active-instances",
			path:       "/api/v1/internal/workflows/" + w27aWorkflow + "/active-instances?tenant_id=" + w27aTenant,
			headers:    w27aSystemHeaders(),
			wantCall:   w27aCall{"check active instances", uuid.MustParse(w27aTenant), uuid.Nil, uuid.MustParse(w27aWorkflow)},
			wantFields: map[string]any{"has_active": true, "count": float64(7)},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w27aNewRig(t, "test")

			w := rig.send(w27aRequest{method: http.MethodGet, path: tc.path, headers: tc.headers})

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			assert.Equal(t, []w27aCall{tc.wantCall}, rig.recorded(), "exactly the one handler this path belongs to ran")
			body := w27aBody(t, w)
			for k, v := range tc.wantFields {
				assert.Equal(t, v, body[k], k)
			}
		})
	}
}

func TestRouter_ActiveByUser_ReachesTheTaskServiceForAnAdminWithTheQueriedUser(t *testing.T) {
	rig := w27aNewRig(t, "test")

	w := rig.send(w27aRequest{method: http.MethodGet, path: "/api/v1/workflows/active-by-user?user_id=" + w27aDelegate, headers: w27aAdminHeaders()})

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.Equal(t, []w27aCall{{"active by user", uuid.MustParse(w27aTenant), uuid.Nil, uuid.MustParse(w27aDelegate)}}, rig.recorded())
	assert.Equal(t, map[string]any{"items": []any{}}, w27aBody(t, w))
}

func TestRouter_CompleteOutsideWait_EncodedSlashInNodeIsOneKeyAndUnencodedSlashIsNotFound(t *testing.T) {
	cases := []struct {
		name     string
		node     string
		wantCode int
		wantCall []w27aCall
	}{
		{
			name: "percent-encoded slash", node: "finance%2FReceive_Bid", wantCode: http.StatusAccepted,
			wantCall: []w27aCall{{"answer outside wait finance/Receive_Bid", uuid.MustParse(w27aTenant), uuid.MustParse(w27aUser), uuid.MustParse(w27aInstance)}},
		},
		{name: "unencoded slash", node: "finance/Receive_Bid", wantCode: http.StatusNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w27aNewRig(t, "test")
			path := "/api/v1/instances/" + w27aInstance + "/nodes/" + tc.node + "/complete"

			w := rig.send(w27aRequest{method: http.MethodPost, path: path, headers: w27aPublicHeaders(), body: `{"variables":{"ok":true}}`})

			require.Equal(t, tc.wantCode, w.Code, w.Body.String())
			assert.Equal(t, tc.wantCall, rig.recorded())
			if tc.wantCode == http.StatusNotFound {
				body := w27aBody(t, w)
				assert.Equal(t, "NOT_FOUND", body["code"])
				assert.Equal(t, w27aNoRouteText, body["detail"])
				assert.Equal(t, w27aNotFoundType, body["type"])
			}
		})
	}
}
