package http_test

import (
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

const w27aGatewayRefusal = "missing or invalid authentication headers"

type w27aRoute struct {
	name   string
	method string
	path   string
}

func w27aInternalRoutes() []w27aRoute {
	return []w27aRoute{
		{"reassign delegate", http.MethodPost, "/api/v1/internal/workflows/reassign-delegate"},
		{"cancel by delegate", http.MethodPost, "/api/v1/internal/workflows/cancel-by-delegate"},
		{"delegate impact", http.MethodGet, "/api/v1/internal/workflows/delegate-impact"},
		{"active instances", http.MethodGet, "/api/v1/internal/workflows/" + w27aWorkflow + "/active-instances"},
		{"pause user tasks", http.MethodPost, "/api/v1/internal/users/" + w27aUser + "/pause-tasks"},
		{"delegation events", http.MethodPost, "/api/v1/internal/events/delegation"},
		{"user profile events", http.MethodPost, "/api/v1/internal/events/user-profile"},
		{"tenant events", http.MethodPost, "/api/v1/internal/events/tenant"},
		{"workflow task events", http.MethodPost, "/api/v1/internal/events/workflow-task"},
		{"complete connector task", http.MethodPost, "/api/v1/internal/connector-tasks/" + w27aTask + "/complete"},
		{"fail connector task", http.MethodPost, "/api/v1/internal/connector-tasks/" + w27aTask + "/fail"},
	}
}

func w27aPublicRoutes() []w27aRoute {
	task := "/api/v1/tasks/" + w27aTask
	inst := "/api/v1/instances/" + w27aInstance
	return []w27aRoute{
		{"list tasks", http.MethodGet, "/api/v1/tasks"},
		{"get task", http.MethodGet, task},
		{"claim task", http.MethodPost, task + "/claim"},
		{"complete task", http.MethodPost, task + "/complete"},
		{"defer task", http.MethodPost, task + "/defer"},
		{"reassign task", http.MethodPost, task + "/reassign"},
		{"active by user", http.MethodGet, "/api/v1/workflows/active-by-user"},
		{"override node", http.MethodPost, inst + "/nodes/Review/override"},
		{"complete outside wait", http.MethodPost, inst + "/nodes/Wait_1/complete"},
		{"fire boundary", http.MethodPost, inst + "/nodes/Wait_1/boundaries/Timer_1/fire"},
		{"start instance", http.MethodPost, "/api/v1/instances"},
		{"list instances", http.MethodGet, "/api/v1/instances"},
		{"get instance", http.MethodGet, inst},
		{"list instance events", http.MethodGet, inst + "/events"},
		{"pause instance", http.MethodPost, inst + "/pause"},
		{"resume instance", http.MethodPost, inst + "/resume"},
		{"cancel instance", http.MethodPost, inst + "/cancel"},
		{"terminate instance", http.MethodPost, inst + "/terminate"},
		{"force forward", http.MethodPost, inst + "/force-forward"},
		{"force back", http.MethodPost, inst + "/force-back"},
	}
}

func TestRouter_InternalRoutes_RefuseACallerWithoutTheSystemIdentityBeforeAnyHandlerRuns(t *testing.T) {
	obstest.Init(t)
	callers := []struct {
		name       string
		headers    map[string]string
		wantReason string
	}{
		{name: "no identity headers", headers: nil, wantReason: "missing_user"},
		{name: "system role without a user id", headers: map[string]string{"x-tenant-roles": "iam-system"}, wantReason: "missing_user"},
		{name: "user id without the system role", headers: map[string]string{"x-user-id": w27aUser}, wantReason: "missing_role"},
		{name: "a public caller: tenant and user ids, an admin role, no system role", headers: w27aAdminHeaders(), wantReason: "missing_role"},
	}
	for _, route := range w27aInternalRoutes() {
		for _, caller := range callers {
			t.Run(route.name+"/"+caller.name, func(t *testing.T) {
				rig := w27aNewRig(t, "test")
				reasons := []string{"missing_user", "missing_role"}
				before := map[string]float64{}
				for _, r := range reasons {
					before[r] = obstest.Sum(t, w27aRejectMetric, map[string]string{"reason": r})
				}

				w := rig.send(w27aRequest{method: route.method, path: route.path, headers: caller.headers, body: `{}`})

				require.Equal(t, http.StatusUnauthorized, w.Code, w.Body.String())
				assert.Equal(t, "application/problem+json", w.Header().Get("Content-Type"))
				body := w27aBody(t, w)
				assert.Equal(t, "https://errors.bcbp.io/execution/unauthorized", body["type"])
				assert.Equal(t, "Unauthorized", body["title"])
				assert.EqualValues(t, http.StatusUnauthorized, body["status"])
				assert.Equal(t, "missing or invalid internal caller identity", body["detail"])
				assert.Equal(t, route.path, body["instance"])
				assert.Equal(t, "UNAUTHORIZED", body["code"])
				assert.Empty(t, rig.recorded(), "the handler never ran")
				assert.Zero(t, rig.cache.claims(), "no idempotency claim was made")
				for _, r := range reasons {
					want := before[r]
					if r == caller.wantReason {
						want++
					}
					assert.Equal(t, want, obstest.Sum(t, w27aRejectMetric, map[string]string{"reason": r}), "counter reason="+r)
				}
			})
		}
	}
}

func TestRouter_PublicRoutes_RefuseSystemCallerHeadersWithTheGatewayBodyAndNoRejectionCount(t *testing.T) {
	obstest.Init(t)
	for _, route := range w27aPublicRoutes() {
		t.Run(route.name, func(t *testing.T) {
			rig := w27aNewRig(t, "test")
			before := w27aRejected(t)

			w := rig.send(w27aRequest{method: route.method, path: route.path, headers: w27aSystemHeaders(), body: `{}`})

			require.Equal(t, http.StatusUnauthorized, w.Code, w.Body.String())
			body := w27aBody(t, w)
			assert.Equal(t, w27aGatewayRefusal, body["error"], "the public group answers with its own gateway body, not the internal problem body")
			assert.EqualValues(t, http.StatusUnauthorized, body["status"])
			assert.NotContains(t, body, "code")
			assert.Empty(t, rig.recorded())
			assert.Equal(t, before, w27aRejected(t), "the internal guard did not run on a public route")
		})
	}
}

func TestRouter_RouteGroups_EachGroupRunsOnlyItsOwnIdentityGuard(t *testing.T) {
	obstest.Init(t)
	tenant := uuid.MustParse(w27aTenant)
	delegate, other := uuid.MustParse(w27aDelegate), uuid.MustParse(w27aOther)
	t.Run("a system caller with no tenant id is admitted on internal routes", func(t *testing.T) {
		rig := w27aNewRig(t, "test")
		cases := []struct {
			name     string
			route    w27aRoute
			body     string
			wantBody map[string]any
			wantCall w27aCall
		}{
			{
				name: "reassign delegate", route: w27aInternalRoutes()[0],
				body:     `{"tenant_id":"` + w27aTenant + `","old_delegate_id":"` + w27aDelegate + `","new_delegate_id":"` + w27aOther + `"}`,
				wantBody: map[string]any{"reassigned": float64(4)},
				wantCall: w27aCall{"reassign delegate", tenant, other, delegate},
			},
			{
				name: "cancel by delegate", route: w27aInternalRoutes()[1],
				body:     `{"tenant_id":"` + w27aTenant + `","delegate_user_id":"` + w27aDelegate + `"}`,
				wantBody: map[string]any{"cancelled": float64(3)},
				wantCall: w27aCall{"cancel by delegate", tenant, uuid.Nil, delegate},
			},
		}
		before := w27aRejected(t)
		for _, tc := range cases {
			w := rig.send(w27aRequest{method: tc.route.method, path: tc.route.path, headers: w27aSystemHeaders(), body: tc.body})

			require.Equal(t, http.StatusOK, w.Code, tc.name+": "+w.Body.String())
			assert.Equal(t, tc.wantBody, w27aBody(t, w), tc.name)
			calls := rig.recorded()
			require.NotEmpty(t, calls, tc.name)
			assert.Equal(t, tc.wantCall, calls[len(calls)-1], tc.name)
		}
		assert.Len(t, rig.recorded(), len(cases))
		assert.Equal(t, before, w27aRejected(t), "an admitted system caller is not counted as rejected")
	})

	t.Run("a public caller without the system role is admitted on public routes", func(t *testing.T) {
		rig := w27aNewRig(t, "test")
		before := w27aRejected(t)

		w := rig.send(w27aRequest{method: http.MethodPost, path: "/api/v1/tasks/" + w27aTask + "/claim", headers: w27aPublicHeaders(), body: `{"record_version":2}`})

		require.Equal(t, http.StatusAccepted, w.Code, w.Body.String())
		assert.Equal(t, []w27aCall{{"claim task version=2", tenant, uuid.MustParse(w27aUser), uuid.MustParse(w27aTask)}}, rig.recorded())
		assert.Equal(t, before, w27aRejected(t), "the internal guard did not run on a public route")
	})

	t.Run("the probes need no identity at all", func(t *testing.T) {
		rig := w27aNewRig(t, "test")
		for _, path := range []string{"/healthz", "/readyz"} {
			w := rig.send(w27aRequest{method: http.MethodGet, path: path})

			assert.Equal(t, http.StatusOK, w.Code, path+": "+w.Body.String())
		}
	})
}
