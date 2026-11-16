package http_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRouter_IdempotencyKey_ClaimedOnlyOnTheRoutesTheRouterWraps(t *testing.T) {
	const badID = "not-a-uuid"
	cases := []struct {
		name     string
		method   string
		path     string
		internal bool
		wrapped  bool
	}{
		{name: "claim task", method: http.MethodPost, path: "/api/v1/tasks/" + badID + "/claim", wrapped: true},
		{name: "complete task", method: http.MethodPost, path: "/api/v1/tasks/" + badID + "/complete", wrapped: true},
		{name: "defer task", method: http.MethodPost, path: "/api/v1/tasks/" + badID + "/defer", wrapped: true},
		{name: "reassign task", method: http.MethodPost, path: "/api/v1/tasks/" + badID + "/reassign", wrapped: true},
		{name: "override node", method: http.MethodPost, path: "/api/v1/instances/" + badID + "/nodes/n1/override", wrapped: true},
		{name: "complete outside wait", method: http.MethodPost, path: "/api/v1/instances/" + badID + "/nodes/n1/complete", wrapped: true},
		{name: "fire boundary", method: http.MethodPost, path: "/api/v1/instances/" + badID + "/nodes/n1/boundaries/b1/fire", wrapped: true},
		{name: "start instance", method: http.MethodPost, path: "/api/v1/instances", wrapped: true},
		{name: "pause instance", method: http.MethodPost, path: "/api/v1/instances/" + badID + "/pause", wrapped: true},
		{name: "resume instance", method: http.MethodPost, path: "/api/v1/instances/" + badID + "/resume", wrapped: true},
		{name: "cancel instance", method: http.MethodPost, path: "/api/v1/instances/" + badID + "/cancel", wrapped: true},
		{name: "terminate instance", method: http.MethodPost, path: "/api/v1/instances/" + badID + "/terminate", wrapped: true},
		{name: "force forward", method: http.MethodPost, path: "/api/v1/instances/" + badID + "/force-forward", wrapped: true},
		{name: "force back", method: http.MethodPost, path: "/api/v1/instances/" + badID + "/force-back", wrapped: true},
		{name: "reassign delegate", method: http.MethodPost, path: "/api/v1/internal/workflows/reassign-delegate", internal: true, wrapped: true},
		{name: "cancel by delegate", method: http.MethodPost, path: "/api/v1/internal/workflows/cancel-by-delegate", internal: true, wrapped: true},
		{name: "pause user tasks", method: http.MethodPost, path: "/api/v1/internal/users/" + badID + "/pause-tasks", internal: true},
		{name: "delegation events", method: http.MethodPost, path: "/api/v1/internal/events/delegation", internal: true},
		{name: "user profile events", method: http.MethodPost, path: "/api/v1/internal/events/user-profile", internal: true},
		{name: "tenant events", method: http.MethodPost, path: "/api/v1/internal/events/tenant", internal: true},
		{name: "workflow task events", method: http.MethodPost, path: "/api/v1/internal/events/workflow-task", internal: true},
		{name: "complete connector task", method: http.MethodPost, path: "/api/v1/internal/connector-tasks/" + badID + "/complete", internal: true},
		{name: "fail connector task", method: http.MethodPost, path: "/api/v1/internal/connector-tasks/" + badID + "/fail", internal: true},
	}
	rig := hrfNewRig(t, "test")
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			headers := hrfAuthHeaders()
			if tc.internal {
				headers = hrfSystemHeaders()
			}
			headers["Idempotency-Key"] = "hrf-key-" + tc.name
			before := rig.cache.claimCount()

			w := rig.do(tc.method, tc.path, headers)

			require.NotEqual(t, http.StatusNotFound, w.Code, "the route must exist")
			require.NotEqual(t, http.StatusUnauthorized, w.Code, "the request must pass the route's guard")
			claims := rig.cache.claimsSince(before)
			if !tc.wrapped {
				assert.Empty(t, claims, "this route must not claim an idempotency key")
				return
			}
			require.Len(t, claims, 1)
			assert.Contains(t, claims[0], tc.method+":"+tc.path+":")
			assert.Contains(t, claims[0], "hrf-key-"+tc.name)
		})
	}
}
