package http_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRouter_MalformedIdentityHeaders_AnswerTheSame401AsAMissingIdentity(t *testing.T) {
	rig := hrfNewRig(t, "test")
	routes := []struct{ method, path string }{
		{http.MethodGet, "/api/v1/instances/" + hrfUserID},
		{http.MethodGet, "/api/v1/tasks"},
		{http.MethodPost, "/api/v1/instances/" + hrfUserID + "/pause"},
		{http.MethodGet, "/api/v1/workflows/active-by-user"},
	}
	cases := []struct {
		name    string
		headers map[string]string
	}{
		{"tenant id is not a uuid", map[string]string{"x-tenant-id": "not-a-uuid", "x-user-id": hrfUserID}},
		{"user id is not a uuid", map[string]string{"x-tenant-id": hrfTenantID, "x-user-id": "not-a-uuid"}},
	}
	for _, rt := range routes {
		missing := rig.do(rt.method, rt.path, nil)
		require.Equal(t, http.StatusUnauthorized, missing.Code, missing.Body.String())
		want := hrfBody(t, missing)
		for _, tc := range cases {
			t.Run(rt.method+" "+rt.path+" "+tc.name, func(t *testing.T) {
				w := rig.do(rt.method, rt.path, tc.headers)

				require.Equal(t, http.StatusUnauthorized, w.Code, w.Body.String())
				got := hrfBody(t, w)
				assert.Equal(t, want["error"], got["error"])
				assert.Equal(t, want["status"], got["status"])
				assert.Equal(t, missing.Header().Get("Content-Type"), w.Header().Get("Content-Type"))
				assert.NotEmpty(t, got["request_id"])
				assert.NotEmpty(t, got["trace_id"])
			})
		}
	}
}

func TestRouter_MalformedIdentityHeaders_AreLoggedAsARejectedIdentity(t *testing.T) {
	rig := hrfNewRig(t, "test")
	path := "/api/v1/instances/" + hrfUserID

	w := rig.do(http.MethodGet, path, map[string]string{"x-tenant-id": "not-a-uuid", "x-user-id": hrfUserID})

	require.Equal(t, http.StatusUnauthorized, w.Code)
	var rejected []hrfLogEntry
	for _, e := range rig.logger.entries {
		if e.msg == "auth: request rejected" {
			rejected = append(rejected, e)
		}
	}
	require.Len(t, rejected, 1)
	assert.Equal(t, "warn", rejected[0].level)
	assert.Equal(t, http.StatusUnauthorized, rejected[0].fields["status"])
	assert.Equal(t, http.MethodGet, rejected[0].fields["method"])
	assert.Equal(t, "/api/v1/instances/:id", rejected[0].fields["route"])
	assert.Equal(t, "not-a-uuid", rejected[0].fields["claimed_tenant_id"])
	assert.Equal(t, hrfUserID, rejected[0].fields["claimed_user_id"])
	assert.NotEmpty(t, rejected[0].fields["request_id"])

	access := hrfAccessLogFor(t, rig.logger, "/api/v1/instances/:id")
	assert.Equal(t, "not-a-uuid", access.fields["claimed_tenant_id"], "the access log keeps the identity as claimed")
	assert.NotContains(t, access.fields, "tenant_id", "an identity that was refused is not reported as verified")
	assert.NotContains(t, access.fields, "user_id")
}
