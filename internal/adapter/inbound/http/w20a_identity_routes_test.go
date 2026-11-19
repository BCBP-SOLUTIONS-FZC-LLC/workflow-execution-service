package http_test

import (
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

func TestW20aWriteRoutes_IdentityThatIsMissingOrNotAUUID_IsRefusedWith401AndNeverReachesTheService(t *testing.T) {
	tenant, user := uuid.MustParse(w20aTenantID), uuid.MustParse(w20aUserID)
	inst, task := uuid.MustParse(w20aInstance), uuid.MustParse(w20aTask)
	routes := []struct {
		name       string
		path       string
		body       string
		wantStatus int
		wantCall   w20aCall
		// wantDetails is nil for routes whose service call carries nothing beyond the ids.
		wantDetails []w20aDetail
	}{
		{
			name: "fire boundary", path: "/api/v1/instances/" + w20aInstance + "/nodes/Wait_1/boundaries/Timer_1/fire",
			body: `{"variables":{"answer":"yes"}}`, wantStatus: http.StatusAccepted,
			wantCall:    w20aCall{"answer outside wait", tenant, user, inst},
			wantDetails: []w20aDetail{{waitKey: dsl.BoundaryWaitKey("Wait_1", "Timer_1"), variables: map[string]any{"answer": "yes"}}},
		},
		{
			name: "complete outside wait", path: "/api/v1/instances/" + w20aInstance + "/nodes/Wait_1/complete",
			body: `{"variables":{"answer":"yes"}}`, wantStatus: http.StatusAccepted,
			wantCall:    w20aCall{"answer outside wait", tenant, user, inst},
			wantDetails: []w20aDetail{{waitKey: "Wait_1", variables: map[string]any{"answer": "yes"}}},
		},
		{
			name: "complete task", path: "/api/v1/tasks/" + w20aTask + "/complete",
			body: `{"result_json":{"decision":"approve"},"record_version":3}`, wantStatus: http.StatusAccepted,
			wantCall: w20aCall{"complete task", tenant, user, task},
		},
		{
			name: "reassign task", path: "/api/v1/tasks/" + w20aTask + "/reassign",
			body: `{"new_user_id":"` + w20aDelegateA + `","record_version":3}`, wantStatus: http.StatusAccepted,
			wantCall:    w20aCall{"reassign task", tenant, user, task},
			wantDetails: []w20aDetail{{newUserID: uuid.MustParse(w20aDelegateA)}},
		},
	}
	badIdentities := []struct {
		name    string
		headers map[string]string
	}{
		{"no identity headers", map[string]string{"x-tenant-roles": "tenant_admin"}},
		{"tenant id is not a uuid", map[string]string{"x-tenant-id": "not-a-uuid", "x-user-id": w20aUserID, "x-tenant-roles": "tenant_admin"}},
		{"user id is not a uuid", map[string]string{"x-tenant-id": w20aTenantID, "x-user-id": "not-a-uuid", "x-tenant-roles": "tenant_admin"}},
	}
	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			rig := w20aNewRig(t)
			for _, bad := range badIdentities {
				w := rig.send(w20aRequest{method: http.MethodPost, path: route.path, body: route.body, headers: bad.headers})

				require.Equal(t, http.StatusUnauthorized, w.Code, bad.name+": "+w.Body.String())
				body := w20aJSONBody(t, w)
				assert.Equal(t, "missing or invalid authentication headers", body["error"], bad.name)
				assert.EqualValues(t, http.StatusUnauthorized, body["status"], bad.name)
				assert.NotEmpty(t, body["request_id"], bad.name)
				assert.Empty(t, rig.recorded(), bad.name+": a refused identity must not reach the service")
				assert.Empty(t, rig.recordedDetails(), bad.name)
			}

			ok := rig.send(w20aRequest{method: http.MethodPost, path: route.path, body: route.body, headers: w20aAdminHeaders()})

			require.Equal(t, route.wantStatus, ok.Code, ok.Body.String())
			assert.Equal(t, []w20aCall{route.wantCall}, rig.recorded(), "the same request with a valid identity does reach the service")
			assert.Equal(t, route.wantDetails, rig.recordedDetails())
		})
	}
}
