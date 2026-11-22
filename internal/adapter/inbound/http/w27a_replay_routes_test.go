package http_test

import (
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRouter_RepeatedIdempotencyKey_ReplaysTheStoredAnswerOnTheRemainingWrappedRoutes(t *testing.T) {
	tenant, user := uuid.MustParse(w27aTenant), uuid.MustParse(w27aUser)
	inst, task := uuid.MustParse(w27aInstance), uuid.MustParse(w27aTask)
	instPath := "/api/v1/instances/" + w27aInstance
	cases := []struct {
		name       string
		path       string
		headers    map[string]string
		body       string
		wantStatus int
		wantCall   w27aCall
	}{
		{
			name: "claim task", path: "/api/v1/tasks/" + w27aTask + "/claim", headers: w27aPublicHeaders(),
			body: `{"record_version":2}`, wantStatus: http.StatusAccepted,
			wantCall: w27aCall{"claim task version=2", tenant, user, task},
		},
		{
			name: "override node assignee", path: instPath + "/nodes/Review/override", headers: w27aAdminHeaders(),
			body: `{"new_user_id":"` + w27aDelegate + `","record_version":2}`, wantStatus: http.StatusOK,
			wantCall: w27aCall{`override assignee node=Review new_user=` + w27aDelegate + ` version=2 reason=""`, tenant, user, inst},
		},
		{
			name: "pause instance", path: instPath + "/pause", headers: w27aAdminHeaders(),
			body: `{"reason":"audit hold","record_version":2}`, wantStatus: http.StatusAccepted,
			wantCall: w27aCall{`pause instance reason="audit hold" version=2`, tenant, user, inst},
		},
		{
			name: "resume instance", path: instPath + "/resume", headers: w27aAdminHeaders(),
			body: `{"record_version":2}`, wantStatus: http.StatusAccepted,
			wantCall: w27aCall{"resume instance version=2", tenant, user, inst},
		},
		{
			name: "cancel instance", path: instPath + "/cancel", headers: w27aAdminHeaders(),
			body: `{"reason":"duplicate","record_version":2}`, wantStatus: http.StatusAccepted,
			wantCall: w27aCall{`cancel instance reason="duplicate" version=2`, tenant, user, inst},
		},
		{
			name: "terminate instance", path: instPath + "/terminate", headers: w27aAdminHeaders(),
			body: `{"reason":"runaway"}`, wantStatus: http.StatusOK,
			wantCall: w27aCall{`terminate instance reason="runaway"`, tenant, user, inst},
		},
		{
			name: "force back", path: instPath + "/force-back", headers: w27aAdminHeaders(),
			body: `{"target_node_key":"Review","record_version":2}`, wantStatus: http.StatusAccepted,
			wantCall: w27aCall{`force back instance target="Review" version=2`, tenant, user, inst},
		},
		{
			name: "reassign delegate", path: "/api/v1/internal/workflows/reassign-delegate", headers: w27aSystemHeaders(),
			body:       `{"tenant_id":"` + w27aTenant + `","old_delegate_id":"` + w27aDelegate + `","new_delegate_id":"` + w27aOther + `"}`,
			wantStatus: http.StatusOK,
			wantCall:   w27aCall{"reassign delegate", tenant, uuid.MustParse(w27aOther), uuid.MustParse(w27aDelegate)},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w27aNewRig(t, "test")
			keyed := func(key string) w27aRequest {
				headers := map[string]string{"Idempotency-Key": key}
				for k, v := range tc.headers {
					headers[k] = v
				}
				return w27aRequest{method: http.MethodPost, path: tc.path, body: tc.body, headers: headers}
			}

			first := rig.send(keyed("w27a-same"))
			require.Equal(t, tc.wantStatus, first.Code, first.Body.String())
			replay := rig.send(keyed("w27a-same"))

			assert.Equal(t, tc.wantStatus, replay.Code, replay.Body.String())
			assert.Equal(t, first.Body.String(), replay.Body.String(), "the replay answers with the stored body")
			assert.Equal(t, []w27aCall{tc.wantCall}, rig.recorded(), "the handler ran once, with the caller's ids")
			assert.Equal(t, 2, rig.cache.claims(), "both requests went through the key claim")

			fresh := rig.send(keyed("w27a-other"))

			require.Equal(t, tc.wantStatus, fresh.Code, fresh.Body.String())
			assert.Equal(t, []w27aCall{tc.wantCall, tc.wantCall}, rig.recorded(), "a different key runs the handler again")
		})
	}
}
