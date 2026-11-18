package http_test

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

const (
	w20aVersionID  = "55555555-5555-4555-8555-555555555555"
	w20aDelegateA  = "66666666-6666-4666-8666-666666666666"
	w20aDelegateB  = "77777777-7777-4777-8777-777777777777"
	w20aReplayType = "https://errors.bcbp.io/execution/idempotency-key-replay"
)

type w20aIdemRoute struct {
	name       string
	request    w20aRequest
	otherBody  string
	wantStatus int
	wantCall   w20aCall
	// wantDetails is nil for routes whose service call carries nothing beyond the ids.
	wantDetails []w20aDetail
}

func w20aIdemRoutes() []w20aIdemRoute {
	tenant, user := uuid.MustParse(w20aTenantID), uuid.MustParse(w20aUserID)
	inst, task := uuid.MustParse(w20aInstance), uuid.MustParse(w20aTask)
	instPath := "/api/v1/instances/" + w20aInstance
	taskPath := "/api/v1/tasks/" + w20aTask
	return []w20aIdemRoute{
		{
			name: "start instance",
			request: w20aRequest{
				method: http.MethodPost, path: "/api/v1/instances", headers: w20aUserHeaders(),
				body: `{"subject_id":"subj-1","business_key":"bk-1","workflow_version_id":"` + w20aVersionID + `"}`,
			},
			otherBody:  `{"subject_id":"subj-1","business_key":"bk-2","workflow_version_id":"` + w20aVersionID + `"}`,
			wantStatus: http.StatusCreated,
			wantCall:   w20aCall{"start instance", tenant, user, uuid.MustParse(w20aVersionID)},
		},
		{
			name: "complete task",
			request: w20aRequest{
				method: http.MethodPost, path: taskPath + "/complete", headers: w20aUserHeaders(),
				body: `{"result_json":{"decision":"approve"},"record_version":3}`,
			},
			otherBody:  `{"result_json":{"decision":"reject"},"record_version":3}`,
			wantStatus: http.StatusAccepted,
			wantCall:   w20aCall{"complete task", tenant, user, task},
		},
		{
			name: "defer task",
			request: w20aRequest{
				method: http.MethodPost, path: taskPath + "/defer", headers: w20aUserHeaders(),
				body: `{"reason":"waiting for a quote","record_version":3}`,
			},
			otherBody:  `{"reason":"waiting for a signature","record_version":3}`,
			wantStatus: http.StatusOK,
			wantCall:   w20aCall{"defer task", tenant, user, task},
		},
		{
			name: "reassign task",
			request: w20aRequest{
				method: http.MethodPost, path: taskPath + "/reassign", headers: w20aAdminHeaders(),
				body: `{"new_user_id":"` + w20aDelegateA + `","record_version":3}`,
			},
			otherBody:   `{"new_user_id":"` + w20aDelegateB + `","record_version":3}`,
			wantStatus:  http.StatusAccepted,
			wantCall:    w20aCall{"reassign task", tenant, user, task},
			wantDetails: []w20aDetail{{newUserID: uuid.MustParse(w20aDelegateA)}},
		},
		{
			name: "complete outside wait",
			request: w20aRequest{
				method: http.MethodPost, path: instPath + "/nodes/Wait_1/complete", headers: w20aUserHeaders(),
				body: `{"variables":{"answer":"yes"}}`,
			},
			otherBody:   `{"variables":{"answer":"no"}}`,
			wantStatus:  http.StatusAccepted,
			wantCall:    w20aCall{"answer outside wait", tenant, user, inst},
			wantDetails: []w20aDetail{{waitKey: "Wait_1", variables: map[string]any{"answer": "yes"}}},
		},
		{
			name: "fire boundary",
			request: w20aRequest{
				method: http.MethodPost, path: instPath + "/nodes/Wait_1/boundaries/Timer_1/fire", headers: w20aUserHeaders(),
				body: `{"variables":{"answer":"yes"}}`,
			},
			otherBody:   `{"variables":{"answer":"no"}}`,
			wantStatus:  http.StatusAccepted,
			wantCall:    w20aCall{"answer outside wait", tenant, user, inst},
			wantDetails: []w20aDetail{{waitKey: dsl.BoundaryWaitKey("Wait_1", "Timer_1"), variables: map[string]any{"answer": "yes"}}},
		},
		{
			name: "cancel by delegate",
			request: w20aRequest{
				method: http.MethodPost, path: "/api/v1/internal/workflows/cancel-by-delegate", headers: w20aSystemHeaders(),
				body: `{"tenant_id":"` + w20aTenantID + `","delegate_user_id":"` + w20aDelegateA + `"}`,
			},
			otherBody:  `{"tenant_id":"` + w20aTenantID + `","delegate_user_id":"` + w20aDelegateB + `"}`,
			wantStatus: http.StatusOK,
			wantCall:   w20aCall{"cancel by delegate", tenant, uuid.Nil, uuid.MustParse(w20aDelegateA)},
		},
	}
}

func (tc w20aIdemRoute) keyed(key string) w20aRequest {
	req := tc.request
	req.headers = w20aWithKey(tc.request.headers, key)
	return req
}

func TestW20aIdempotentRoutes_SameKeyAndBody_ReplaysTheStoredAnswerWithoutASecondCall(t *testing.T) {
	for _, tc := range w20aIdemRoutes() {
		t.Run(tc.name, func(t *testing.T) {
			rig := w20aNewRig(t)
			req := tc.keyed("w20a-replay")

			first := rig.send(req)
			require.Equal(t, tc.wantStatus, first.Code, first.Body.String())
			replay := rig.send(req)

			assert.Equal(t, tc.wantStatus, replay.Code, replay.Body.String())
			assert.Equal(t, first.Body.String(), replay.Body.String(), "a replay answers with the stored body")
			assert.Equal(t, []w20aCall{tc.wantCall}, rig.recorded(), "the service is called once, with the caller's ids")
			assert.Equal(t, tc.wantDetails, rig.recordedDetails(), "the one call carries the route's wait key, variables or new user")
		})
	}
}

func TestW20aIdempotentRoutes_SameKeyChangedBody_IsRefusedWith409(t *testing.T) {
	for _, tc := range w20aIdemRoutes() {
		t.Run(tc.name, func(t *testing.T) {
			rig := w20aNewRig(t)
			first := rig.send(tc.keyed("w20a-changed"))
			require.Equal(t, tc.wantStatus, first.Code, first.Body.String())
			changed := tc.keyed("w20a-changed")
			changed.body = tc.otherBody

			w := rig.send(changed)

			require.Equal(t, http.StatusConflict, w.Code, w.Body.String())
			body := w20aJSONBody(t, w)
			assert.Equal(t, "IDEMPOTENCY_KEY_REPLAY", body["code"])
			assert.Equal(t, w20aReplayType, body["type"])
			assert.EqualValues(t, http.StatusConflict, body["status"])
			assert.Equal(t, []w20aCall{tc.wantCall}, rig.recorded(), "the changed request must not reach the service")
			assert.Equal(t, tc.wantDetails, rig.recordedDetails(), "only the first request's arguments reached the service")
		})
	}
}

// The first request is held inside the service while the same key arrives again.
func TestW20aIdempotentRoutes_SameKeyWhileTheFirstIsRunning_IsRefusedWith409(t *testing.T) {
	for _, tc := range w20aIdemRoutes() {
		t.Run(tc.name, func(t *testing.T) {
			rig := w20aNewRig(t)
			gate := &w20aGate{entered: make(chan struct{}, 1), release: make(chan struct{})}
			rig.gate = gate
			t.Cleanup(gate.open)
			req := tc.keyed("w20a-inflight")
			firstDone := make(chan string, 1)
			firstStatus := make(chan int, 1)
			go func() {
				w := rig.send(req)
				firstStatus <- w.Code
				firstDone <- w.Body.String()
			}()
			select {
			case <-gate.entered:
			case <-time.After(w20aWaitLimit):
				require.FailNow(t, "the first request never reached the service")
			}

			second := rig.send(req)

			require.Equal(t, http.StatusConflict, second.Code, second.Body.String())
			body := w20aJSONBody(t, second)
			assert.Equal(t, "IDEMPOTENCY_KEY_REPLAY", body["code"])
			assert.Equal(t, w20aReplayType, body["type"])
			assert.Equal(t, []w20aCall{tc.wantCall}, rig.recorded(), "the request racing the first must not reach the service")
			assert.Equal(t, tc.wantDetails, rig.recordedDetails())

			gate.open()
			var firstBody string
			select {
			case status := <-firstStatus:
				assert.Equal(t, tc.wantStatus, status)
				firstBody = <-firstDone
			case <-time.After(w20aWaitLimit):
				require.FailNow(t, "the first request never finished")
			}
			third := rig.send(req)
			assert.Equal(t, tc.wantStatus, third.Code, "once the first finished, the same key replays its answer")
			assert.Equal(t, firstBody, third.Body.String())
			assert.Equal(t, []w20aCall{tc.wantCall}, rig.recorded())
		})
	}
}

// A cache claim error fails open by design: a cache outage must not block the call, so the handler runs
// unprotected and the failure is logged.
func TestW20aCancelByDelegate_ClaimFails_RunsTheHandlerEveryTimeWithoutProtection(t *testing.T) {
	rig := w20aNewRig(t)
	rig.cache.nxErr = errors.New("cache unavailable")
	route := w20aIdemRoutes()[6]
	require.Equal(t, "cancel by delegate", route.name)
	req := route.keyed("w20a-no-claim")

	first := rig.send(req)
	second := rig.send(req)

	assert.Equal(t, http.StatusOK, first.Code, first.Body.String())
	assert.Equal(t, http.StatusOK, second.Code, second.Body.String())
	assert.Equal(t, []w20aCall{route.wantCall, route.wantCall}, rig.recorded(), "both requests ran the handler")
	assert.Equal(t, 2, rig.log.warnsWithPrefix("idempotency:"), "each unprotected run is logged")
	claims, sets, stored := rig.cache.touched()
	assert.Equal(t, 2, claims)
	assert.Zero(t, sets, "an answer produced without a claim is not stored")
	assert.Zero(t, stored)
}

func TestW20aDelegateImpact_IdempotencyKeyIsIgnored_EveryReadReachesTheService(t *testing.T) {
	rig := w20aNewRig(t)
	req := w20aRequest{
		method:  http.MethodGet,
		path:    "/api/v1/internal/workflows/delegate-impact?tenant_id=" + w20aTenantID + "&delegate_user_id=" + w20aDelegateA,
		headers: w20aWithKey(w20aSystemHeaders(), "w20a-read"),
	}
	want := w20aCall{"delegate impact", uuid.MustParse(w20aTenantID), uuid.Nil, uuid.MustParse(w20aDelegateA)}

	first := rig.send(req)
	second := rig.send(req)

	for _, w := range []int{first.Code, second.Code} {
		assert.Equal(t, http.StatusOK, w)
	}
	assert.EqualValues(t, 2, w20aJSONBody(t, first)["active_workflows"])
	assert.EqualValues(t, 2, w20aJSONBody(t, second)["active_workflows"])
	assert.Equal(t, []w20aCall{want, want}, rig.recorded(), "a read is never answered from a stored replay")
	claims, sets, stored := rig.cache.touched()
	assert.Zero(t, claims, "the read route does not claim the key")
	assert.Zero(t, sets)
	assert.Zero(t, stored)
}
