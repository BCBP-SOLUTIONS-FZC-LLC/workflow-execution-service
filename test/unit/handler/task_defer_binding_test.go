package handler_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type hreDeferredTask struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

func TestDeferTask_WithoutReason_Returns200WithEmptyReason(t *testing.T) {
	fake, calls := hreTaskFake()
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, hreRawRequest(http.MethodPost, hreTaskPath(testTaskID.String(), "defer"), `{"record_version":4}`))

	require.Equal(t, http.StatusOK, w.Code)
	var body hreDeferredTask
	decodeJSON(t, w.Body, &body)
	assert.Equal(t, "DEFERRED", body.Status)
	assert.Equal(t, testTaskID.String(), body.ID)
	assert.Equal(t, []hreTaskCall{{op: "defer", tenantID: testTenantID, taskID: testTaskID, userID: testUserID, reason: "", recordVersion: 4}}, *calls)
}

func TestDeferTask_ReasonOfExactlyTheLimit_Returns200(t *testing.T) {
	cases := []struct {
		name   string
		reason string
	}{
		{name: "500 ascii characters", reason: strings.Repeat("a", 500)},
		{name: "500 multi-byte characters", reason: strings.Repeat("é", 500)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake, calls := hreTaskFake()
			router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

			w := do(router, req(http.MethodPost, hreTaskPath(testTaskID.String(), "defer"), map[string]any{"reason": tc.reason, "record_version": 4}))

			require.Equal(t, http.StatusOK, w.Code)
			var body hreDeferredTask
			decodeJSON(t, w.Body, &body)
			assert.Equal(t, "DEFERRED", body.Status)
			assert.Equal(t, []hreTaskCall{{op: "defer", tenantID: testTenantID, taskID: testTaskID, userID: testUserID, reason: tc.reason, recordVersion: 4}}, *calls)
		})
	}
}

func TestDeferTask_StringRecordVersion_Returns400(t *testing.T) {
	fake, calls := hreTaskFake()
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, hreRawRequest(http.MethodPost, hreTaskPath(testTaskID.String(), "defer"), `{"reason":"backlog","record_version":"1"}`))

	require.Equal(t, http.StatusBadRequest, w.Code)
	prob := hreDecodeProblem(t, w)
	assert.Equal(t, "BAD_REQUEST", prob.Code)
	assert.Equal(t, "request body has a field of the wrong type", prob.Detail)
	assert.Equal(t, []hreParam{{Name: "record_version", Reason: "has the wrong type"}}, prob.InvalidParams)
	assert.Empty(t, *calls, "a refused request must not reach the service")
	hreAssertNoInternalText(t, w)
}
