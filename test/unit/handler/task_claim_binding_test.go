package handler_test

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type hhcClaimCall struct {
	tenantID      uuid.UUID
	taskID        uuid.UUID
	userID        uuid.UUID
	recordVersion int64
}

func hhcClaimFake(result error) (*fakeTaskService, *[]hhcClaimCall) {
	var calls []hhcClaimCall
	fake := &fakeTaskService{
		claim: func(_ context.Context, tenantID, taskID, userID uuid.UUID, recordVersion int64) (*port.Task, error) {
			calls = append(calls, hhcClaimCall{tenantID: tenantID, taskID: taskID, userID: userID, recordVersion: recordVersion})
			if result != nil {
				return nil, result
			}
			return newTestTask(), nil
		},
	}
	return fake, &calls
}

func hhcClaimPath(id string) string { return "/api/v1/tasks/" + id + "/claim" }

func TestClaimTask_RefusedBodies_Return400(t *testing.T) {
	cases := []struct {
		name       string
		body       string
		wantDetail string
		wantParams []hhcParam
	}{
		{name: "string record_version", body: `{"record_version":"1"}`,
			wantDetail: "request body has a field of the wrong type",
			wantParams: []hhcParam{{Name: "record_version", Reason: "has the wrong type"}}},
		{name: "fractional record_version", body: `{"record_version":1.5}`,
			wantDetail: "request body has a field of the wrong type",
			wantParams: []hhcParam{{Name: "record_version", Reason: "has the wrong type"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake, calls := hhcClaimFake(nil)
			router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

			w := do(router, hhcRawRequest(http.MethodPost, hhcClaimPath(testTaskID.String()), tc.body))

			require.Equal(t, http.StatusBadRequest, w.Code)
			prob := hhcDecodeProblem(t, w)
			assert.Equal(t, "BAD_REQUEST", prob.Code)
			assert.Equal(t, tc.wantDetail, prob.Detail)
			assert.Equal(t, tc.wantParams, prob.InvalidParams)
			assert.Empty(t, *calls, "a refused request must not reach the service")
			hhcAssertNoInternalText(t, w)
		})
	}
}

func TestClaimTask_Conflict_PassesCallerIdsAnd409(t *testing.T) {
	fake, calls := hhcClaimFake(port.ErrRecordVersionConflict)
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodPost, hhcClaimPath(testTaskID.String()), map[string]any{"record_version": 4}))

	require.Equal(t, http.StatusConflict, w.Code)
	prob := hhcDecodeProblem(t, w)
	assert.Equal(t, "RECORD_VERSION_CONFLICT", prob.Code)
	assert.Equal(t, []hhcClaimCall{{tenantID: testTenantID, taskID: testTaskID, userID: testUserID, recordVersion: 4}}, *calls)
	hhcAssertNoInternalText(t, w)
}

var hhcLetteredTaskID = uuid.MustParse("abcdef01-2345-4678-9abc-def012345678")

func TestGetTask_NonCanonicalIDIsReturnedCanonical(t *testing.T) {
	cases := []struct {
		name string
		id   string
	}{
		{name: "uppercase", id: strings.ToUpper(hhcLetteredTaskID.String())},
		{name: "without hyphens", id: strings.ReplaceAll(hhcLetteredTaskID.String(), "-", "")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.NotEqual(t, hhcLetteredTaskID.String(), tc.id, "the request must differ from the canonical form")
			var gotTenant, gotTask uuid.UUID
			fake := &fakeTaskService{
				get: func(_ context.Context, tenantID, taskID uuid.UUID, _ port.ReadScope) (*port.Task, []*port.TaskAssignment, error) {
					gotTenant, gotTask = tenantID, taskID
					task := newTestTask()
					task.ID = taskID
					return task, nil, nil
				},
			}
			router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

			w := do(router, asAdmin(req(http.MethodGet, "/api/v1/tasks/"+tc.id, nil)))

			require.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, testTenantID, gotTenant)
			assert.Equal(t, hhcLetteredTaskID, gotTask)
			assert.Contains(t, w.Body.String(), `"id":"`+hhcLetteredTaskID.String()+`"`)
			assert.NotContains(t, w.Body.String(), tc.id)
		})
	}
}
