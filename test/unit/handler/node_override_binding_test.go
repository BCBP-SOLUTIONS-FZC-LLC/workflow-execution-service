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

type hreOverrideRecorder struct {
	lookups   []string
	overrides []port.AssigneeOverrideInput
	eligible  []hreEligibilityCall
}

type hreEligibilityCall struct {
	tenantID     uuid.UUID
	newUserID    uuid.UUID
	departmentID uuid.UUID
	level        string
}

var hrePreviousUserID = uuid.MustParse("77777777-7777-4777-8777-777777777777")

// hreOverrideFake looks a task up per node key, records every later step and
// answers an override with the reason it was given.
func hreOverrideFake(t *testing.T, lookupErr error) (*fakeTaskService, *fakeEligibilityChecker, *hreOverrideRecorder) {
	rec := &hreOverrideRecorder{}
	fake := &fakeTaskService{
		overrideAssignee: func(_ context.Context, in port.AssigneeOverrideInput) (*port.AssigneeOverride, error) {
			assert.Equal(t, testTenantID, in.TenantID)
			assert.Equal(t, testInstID, in.InstanceID)
			rec.lookups = append(rec.lookups, in.NodeKey)
			if lookupErr != nil {
				return nil, lookupErr
			}
			rec.overrides = append(rec.overrides, in)
			return &port.AssigneeOverride{
				WorkflowInstanceID: in.InstanceID, NodeKey: in.NodeKey, PreviousUserID: hrePreviousUserID,
				NewUserID: in.NewUserID, Reason: in.Reason, ActorUserID: in.ActorUserID, RecordVersion: in.RecordVersion + 1,
			}, nil
		},
	}
	elig := &fakeEligibilityChecker{check: func(_ context.Context, tenantID, newUserID, departmentID uuid.UUID, level string) (bool, error) {
		rec.eligible = append(rec.eligible, hreEligibilityCall{tenantID: tenantID, newUserID: newUserID, departmentID: departmentID, level: level})
		return true, nil
	}}
	return fake, elig, rec
}

func TestOverrideNodeAssignee_ReasonOfExactlyTheLimit_IsPassedAndEchoedInFull(t *testing.T) {
	cases := []struct {
		name   string
		reason string
	}{
		{name: "500 ascii characters", reason: strings.Repeat("r", 500)},
		{name: "500 multi-byte characters", reason: strings.Repeat("é", 500)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake, elig, rec := hreOverrideFake(t, nil)
			router := newRouter(newHandler(fake, elig))
			newUser := uuid.New()

			w := do(router, asAdmin(req(http.MethodPost, overridePath(testInstID, "review_finance"), map[string]any{
				"new_user_id": newUser, "reason": tc.reason, "record_version": 4,
			})))

			require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
			require.Len(t, rec.overrides, 1)
			assert.Equal(t, port.AssigneeOverrideInput{
				TenantID: testTenantID, InstanceID: testInstID, NodeKey: "review_finance",
				NewUserID: newUser, Reason: tc.reason, ActorUserID: testUserID, RecordVersion: 4,
			}, rec.overrides[0])
			assert.Empty(t, rec.eligible, "the service asks eligibility, once, before it stores anything")
			var body struct {
				Reason string `json:"reason"`
			}
			decodeJSON(t, w.Body, &body)
			assert.Equal(t, tc.reason, body.Reason)
		})
	}
}

func TestOverrideNodeAssignee_NodeSegmentWithSQLMetacharacters_Returns404(t *testing.T) {
	const hostileKey = `' OR 1=1 --`
	fake, elig, rec := hreOverrideFake(t, port.ErrTaskNotFound)
	router := newRouter(newHandler(fake, elig))

	w := do(router, asAdmin(req(http.MethodPost, overridePath(testInstID, "%27%20OR%201%3D1%20--"), map[string]any{
		"new_user_id": uuid.New(), "record_version": 4,
	})))

	require.Equal(t, http.StatusNotFound, w.Code)
	prob := hreDecodeProblem(t, w)
	assert.Equal(t, "TASK_NOT_FOUND", prob.Code)
	assert.Equal(t, []string{hostileKey}, rec.lookups, "the lookup receives the segment verbatim, as data")
	assert.Empty(t, rec.eligible)
	assert.Empty(t, rec.overrides, "nothing is changed for an unknown node")
	hreAssertNoInternalText(t, w)
}

func TestOverrideNodeAssignee_NonUUIDInstanceID_Returns400(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{name: "valid body", body: `{"new_user_id":"55555555-5555-5555-5555-555555555555","record_version":4}`},
		{name: "invalid body still reports the id", body: `{bad`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake, elig, rec := hreOverrideFake(t, nil)
			router := newRouter(newHandler(fake, elig))

			w := do(router, hreRawRequest(http.MethodPost, "/api/v1/instances/abc/nodes/review_finance/override", tc.body))

			require.Equal(t, http.StatusBadRequest, w.Code)
			prob := hreDecodeProblem(t, w)
			assert.Equal(t, "BAD_REQUEST", prob.Code)
			assert.Equal(t, "invalid id path parameter", prob.Detail)
			assert.Equal(t, []hreParam{{Name: "id", Reason: "is not valid"}}, prob.InvalidParams)
			assert.Empty(t, rec.lookups, "no lookup for a refused request")
			assert.Empty(t, rec.overrides)
			hreAssertNoInternalText(t, w)
		})
	}
}
