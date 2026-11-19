package handler_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type w26bOverrideCall struct {
	in port.AssigneeOverrideInput
}

// w26bOverrideTasks records every override input and answers with an override that echoes it.
func w26bOverrideTasks() (*fakeTaskService, *[]w26bOverrideCall) {
	var calls []w26bOverrideCall
	fake := &fakeTaskService{
		overrideAssignee: func(_ context.Context, in port.AssigneeOverrideInput) (*port.AssigneeOverride, error) {
			calls = append(calls, w26bOverrideCall{in})
			return &port.AssigneeOverride{
				WorkflowInstanceID: in.InstanceID, NodeKey: in.NodeKey, NewUserID: in.NewUserID, Reason: in.Reason,
				ActorUserID: in.ActorUserID, RecordVersion: in.RecordVersion + 1,
			}, nil
		},
	}
	return fake, &calls
}

func TestOverrideNodeAssignee_ContentTypeIsNotJSON_Returns415(t *testing.T) {
	cases := []struct {
		name        string
		contentType string
	}{
		{name: "text/plain", contentType: "text/plain"},
		{name: "application/xml", contentType: "application/xml"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake, calls := w26bOverrideTasks()
			router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))
			body := `{"new_user_id":"` + uuid.NewString() + `","record_version":4}`

			rec := do(router, w26bAdminPost(overridePath(testInstID, "review_finance"), tc.contentType, body))

			p := w26bProblemOf(t, rec, http.StatusUnsupportedMediaType)
			assert.Equal(t, "UNSUPPORTED_MEDIA_TYPE", p.Code)
			assert.Equal(t, w26bProblemBase+"unsupported-media-type", p.Type)
			assert.Equal(t, "request body must be application/json", p.Detail)
			assert.Empty(t, *calls, "a refused request must not reach the service")
		})
	}
}

func TestOverrideNodeAssignee_ReasonOver500Characters_NamesTheFieldAndTheServiceIsNotCalled(t *testing.T) {
	cases := []struct {
		name       string
		reason     string
		wantStatus int
	}{
		{name: "501 characters", reason: strings.Repeat("a", 501), wantStatus: http.StatusBadRequest},
		{name: "501 multi-byte characters", reason: strings.Repeat("é", 501), wantStatus: http.StatusBadRequest},
		{name: "exactly 500 characters", reason: strings.Repeat("a", 500), wantStatus: http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake, calls := w26bOverrideTasks()
			router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))
			body, err := json.Marshal(map[string]any{"new_user_id": uuid.NewString(), "record_version": 4, "reason": tc.reason})
			require.NoError(t, err)

			rec := do(router, w26bAdminPost(overridePath(testInstID, "review_finance"), "application/json", string(body)))

			if tc.wantStatus == http.StatusOK {
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
				require.Len(t, *calls, 1)
				assert.Equal(t, tc.reason, (*calls)[0].in.Reason, "a reason at the cap reaches the service whole")
				assert.Equal(t, "review_finance", (*calls)[0].in.NodeKey)
				assert.EqualValues(t, 4, (*calls)[0].in.RecordVersion)
				return
			}
			p := w26bProblemOf(t, rec, http.StatusBadRequest)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.Equal(t, w26bProblemBase+"bad-request", p.Type)
			assert.Equal(t, "request body has missing or invalid fields", p.Detail)
			require.Len(t, p.InvalidParams, 1)
			assert.Equal(t, "reason", p.InvalidParams[0].Name)
			assert.Equal(t, "is not valid", p.InvalidParams[0].Reason)
			assert.NotContains(t, rec.Body.String(), tc.reason, "the refused value is not echoed")
			assert.Empty(t, *calls, "a refused request must not reach the service")
		})
	}
}

// An omitted or null reason reaches the service as the empty string, and the answer then carries no reason field;
// a reason that was given is echoed back.
func TestOverrideNodeAssignee_ReasonOmittedOrNull_AnswersWithoutAReasonField(t *testing.T) {
	user := uuid.NewString()
	base := `"new_user_id":"` + user + `","record_version":4`
	cases := []struct {
		name       string
		body       string
		wantReason string
	}{
		{name: "omitted", body: `{` + base + `}`},
		{name: "null", body: `{` + base + `,"reason":null}`},
		{name: "given", body: `{` + base + `,"reason":"covering leave"}`, wantReason: "covering leave"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake, calls := w26bOverrideTasks()
			router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

			rec := do(router, w26bAdminPost(overridePath(testInstID, "review_finance"), "application/json", tc.body))

			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			require.Len(t, *calls, 1)
			assert.Equal(t, tc.wantReason, (*calls)[0].in.Reason)
			assert.Equal(t, testTenantID, (*calls)[0].in.TenantID)
			assert.Equal(t, testInstID, (*calls)[0].in.InstanceID)
			assert.Equal(t, testUserID, (*calls)[0].in.ActorUserID)
			var fields map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &fields), rec.Body.String())
			assert.Equal(t, user, fields["new_user_id"])
			if tc.wantReason == "" {
				assert.NotContains(t, fields, "reason", "an override with no reason answers without the field")
				return
			}
			assert.Equal(t, tc.wantReason, fields["reason"])
		})
	}
}

type w26bAnswerCall struct {
	tenantID, instanceID uuid.UUID
	key                  string
	variables            map[string]any
}

func w26bAnswerFake() (*fakeInstanceService, *[]w26bAnswerCall) {
	var calls []w26bAnswerCall
	fake := &fakeInstanceService{
		answerOutsideWait: func(_ context.Context, tenantID, instanceID uuid.UUID, _ port.ReadScope, key string, variables map[string]any) error {
			calls = append(calls, w26bAnswerCall{tenantID, instanceID, key, variables})
			return nil
		},
	}
	return fake, &calls
}

func TestOutsideAnswer_ExtraTopLevelField_NamesTheFieldAndTheServiceIsNotCalled(t *testing.T) {
	routes := []struct{ name, path string }{
		{name: "complete a wait", path: completePath("Wait_1")},
		{name: "fire a boundary", path: instancePath(testInstID) + "/nodes/Wait_1/boundaries/Timer_1/fire"},
	}
	bodies := []struct {
		name       string
		body       string
		wantParams []string
	}{
		{name: "one unknown field", body: `{"vars":{}}`, wantParams: []string{"vars"}},
		{name: "a known field and an unknown one", body: `{"variables":{"a":1},"vars":{}}`, wantParams: []string{"vars"}},
		{name: "two unknown fields", body: `{"vars":{},"extra":1}`, wantParams: []string{"vars", "extra"}},
	}
	for _, rt := range routes {
		for _, tc := range bodies {
			t.Run(rt.name+"/"+tc.name, func(t *testing.T) {
				fake, calls := w26bAnswerFake()
				router := newRouter(newInstanceHandler(fake))

				rec := do(router, w26bAdminPost(rt.path, "application/json", tc.body))

				p := w26bProblemOf(t, rec, http.StatusBadRequest)
				assert.Equal(t, "BAD_REQUEST", p.Code)
				assert.Equal(t, w26bProblemBase+"bad-request", p.Type)
				assert.Equal(t, "request body has fields this endpoint does not define", p.Detail)
				var names []string
				for _, param := range p.InvalidParams {
					names = append(names, param.Name)
					assert.Equal(t, "is not a known field", param.Reason)
				}
				assert.ElementsMatch(t, tc.wantParams, names)
				assert.Empty(t, *calls, "a refused request must not reach the service")
			})
		}
	}
}

func TestOutsideAnswer_VariablesIsNotAnObject_NamesTheFieldAsTheWrongType(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{name: "string", body: `{"variables":"yes"}`},
		{name: "array", body: `{"variables":["yes"]}`},
		{name: "number", body: `{"variables":3}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake, calls := w26bAnswerFake()
			router := newRouter(newInstanceHandler(fake))

			rec := do(router, w26bAdminPost(completePath("Wait_1"), "application/json", tc.body))

			p := w26bProblemOf(t, rec, http.StatusBadRequest)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.Equal(t, w26bProblemBase+"bad-request", p.Type)
			assert.Equal(t, "request body has a field of the wrong type", p.Detail)
			require.Len(t, p.InvalidParams, 1)
			assert.Equal(t, "variables", p.InvalidParams[0].Name)
			assert.Equal(t, "has the wrong type", p.InvalidParams[0].Reason)
			assert.Empty(t, *calls, "a refused request must not reach the service")
		})
	}

	t.Run("an object is accepted", func(t *testing.T) {
		fake, calls := w26bAnswerFake()
		router := newRouter(newInstanceHandler(fake))

		rec := do(router, w26bAdminPost(completePath("Wait_1"), "application/json", `{"variables":{"answer":"yes"}}`))

		require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
		assert.Equal(t, []w26bAnswerCall{{testTenantID, testInstID, "Wait_1", map[string]any{"answer": "yes"}}}, *calls)
	})
}
