package handler_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func w25aAdminRaw(method, target, contentType, body string) *http.Request {
	r := w25aRaw(method, target, contentType, body)
	r.Header.Set("x-tenant-roles", "tenant_admin")
	return r
}

type w25aParam struct{ name, reason string }

func w25aParams(p w25aProblem) []w25aParam {
	out := make([]w25aParam, len(p.InvalidParams))
	for i, ip := range p.InvalidParams {
		out[i] = w25aParam{ip.Name, ip.Reason}
	}
	return out
}

// w25aSignalBody checks a 202 answer carries the signal message.
func w25aSignalBody(t *testing.T, body string) {
	t.Helper()
	assert.JSONEq(t, `{"message":"signal accepted"}`, body)
}

type w25aPauseCall struct {
	tenantID, instanceID, actor uuid.UUID
	reason                      string
	version                     int64
}

// A pause reason of 500 characters (counted as characters, not bytes) is accepted, as is a null or absent reason,
// and the service gets the reason as sent (empty when null or absent); 501 characters is a 400 naming the reason.
func TestPauseInstance_ReasonLength_BoundaryAndNullOrOmitted(t *testing.T) {
	cases := []struct {
		name       string
		body       string
		wantStatus int
		wantReason string
	}{
		{name: "500 characters", body: `{"reason":"` + strings.Repeat("r", 500) + `","record_version":3}`, wantStatus: http.StatusAccepted, wantReason: strings.Repeat("r", 500)},
		{name: "500 multi-byte characters", body: `{"reason":"` + strings.Repeat("é", 500) + `","record_version":3}`, wantStatus: http.StatusAccepted, wantReason: strings.Repeat("é", 500)},
		{name: "null reason", body: `{"reason":null,"record_version":3}`, wantStatus: http.StatusAccepted, wantReason: ""},
		{name: "omitted reason", body: `{"record_version":3}`, wantStatus: http.StatusAccepted, wantReason: ""},
		{name: "501 characters", body: `{"reason":"` + strings.Repeat("r", 501) + `","record_version":3}`, wantStatus: http.StatusBadRequest},
		{name: "501 multi-byte characters", body: `{"reason":"` + strings.Repeat("é", 501) + `","record_version":3}`, wantStatus: http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls []w25aPauseCall
			instances := &fakeInstanceService{
				pause: func(_ context.Context, tenantID, instanceID, actor uuid.UUID, reason string, version int64) error {
					calls = append(calls, w25aPauseCall{tenantID, instanceID, actor, reason, version})
					return nil
				},
			}
			router := newRouter(newInstanceHandler(instances))

			w := w25aServe(router, w25aAdminRaw(http.MethodPost, w25aInstancePath(testInstID, "/pause"), "application/json", tc.body))

			if tc.wantStatus == http.StatusBadRequest {
				p := w25aDecodeProblem(t, w, http.StatusBadRequest)
				assert.Equal(t, "BAD_REQUEST", p.Code)
				assert.Equal(t, []w25aParam{{"reason", "is not valid"}}, w25aParams(p))
				assert.Empty(t, calls)
				assert.NotContains(t, w.Body.String(), strings.Repeat("r", 20))
				return
			}
			require.Equal(t, http.StatusAccepted, w.Code, w.Body.String())
			w25aSignalBody(t, w.Body.String())
			assert.Equal(t, []w25aPauseCall{{testTenantID, testInstID, testUserID, tc.wantReason, 3}}, calls)
		})
	}
}

// A pause body whose record_version is a string or whose reason is a number is a 400 BAD_REQUEST naming the field
// with "has the wrong type"; the service is not called and the decoder's own wording stays out of the body.
func TestPauseInstance_WrongTypedFields_NameTheFieldAsTheWrongType(t *testing.T) {
	cases := []struct {
		name  string
		body  string
		field string
	}{
		{name: "record_version as a string", body: `{"record_version":"3"}`, field: "record_version"},
		{name: "record_version as an array", body: `{"record_version":[3]}`, field: "record_version"},
		{name: "reason as a number", body: `{"reason":7,"record_version":3}`, field: "reason"},
		{name: "reason as an object", body: `{"reason":{"a":1},"record_version":3}`, field: "reason"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var called bool
			instances := &fakeInstanceService{
				pause: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string, int64) error {
					called = true
					return nil
				},
			}
			router := newRouter(newInstanceHandler(instances))

			w := w25aServe(router, w25aAdminRaw(http.MethodPost, w25aInstancePath(testInstID, "/pause"), "application/json", tc.body))

			p := w25aDecodeProblem(t, w, http.StatusBadRequest)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.Equal(t, "request body has a field of the wrong type", p.Detail)
			assert.Equal(t, []w25aParam{{tc.field, "has the wrong type"}}, w25aParams(p))
			assert.NotContains(t, w.Body.String(), "cannot unmarshal")
			assert.NotContains(t, w.Body.String(), "Go struct")
			assert.False(t, called)
		})
	}
}

// When the request deadline passes during the instance read or the signal of a resume, the answer is a 503
// UPSTREAM_UNAVAILABLE with the detail "request timed out"; the wrapped cause stays out of the body.
func TestResumeInstance_DeadlinePassed_Answers503RequestTimedOut(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"deadline during the read", fmt.Errorf("read instance row: %w", context.DeadlineExceeded)},
		{"deadline during the signal", fmt.Errorf("signal workflow wf-123: %w", context.DeadlineExceeded)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var gotInstance uuid.UUID
			instances := &fakeInstanceService{
				resume: func(_ context.Context, tenantID, instanceID, actor uuid.UUID, version int64) error {
					assert.Equal(t, testTenantID, tenantID)
					assert.Equal(t, testUserID, actor)
					assert.EqualValues(t, 2, version)
					gotInstance = instanceID
					return tc.err
				},
			}
			router := newRouter(newInstanceHandler(instances))

			w := w25aServe(router, w25aAdminRaw(http.MethodPost, w25aInstancePath(testInstID, "/resume"), "application/json", `{"record_version":2}`))

			p := w25aDecodeProblem(t, w, http.StatusServiceUnavailable)
			assert.Equal(t, testInstID, gotInstance)
			assert.Equal(t, "UPSTREAM_UNAVAILABLE", p.Code)
			assert.Equal(t, "request timed out", p.Detail)
			assert.Equal(t, "https://errors.bcbp.io/execution/upstream-unavailable", p.Type)
			assert.Equal(t, w25aInstancePath(testInstID, "/resume"), p.Instance)
			assert.NotContains(t, w.Body.String(), "wf-123")
			assert.NotContains(t, w.Body.String(), "instance row")
		})
	}
}

// The terminate route's body checks, all answered before the service is called: a body that is empty, blank,
// truncated or a bare word is 400 "request body is not valid JSON"; text/plain is 415; a body over 10 MB is 413.
func TestTerminateInstance_BodyRefusals_AnswerFixedProblemsAndNeverTerminate(t *testing.T) {
	cases := []struct {
		name        string
		contentType string
		body        string
		wantStatus  int
		wantCode    string
		wantDetail  string
	}{
		{"empty body", "application/json", ``, http.StatusBadRequest, "BAD_REQUEST", "request body is not valid JSON"},
		{"blank body", "application/json", "  \n ", http.StatusBadRequest, "BAD_REQUEST", "request body is not valid JSON"},
		{"truncated JSON", "application/json", `{"reason":"dup`, http.StatusBadRequest, "BAD_REQUEST", "request body is not valid JSON"},
		{"bare word", "application/json", `nonsense`, http.StatusBadRequest, "BAD_REQUEST", "request body is not valid JSON"},
		{"text/plain", "text/plain", `{"reason":"duplicate"}`, http.StatusUnsupportedMediaType, "UNSUPPORTED_MEDIA_TYPE", "request body must be application/json"},
		{"over 10 MB", "application/json", `{"reason":"` + strings.Repeat("a", 11<<20) + `"}`, http.StatusRequestEntityTooLarge, "PAYLOAD_TOO_LARGE", "request body exceeds the 10 MB limit"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var called bool
			instances := &fakeInstanceService{
				terminate: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string) error {
					called = true
					return nil
				},
			}
			router := newRouter(newInstanceHandler(instances))

			w := w25aServe(router, w25aAdminRaw(http.MethodPost, w25aInstancePath(testInstID, "/terminate"), tc.contentType, tc.body))

			p := w25aDecodeProblem(t, w, tc.wantStatus)
			assert.Equal(t, tc.wantCode, p.Code)
			assert.Equal(t, tc.wantDetail, p.Detail)
			assert.Empty(t, p.InvalidParams)
			assert.False(t, called, "a refused body must not terminate the instance")
		})
	}
}

// A terminate body without a usable reason (absent, empty string or null) is a 400 naming reason as required.
func TestTerminateInstance_ReasonAbsentEmptyOrNull_NamesTheReasonAsRequired(t *testing.T) {
	cases := []struct{ name, body string }{
		{"empty object", `{}`},
		{"empty string", `{"reason":""}`},
		{"null", `{"reason":null}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var called bool
			instances := &fakeInstanceService{
				terminate: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string) error {
					called = true
					return nil
				},
			}
			router := newRouter(newInstanceHandler(instances))

			w := w25aServe(router, w25aAdminRaw(http.MethodPost, w25aInstancePath(testInstID, "/terminate"), "application/json", tc.body))

			p := w25aDecodeProblem(t, w, http.StatusBadRequest)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.Equal(t, "request body has missing or invalid fields", p.Detail)
			assert.Equal(t, []w25aParam{{"reason", "is required"}}, w25aParams(p))
			assert.False(t, called)
		})
	}
}

// The fire route refuses variables that are not an object (a string, an array, a number) as a 400 naming
// variables with "has the wrong type", and the wait is not answered.
func TestFireOutsideBoundary_VariablesThatAreNotAnObject_NameVariablesAsTheWrongType(t *testing.T) {
	path := "/api/v1/instances/" + testInstID.String() + "/nodes/Wait_1/boundaries/Timer_1/fire"
	cases := []struct {
		name string
		body string
	}{
		{"a string", `{"variables":"yes"}`},
		{"an array", `{"variables":["yes"]}`},
		{"a number", `{"variables":3}`},
		{"a boolean", `{"variables":true}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls int
			instances := &fakeInstanceService{
				answerOutsideWait: func(context.Context, uuid.UUID, uuid.UUID, port.ReadScope, string, map[string]any) error {
					calls++
					return errors.New("must not be called")
				},
			}
			router := newRouter(newInstanceHandler(instances))

			w := w25aServe(router, w25aAdminRaw(http.MethodPost, path, "application/json", tc.body))

			p := w25aDecodeProblem(t, w, http.StatusBadRequest)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.Equal(t, "request body has a field of the wrong type", p.Detail)
			assert.Equal(t, []w25aParam{{"variables", "has the wrong type"}}, w25aParams(p))
			assert.Zero(t, calls)
		})
	}
}
