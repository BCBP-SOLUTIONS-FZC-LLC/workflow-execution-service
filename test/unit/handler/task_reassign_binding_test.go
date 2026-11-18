package handler_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReassignTask_UnusableCallerIdentity_Returns500(t *testing.T) {
	for _, tc := range hreIdentityCases() {
		t.Run(tc.name, func(t *testing.T) {
			fake, calls := hreTaskFake()
			h := newHandler(fake, &fakeEligibilityChecker{})
			router := newRouter(h)
			if tc.bare {
				router = hreBareEngine(http.MethodPost, "/api/v1/tasks/:id/reassign", h.ReassignTask)
			}
			r := hreRawRequest(http.MethodPost, hreTaskPath(testTaskID.String(), "reassign"), `{"new_user_id":"55555555-5555-5555-5555-555555555555","record_version":4}`)
			if tc.nonAdmin {
				r.Header.Del("x-tenant-roles")
			}
			if tc.header != "" {
				r.Header.Set(tc.header, tc.bad)
			}

			w := do(router, r)

			require.Equal(t, http.StatusInternalServerError, w.Code)
			prob := hreDecodeProblem(t, w)
			assert.Equal(t, "INTERNAL_ERROR", prob.Code)
			assert.Equal(t, tc.wantDetail, prob.Detail)
			assert.Empty(t, prob.InvalidParams)
			assert.Empty(t, *calls, "an unusable identity must not reach the service")
			assert.NotContains(t, w.Body.String(), "not-a-uuid", "the rejected header value must not be echoed")
			hreAssertNoInternalText(t, w)
		})
	}
}

func TestReassignTask_EmptyOrMalformedJSONBody_Returns400(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{name: "empty body", body: ``},
		{name: "whitespace only", body: " \n\t "},
		{name: "not json", body: `{bad`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake, calls := hreTaskFake()
			router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

			w := do(router, hreRawRequest(http.MethodPost, hreTaskPath(testTaskID.String(), "reassign"), tc.body))

			require.Equal(t, http.StatusBadRequest, w.Code)
			prob := hreDecodeProblem(t, w)
			assert.Equal(t, "BAD_REQUEST", prob.Code)
			assert.Equal(t, "request body is not valid JSON", prob.Detail)
			assert.Empty(t, prob.InvalidParams)
			assert.Empty(t, *calls, "a refused request must not reach the service")
			hreAssertNoInternalText(t, w)
		})
	}
}

func TestReassignTask_NonUUIDNewUserID_Returns400(t *testing.T) {
	fake, calls := hreTaskFake()
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, hreRawRequest(http.MethodPost, hreTaskPath(testTaskID.String(), "reassign"), `{"new_user_id":"abc","record_version":1}`))

	require.Equal(t, http.StatusBadRequest, w.Code)
	prob := hreDecodeProblem(t, w)
	assert.Equal(t, "BAD_REQUEST", prob.Code)
	assert.Empty(t, *calls, "a refused request must not reach the service")
	hreAssertNoInternalText(t, w)
}

func TestReassignTask_WrongJSONType_Returns400NamingTheJSONField(t *testing.T) {
	cases := []struct {
		name      string
		body      string
		wantField string
	}{
		{name: "new_user_id as a number", body: `{"new_user_id":123,"record_version":1}`, wantField: "new_user_id"},
		{name: "record_version as a string", body: `{"new_user_id":"55555555-5555-5555-5555-555555555555","record_version":"1"}`, wantField: "record_version"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake, calls := hreTaskFake()
			router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

			w := do(router, hreRawRequest(http.MethodPost, hreTaskPath(testTaskID.String(), "reassign"), tc.body))

			require.Equal(t, http.StatusBadRequest, w.Code)
			prob := hreDecodeProblem(t, w)
			assert.Equal(t, "BAD_REQUEST", prob.Code)
			assert.Equal(t, "request body has a field of the wrong type", prob.Detail)
			assert.Equal(t, []hreParam{{Name: tc.wantField, Reason: "has the wrong type"}}, prob.InvalidParams)
			assert.Empty(t, *calls, "a refused request must not reach the service")
			hreAssertNoInternalText(t, w)
		})
	}
}
