package handler_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompleteTask_UnusableCallerIdentity_Returns500(t *testing.T) {
	for _, tc := range hreIdentityCases() {
		t.Run(tc.name, func(t *testing.T) {
			fake, calls := hreTaskFake()
			h := newHandler(fake, &fakeEligibilityChecker{})
			router := newRouter(h)
			if tc.bare {
				router = hreBareEngine(http.MethodPost, "/api/v1/tasks/:id/complete", h.CompleteTask)
			}
			r := hreRawRequest(http.MethodPost, hreTaskPath(testTaskID.String(), "complete"), `{"record_version":4}`)
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

func TestCompleteTask_EmptyOrMalformedJSONBody_Returns400(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{name: "empty body", body: ``},
		{name: "truncated object", body: `{"record_version":`},
		{name: "not json", body: `{bad`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake, calls := hreTaskFake()
			router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

			w := do(router, hreRawRequest(http.MethodPost, hreTaskPath(testTaskID.String(), "complete"), tc.body))

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

func TestCompleteTask_WrongTypeRecordVersion_Returns400(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{name: "string", body: `{"record_version":"2"}`},
		{name: "fraction", body: `{"record_version":2.5}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake, calls := hreTaskFake()
			router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

			w := do(router, hreRawRequest(http.MethodPost, hreTaskPath(testTaskID.String(), "complete"), tc.body))

			require.Equal(t, http.StatusBadRequest, w.Code)
			prob := hreDecodeProblem(t, w)
			assert.Equal(t, "BAD_REQUEST", prob.Code)
			assert.Equal(t, "request body has a field of the wrong type", prob.Detail)
			assert.Equal(t, []hreParam{{Name: "record_version", Reason: "has the wrong type"}}, prob.InvalidParams)
			assert.Empty(t, *calls, "a refused request must not reach the service")
			hreAssertNoInternalText(t, w)
		})
	}
}

func TestCompleteTask_NonUUIDTaskID_Returns400(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{name: "valid body", body: `{"record_version":4}`},
		{name: "invalid body still reports the id", body: `{bad`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fake, calls := hreTaskFake()
			router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

			w := do(router, hreRawRequest(http.MethodPost, hreTaskPath("not-a-uuid", "complete"), tc.body))

			require.Equal(t, http.StatusBadRequest, w.Code)
			prob := hreDecodeProblem(t, w)
			assert.Equal(t, "BAD_REQUEST", prob.Code)
			assert.Equal(t, "invalid id path parameter", prob.Detail)
			assert.Empty(t, prob.InvalidParams)
			assert.Empty(t, *calls, "a refused request must not reach the service")
			hreAssertNoInternalText(t, w)
		})
	}
}
