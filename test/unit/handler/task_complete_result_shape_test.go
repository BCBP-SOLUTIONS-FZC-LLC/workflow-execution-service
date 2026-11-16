package handler_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompleteTask_ResultJSONThatIsNotAnObjectIsRefused(t *testing.T) {
	cases := map[string]string{
		"string":  `"approved"`,
		"array":   `[1,2]`,
		"number":  `7`,
		"boolean": `true`,
		"null":    `null`,
	}
	for name, value := range cases {
		t.Run(name, func(t *testing.T) {
			fake, calls := hreTaskFake()
			router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

			w := do(router, hreRawRequest(http.MethodPost, hreTaskPath(testTaskID.String(), "complete"), `{"record_version":4,"result_json":`+value+`}`))

			require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			prob := hreDecodeProblem(t, w)
			assert.Equal(t, "BAD_REQUEST", prob.Code)
			require.Len(t, prob.InvalidParams, 1)
			assert.Equal(t, "result_json", prob.InvalidParams[0].Name)
			assert.Empty(t, *calls, "a refused payload must not reach the service")
		})
	}
}

func TestCompleteTask_ResultJSONObjectOrAbsentReachesTheService(t *testing.T) {
	cases := map[string]struct{ body, want string }{
		"object": {body: `{"record_version":4,"result_json":{"approved":true}}`, want: `{"approved":true}`},
		"absent": {body: `{"record_version":4}`, want: ``},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			fake, calls := hreTaskFake()
			router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

			w := do(router, hreRawRequest(http.MethodPost, hreTaskPath(testTaskID.String(), "complete"), tc.body))

			require.Equal(t, http.StatusAccepted, w.Code, w.Body.String())
			require.Len(t, *calls, 1)
			assert.Equal(t, tc.want, (*calls)[0].result)
		})
	}
}
