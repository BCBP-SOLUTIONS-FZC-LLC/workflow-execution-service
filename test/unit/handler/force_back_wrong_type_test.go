package handler_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestForceBackInstance_WrongTypeOrInvalidJSONBody_Returns400(t *testing.T) {
	cases := []struct {
		name       string
		body       string
		wantDetail string
		wantParams []hreParam
	}{
		{name: "string record_version", body: `{"record_version":"3"}`,
			wantDetail: "request body has a field of the wrong type",
			wantParams: []hreParam{{Name: "record_version", Reason: "has the wrong type"}}},
		{name: "invalid JSON", body: `{"record_version":`, wantDetail: "request body is not valid JSON"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls int
			fake := &fakeInstanceService{forceBack: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string, int64) error {
				calls++
				return nil
			}}
			router := newRouter(newInstanceHandler(fake))

			w := do(router, hreRawRequest(http.MethodPost, instancePath(testInstID)+"/force-back", tc.body))

			require.Equal(t, http.StatusBadRequest, w.Code)
			prob := hreDecodeProblem(t, w)
			assert.Equal(t, "BAD_REQUEST", prob.Code)
			assert.Equal(t, tc.wantDetail, prob.Detail)
			assert.Equal(t, tc.wantParams, prob.InvalidParams)
			assert.Zero(t, calls, "a refused request must not reach the service")
			hreAssertNoInternalText(t, w)
		})
	}
}
