package handler_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
)

func hrdForceForwardFake(called *bool) *fakeInstanceService {
	return &fakeInstanceService{
		forceForward: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string, int64) error {
			*called = true
			return nil
		},
	}
}

func TestForceForwardInstance_NonUUIDInstanceID_Returns400(t *testing.T) {
	called := false
	router := newRouter(handler.New(handler.Services{Instances: hrdForceForwardFake(&called)}))

	w := do(router, hrdPublicRequest(http.MethodPost, "/api/v1/instances/not-a-uuid/force-forward",
		`{"target_node_key":"billing/prep","record_version":3}`, true))

	p := hrdDecodeProblem(t, w, http.StatusBadRequest)
	assert.Equal(t, "BAD_REQUEST", p.Code)
	assert.Equal(t, "invalid id path parameter", p.Detail)
	assert.Equal(t, []hrdParam{{Name: "id", Reason: "is not valid"}}, p.InvalidParams)
	hrdAssertNoInternalText(t, w)
	assert.False(t, called, "a refused request must not reach the service")
}

func TestForceForwardInstance_RecordVersionAbsentOrZero_NamesTheField(t *testing.T) {
	cases := []struct {
		name, body string
		wantReason string
	}{
		{"absent", `{"target_node_key":"billing/prep"}`, "is required"},
		{"zero", `{"target_node_key":"billing/prep","record_version":0}`, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			router := newRouter(handler.New(handler.Services{Instances: hrdForceForwardFake(&called)}))

			w := do(router, hrdPublicRequest(http.MethodPost, instancePath(testInstID)+"/force-forward", tc.body, true))

			p := hrdDecodeProblem(t, w, http.StatusBadRequest)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			require.Len(t, p.InvalidParams, 1)
			assert.Equal(t, "record_version", p.InvalidParams[0].Name)
			if tc.wantReason != "" {
				assert.Equal(t, tc.wantReason, p.InvalidParams[0].Reason)
			}
			hrdAssertNoInternalText(t, w)
			assert.False(t, called, "a refused request must not reach the service")
		})
	}
}

func TestForceForwardInstance_RecordVersionAsString_Returns400(t *testing.T) {
	called := false
	router := newRouter(handler.New(handler.Services{Instances: hrdForceForwardFake(&called)}))

	w := do(router, hrdPublicRequest(http.MethodPost, instancePath(testInstID)+"/force-forward",
		`{"target_node_key":"billing/prep","record_version":"3"}`, true))

	p := hrdDecodeProblem(t, w, http.StatusBadRequest)
	assert.Equal(t, "BAD_REQUEST", p.Code)
	assert.Equal(t, "request body has a field of the wrong type", p.Detail)
	assert.Equal(t, []hrdParam{{Name: "record_version", Reason: "has the wrong type"}}, p.InvalidParams)
	hrdAssertNoInternalText(t, w)
	assert.False(t, called, "a refused request must not reach the service")
}
