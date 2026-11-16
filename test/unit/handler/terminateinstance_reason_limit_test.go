package handler_test

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
)

func TestTerminateInstance_ReasonOf501Characters_Returns400(t *testing.T) {
	called := false
	router := newRouter(handler.New(handler.Services{Instances: &fakeInstanceService{
		terminate: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string) error {
			called = true
			return nil
		},
	}}))

	w := do(router, hrdPublicRequest(http.MethodPost, instancePath(testInstID)+"/terminate",
		`{"reason":"`+strings.Repeat("r", 501)+`"}`, true))

	p := hrdDecodeProblem(t, w, http.StatusBadRequest)
	assert.Equal(t, "BAD_REQUEST", p.Code)
	assert.Equal(t, []hrdParam{{Name: "reason", Reason: "is not valid"}}, p.InvalidParams)
	hrdAssertNoInternalText(t, w)
	assert.NotContains(t, w.Body.String(), strings.Repeat("r", 20), "the oversized reason is not echoed back")
	assert.False(t, called, "a refused request must not terminate the instance")
}
