package handler

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// A callback whose earlier attempt is still in flight is retried by the worker; the answer says so instead of
// claiming a timeout.
func TestMapErr_CallbackInFlightIsRetryableAndSaysWhy(t *testing.T) {
	status, code, detail := mapErr(fmt.Errorf("complete callback: %w", port.ErrCallbackInFlight))

	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Equal(t, CodeUpstreamUnavailable, code)
	assert.Equal(t, "an earlier attempt of this callback is still in progress; retry later", detail)
}
