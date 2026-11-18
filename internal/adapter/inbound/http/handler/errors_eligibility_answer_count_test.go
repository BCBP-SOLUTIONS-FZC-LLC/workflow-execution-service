package handler

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestMapErr_AnEligibilityAnswerOfTheWrongSizeIsAnUpstreamFailure(t *testing.T) {
	status, code, _ := mapErr(fmt.Errorf("check assignee eligibility: %w", port.ErrEligibilityAnswerCount))

	assert.Equal(t, http.StatusServiceUnavailable, status)
	assert.Equal(t, CodeUpstreamUnavailable, code)
}
