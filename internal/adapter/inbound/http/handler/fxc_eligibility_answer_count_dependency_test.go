package handler

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

func fxcRespondWith(t *testing.T, err error) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/", nil)
	errResponse(c, err)
	return w
}

func TestErrResponse_AnEligibilityAnswerOfTheWrongSizeCountsAsAnIAMDependencyError(t *testing.T) {
	obstest.Init(t)
	iam := observability.UpstreamDependencyErrorsTotal.WithLabelValues("iam")
	definition := observability.UpstreamDependencyErrorsTotal.WithLabelValues("definition_service")
	iamBefore, definitionBefore := testutil.ToFloat64(iam), testutil.ToFloat64(definition)

	w := fxcRespondWith(t, fmt.Errorf("check assignee eligibility: %w", port.ErrEligibilityAnswerCount))

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Equal(t, iamBefore+1, testutil.ToFloat64(iam), "the wrong-sized answer shows on the upstream error alert for iam")
	assert.Equal(t, definitionBefore, testutil.ToFloat64(definition))
}

func TestErrResponse_AnErrorThatIsNoUpstreamFailureCountsNoDependencyError(t *testing.T) {
	obstest.Init(t)
	iam := observability.UpstreamDependencyErrorsTotal.WithLabelValues("iam")
	before := testutil.ToFloat64(iam)

	w := fxcRespondWith(t, errors.Join(port.ErrNotAssignee))

	require.Equal(t, http.StatusForbidden, w.Code)
	assert.Equal(t, before, testutil.ToFloat64(iam))
}
