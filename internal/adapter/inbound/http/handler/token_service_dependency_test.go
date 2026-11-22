package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	adapterhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

func TestErrResponse_AFailedTokenServiceCallCountsAsATokenServiceDependencyError(t *testing.T) {
	obstest.Init(t)
	tokenService := observability.UpstreamDependencyErrorsTotal.WithLabelValues("token_service")
	iam := observability.UpstreamDependencyErrorsTotal.WithLabelValues("iam")
	tokenBefore, iamBefore := testutil.ToFloat64(tokenService), testutil.ToFloat64(iam)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer server.Close()
	client := adapterhttp.NewAutomationPrincipalClient(server.URL, time.Second, time.Minute)
	_, err := client.AutomationPrincipal(context.Background(), uuid.New())
	require.Error(t, err)

	w := fxcRespondWith(t, err)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Equal(t, tokenBefore+1, testutil.ToFloat64(tokenService))
	assert.Equal(t, iamBefore, testutil.ToFloat64(iam), "the IAM label is for the IAM client only")
}
