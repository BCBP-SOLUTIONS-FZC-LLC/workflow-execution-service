package automationprincipal_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	httpadapter "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// A base URL that cannot be turned into a request is not "no principal": it fails closed as an upstream failure,
// so callers answer 503 like any other failed call to the token service.
func TestAutomationPrincipal_MalformedBaseURL_FailsToBuildTheRequestAsAnUpstreamFailure(t *testing.T) {
	for name, base := range map[string]string{"unterminated host": "http://[::1", "bad scheme": "ht!tp://token-service"} {
		t.Run(name, func(t *testing.T) {
			c := httpadapter.NewAutomationPrincipalClient(base, time.Second, time.Minute)

			principal, err := c.AutomationPrincipal(context.Background(), tenantA)

			require.Error(t, err)
			assert.Contains(t, err.Error(), "build token service request")
			assert.NotErrorIs(t, err, port.ErrNoAutomationPrincipal)
			assert.ErrorIs(t, err, httpadapter.ErrUpstreamUnavailable)
			assert.Equal(t, port.AutomationPrincipal{}, principal)
		})
	}
}
