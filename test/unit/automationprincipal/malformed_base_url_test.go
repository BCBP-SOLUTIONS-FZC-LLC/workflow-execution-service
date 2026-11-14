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

// A base URL that cannot be turned into a request is a configuration fault, not "no principal" and not an upstream
// outage: the error matches neither sentinel, so callers answer it as an internal error.
func TestAutomationPrincipal_MalformedBaseURL_FailsToBuildTheRequestWithNeitherSentinel(t *testing.T) {
	for name, base := range map[string]string{"unterminated host": "http://[::1", "bad scheme": "ht!tp://token-service"} {
		t.Run(name, func(t *testing.T) {
			c := httpadapter.NewAutomationPrincipalClient(base, time.Second, time.Minute)

			principal, err := c.AutomationPrincipal(context.Background(), tenantA)

			require.Error(t, err)
			assert.Contains(t, err.Error(), "build token service request")
			assert.NotErrorIs(t, err, port.ErrNoAutomationPrincipal)
			assert.NotErrorIs(t, err, httpadapter.ErrUpstreamUnavailable)
			assert.Equal(t, port.AutomationPrincipal{}, principal)
		})
	}
}
