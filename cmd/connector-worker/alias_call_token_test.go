package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/connectors/aliasconfig"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/connectors/restcall"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/registry"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
)

// workflow-connectors refuses an empty InternalToken, so an unset setting
// yields a non-empty placeholder and a set one wins.
func TestAliasCallToken(t *testing.T) {
	t.Parallel()

	assert.Equal(t, aliasCallPlaceholder, aliasCallToken(&config.Config{}))
	assert.NotEmpty(t, aliasCallToken(&config.Config{}))
	assert.Equal(t, "alias-tok", aliasCallToken(&config.Config{ConnectorAliasCallToken: "alias-tok"}))
}

// With no CONNECTOR_ALIAS_CALL_TOKEN the worker sends the placeholder to an
// alias target; this is the only unit test that proves what reaches the wire.
func TestAliasCall_SendsThePlaceholderTokenWhenNoneIsConfigured(t *testing.T) {
	for name, tc := range map[string]struct {
		cfg  *config.Config
		want string
	}{
		"none configured": {&config.Config{}, aliasCallPlaceholder},
		"configured":      {&config.Config{ConnectorAliasCallToken: "alias-tok"}, "alias-tok"},
	} {
		t.Run(name, func(t *testing.T) {
			var got atomic.Value
			target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got.Store(r.Header.Get("x-internal-token"))
				_, _ = w.Write([]byte(`{}`))
			}))
			t.Cleanup(target.Close)
			aliases := aliasconfig.Config{RestCall: []aliasconfig.Endpoint{{Alias: "echo", Method: "GET", BaseURL: target.URL, PathTemplate: "/echo", Timeout: time.Second}}}
			pool := &typePool{connectorType: registry.TypeRestCall, sem: make(chan struct{}, 1), timeout: 5 * time.Second, connector: restcall.New(aliases, nil, aliasCallToken(tc.cfg))}
			job := dispatchJob{tenantID: uuid.New(), taskID: uuid.New(), departmentID: uuid.New(), principalSub: "sub", connectorType: registry.TypeRestCall, resolvedInputs: map[string]any{"endpointAlias": "echo"}}

			outcome := execute(context.Background(), &deps{retry: testBounds, aliases: aliases}, pool, job, noopLogger{})

			require.Empty(t, outcome.ErrorClass)
			assert.Equal(t, tc.want, got.Load())
		})
	}
	assert.Equal(t, "unset", aliasCallPlaceholder, "the placeholder reads as what it is, not as a credential")
}
