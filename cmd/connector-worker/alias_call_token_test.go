package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/registry"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
)

// The Workflow services authenticate each other by caller identity and no
// longer have a shared token, so a call to an alias target carries none. This
// is the only unit test that proves what reaches the wire.
func TestAliasCall_SendsNoInternalTokenToTheAliasTarget(t *testing.T) {
	var sawToken atomic.Bool
	var calls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		for name := range r.Header {
			if http.CanonicalHeaderKey(name) == "X-Internal-Token" {
				sawToken.Store(true)
			}
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(target.Close)
	var body atomic.Value
	body.Store(fmt.Sprintf(`{"version":1,"restCall":[{"alias":"echo","method":"GET","baseURL":%q,"pathTemplate":"/echo"}]}`, target.URL))
	d, cfg := aliasRefreshFixture(t, &body)
	pool := &typePool{connectorType: registry.TypeRestCall, sem: make(chan struct{}, 1), timeout: 5 * time.Second}
	d.pools[registry.TypeRestCall] = pool
	d.retry = testBounds

	refreshAliases(context.Background(), d, &config.Config{DefinitionServiceURL: cfg.DefinitionServiceURL, ConnectorAliasFetchTimeout: time.Second}, noopLogger{})
	job := dispatchJob{tenantID: uuid.New(), taskID: uuid.New(), departmentID: uuid.New(), principalSub: "sub", connectorType: registry.TypeRestCall, resolvedInputs: map[string]any{"endpointAlias": "echo"}}

	outcome := execute(context.Background(), d, pool, job, noopLogger{})

	require.Empty(t, outcome.ErrorClass)
	require.EqualValues(t, 1, calls.Load(), "the alias target was called once")
	assert.False(t, sawToken.Load(), "no x-internal-token reaches the alias target")
}
