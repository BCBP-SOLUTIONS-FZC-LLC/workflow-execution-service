package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/openbao"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/connectors"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/connectors/aliasconfig"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/connectors/shared"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/registry"
)

func TestClassifyError(t *testing.T) {
	cases := map[string]error{
		"secret_not_found":      openbao.ErrSecretNotFound,
		"secret_unauthorized":   openbao.ErrUnauthorized,
		"missing_internal_auth": connectors.ErrMissingInternalAuth,
		"validation_error":      connectors.ErrValidation,
		"missing_tenant":        connectors.ErrMissingTenant,
		"upstream_error":        connectors.ErrUpstream,
		"timeout":               context.DeadlineExceeded,
		"unknown_error":         errors.New("boom"),
	}
	for want, err := range cases {
		assert.Equal(t, want, classifyError(fmt.Errorf("wrapped: %w", err)), want)
	}
	assert.Equal(t, "secret_ref_invalid", classifyError(openbao.ErrInvalidPath))
}

func TestAliasMethod(t *testing.T) {
	aliases := aliasconfig.Config{RestCall: []aliasconfig.Endpoint{{Alias: "read", Method: "GET"}, {Alias: "write", Method: "POST"}}}
	assert.Equal(t, "GET", aliasMethod(registry.TypeRestCall, map[string]any{"endpointAlias": "read"}, aliases))
	assert.Equal(t, "POST", aliasMethod(registry.TypeRestCall, map[string]any{"endpointAlias": "write"}, aliases))
	assert.Empty(t, aliasMethod(registry.TypeRestCall, map[string]any{"endpointAlias": "missing"}, aliases))
	assert.Empty(t, aliasMethod(registry.TypeStorage, map[string]any{"endpointAlias": "read"}, aliases))
}

type step struct {
	out map[string]any
	err error
}

type scriptedConnector struct {
	typ   string
	steps []step
	calls int
}

func (c *scriptedConnector) Type() string { return c.typ }

func (c *scriptedConnector) Execute(context.Context, map[string]any) (map[string]any, error) {
	s := c.steps[min(c.calls, len(c.steps)-1)]
	c.calls++
	return s.out, s.err
}

var testBounds = retryBounds{MaxAttempts: 4, InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond}

func transient(err error) error { return shared.Transient("test", err) }

func TestExecuteWithRetry(t *testing.T) {
	run := func(t *testing.T, conn *scriptedConnector, method string) (map[string]any, []connectors.RetryDecision, error) {
		t.Helper()
		var seen []connectors.RetryDecision
		out, err := executeWithRetry(context.Background(), conn, method, nil, testBounds, func(_ int, d connectors.RetryDecision) { seen = append(seen, d) })
		return out, seen, err
	}

	t.Run("a transient failure of a safe type is retried until it succeeds", func(t *testing.T) {
		conn := &scriptedConnector{typ: registry.TypeStorage, steps: []step{{err: transient(connectors.ErrUpstream)}, {out: map[string]any{"ok": true}}}}
		out, seen, err := run(t, conn, "")
		require.NoError(t, err)
		assert.Equal(t, true, out["ok"])
		assert.Equal(t, 2, conn.calls)
		require.Len(t, seen, 1)
		assert.True(t, seen[0].Retry)
	})
	t.Run("transient retries stop at the attempt limit", func(t *testing.T) {
		conn := &scriptedConnector{typ: registry.TypeStorage, steps: []step{{err: transient(connectors.ErrUpstream)}}}
		_, seen, err := run(t, conn, "")
		assert.ErrorIs(t, err, connectors.ErrUpstream)
		assert.Equal(t, testBounds.MaxAttempts, conn.calls)
		assert.Len(t, seen, testBounds.MaxAttempts)
	})
	t.Run("a permanent failure is never retried", func(t *testing.T) {
		conn := &scriptedConnector{typ: registry.TypeStorage, steps: []step{{err: shared.Permanent("denied", connectors.ErrUpstream)}}}
		_, seen, err := run(t, conn, "")
		assert.Error(t, err)
		assert.Equal(t, 1, conn.calls)
		require.Len(t, seen, 1)
		assert.False(t, seen[0].Retry)
	})
	t.Run("an unclassified failure is never retried", func(t *testing.T) {
		conn := &scriptedConnector{typ: registry.TypeStorage, steps: []step{{err: errors.New("boom")}}}
		_, _, err := run(t, conn, "")
		assert.Error(t, err)
		assert.Equal(t, 1, conn.calls)
	})
	t.Run("rest-call retries a transient failure only for an idempotent method", func(t *testing.T) {
		get := &scriptedConnector{typ: registry.TypeRestCall, steps: []step{{err: transient(connectors.ErrUpstream)}, {out: map[string]any{"ok": true}}}}
		_, _, err := run(t, get, "GET")
		assert.NoError(t, err)
		assert.Equal(t, 2, get.calls)

		post := &scriptedConnector{typ: registry.TypeRestCall, steps: []step{{err: transient(connectors.ErrUpstream)}, {out: map[string]any{"ok": true}}}}
		_, _, err = run(t, post, "POST")
		assert.Error(t, err)
		assert.Equal(t, 1, post.calls)
	})
	t.Run("send-email retries only a message the provider never accepted", func(t *testing.T) {
		notDelivered := &scriptedConnector{typ: registry.TypeSendEmail, steps: []step{{err: transient(fmt.Errorf("%w: dns", connectors.ErrNotDelivered))}, {out: map[string]any{"sent": true}}}}
		_, _, err := run(t, notDelivered, "")
		assert.NoError(t, err)
		assert.Equal(t, 2, notDelivered.calls)

		unknown := &scriptedConnector{typ: registry.TypeSendEmail, steps: []step{{err: transient(fmt.Errorf("%w: timeout", connectors.ErrDeliveryUnknown))}, {out: map[string]any{"sent": true}}}}
		_, _, err = run(t, unknown, "")
		assert.Error(t, err)
		assert.Equal(t, 1, unknown.calls)
	})
	t.Run("the output of a failed call is returned with the error", func(t *testing.T) {
		conn := &scriptedConnector{typ: registry.TypeSendEmail, steps: []step{{out: map[string]any{"deliveryOutcome": "unknown"}, err: transient(fmt.Errorf("%w: timeout", connectors.ErrDeliveryUnknown))}}}
		out, _, err := run(t, conn, "")
		assert.Error(t, err)
		assert.Equal(t, "unknown", out["deliveryOutcome"])
	})
	t.Run("waiting between attempts stops at the deadline", func(t *testing.T) {
		conn := &scriptedConnector{typ: registry.TypeStorage, steps: []step{{err: transient(connectors.ErrUpstream)}}}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		slow := retryBounds{MaxAttempts: 100, InitialBackoff: time.Second, MaxBackoff: time.Second}
		_, err := executeWithRetry(ctx, conn, "", nil, slow, func(int, connectors.RetryDecision) {})
		assert.Error(t, err)
		assert.Equal(t, 1, conn.calls)
	})
}

type contextCapturingConnector struct {
	typ         string
	tenant      string
	tenantFound bool
	departments []string
}

func (c *contextCapturingConnector) Type() string { return c.typ }

func (c *contextCapturingConnector) Execute(ctx context.Context, _ map[string]any) (map[string]any, error) {
	c.tenant, c.tenantFound = connectors.TenantFromContext(ctx)
	c.departments, _ = connectors.DepartmentsFromContext(ctx)
	return map[string]any{}, nil
}

func TestExecute_CarriesTheJobsTenantAndDepartment(t *testing.T) {
	conn := &contextCapturingConnector{typ: "capture"}
	pool := &typePool{connectorType: conn.typ, sem: make(chan struct{}, 1), timeout: time.Second, connector: conn}
	job := dispatchJob{tenantID: uuid.New(), taskID: uuid.New(), departmentID: uuid.New(), connectorType: conn.typ}

	outcome := execute(context.Background(), &deps{retry: testBounds}, pool, job, noopLogger{})

	require.Empty(t, outcome.ErrorClass)
	assert.True(t, conn.tenantFound)
	assert.Equal(t, job.tenantID.String(), conn.tenant)
	assert.Equal(t, []string{job.departmentID.String()}, conn.departments)
}
