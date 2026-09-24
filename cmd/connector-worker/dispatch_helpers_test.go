package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/openbao"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/pkg/connectors"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/pkg/connectors/aliasconfig"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/pkg/registry"
)

func TestClassifyError(t *testing.T) {
	cases := map[string]error{
		"secret_not_found":      openbao.ErrSecretNotFound,
		"secret_unauthorized":   openbao.ErrUnauthorized,
		"missing_internal_auth": connectors.ErrMissingInternalAuth,
		"validation_error":      connectors.ErrValidation,
		"upstream_error":        connectors.ErrUpstream,
		"timeout":               context.DeadlineExceeded,
		"unknown_error":         errors.New("boom"),
	}
	for want, err := range cases {
		assert.Equal(t, want, classifyError(fmt.Errorf("wrapped: %w", err)), want)
	}
	assert.Equal(t, "secret_ref_invalid", classifyError(errSecretRefNotOwned))
	assert.Equal(t, "secret_ref_invalid", classifyError(openbao.ErrInvalidPath))
}

func TestIsRetryable(t *testing.T) {
	aliases := aliasconfig.Config{RestCall: []aliasconfig.Endpoint{
		{Alias: "read", Method: "GET"}, {Alias: "write", Method: "POST"},
	}}
	assert.True(t, isRetryable(registry.RetryPolicySafe, registry.TypeStorage, nil, aliases))
	assert.False(t, isRetryable(registry.RetryPolicyUnsafe, registry.TypeSendEmail, nil, aliases))
	assert.True(t, isRetryable(registry.RetryPolicyConditional, registry.TypeRestCall, map[string]any{"endpointAlias": "read"}, aliases))
	assert.False(t, isRetryable(registry.RetryPolicyConditional, registry.TypeRestCall, map[string]any{"endpointAlias": "write"}, aliases))
	assert.False(t, isRetryable(registry.RetryPolicyConditional, registry.TypeRestCall, map[string]any{"endpointAlias": "missing"}, aliases))
	assert.False(t, isRetryable(registry.RetryPolicyConditional, registry.TypeSQLQuery, nil, aliases))
	assert.False(t, isRetryable(registry.RetryPolicy("other"), registry.TypeRestCall, nil, aliases))
}

type flakyConnector struct {
	countingConnector
	failures int32
}

func (c *flakyConnector) Execute(ctx context.Context, in map[string]any) (map[string]any, error) {
	if c.calls.Add(1) <= c.failures {
		return nil, connectors.ErrUpstream
	}
	return map[string]any{"ok": true}, nil
}

func TestRunWithRetry(t *testing.T) {
	t.Run("a retryable call retries until it succeeds", func(t *testing.T) {
		conn := &flakyConnector{countingConnector: countingConnector{typ: "flaky"}, failures: 1}
		out, err := runWithRetry(context.Background(), conn, true, nil)
		assert.NoError(t, err)
		assert.Equal(t, true, out["ok"])
		assert.Equal(t, int32(2), conn.calls.Load())
	})
	t.Run("a non-retryable call fails at once", func(t *testing.T) {
		conn := &flakyConnector{countingConnector: countingConnector{typ: "flaky"}, failures: 1}
		_, err := runWithRetry(context.Background(), conn, false, nil)
		assert.ErrorIs(t, err, connectors.ErrUpstream)
		assert.Equal(t, int32(1), conn.calls.Load())
	})
	t.Run("retries stop at the deadline", func(t *testing.T) {
		conn := &flakyConnector{countingConnector: countingConnector{typ: "flaky"}, failures: 100}
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_, err := runWithRetry(ctx, conn, true, nil)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})
}
