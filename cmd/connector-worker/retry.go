package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/connectors"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/connectors/aliasconfig"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/registry"
)

type retryBounds struct {
	MaxAttempts    int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

func aliasMethod(connectorType string, input map[string]any, aliases aliasconfig.Config) string {
	if connectorType != registry.TypeRestCall {
		return ""
	}
	alias, _ := input["endpointAlias"].(string)
	endpoint, err := aliasconfig.ResolveEndpoint(aliases, alias)
	if err != nil {
		return ""
	}
	return endpoint.Method
}

func executeWithRetry(
	ctx context.Context, conn connectors.Connector, method string, input map[string]any,
	bounds retryBounds, observe func(attempt int, decision connectors.RetryDecision),
) (map[string]any, error) {
	backoff := bounds.InitialBackoff
	for attempt := 1; ; attempt++ {
		out, err := conn.Execute(ctx, input)
		if err == nil {
			return out, nil
		}
		decision := connectors.DecideRetry(conn.Type(), err, method)
		observe(attempt, decision)
		if !decision.Retry || attempt >= bounds.MaxAttempts {
			return out, fmt.Errorf("connector execute: %w", err)
		}
		select {
		case <-ctx.Done():
			return out, fmt.Errorf("connector execute: %w", ctx.Err())
		case <-time.After(withJitter(backoff)):
		}
		backoff = min(backoff*2, bounds.MaxBackoff)
	}
}

func withJitter(d time.Duration) time.Duration {
	return d + time.Duration(rand.Int64N(int64(d/2)+1))
}

func observeFailures(wlog port.Logger, job dispatchJob, input map[string]any) func(int, connectors.RetryDecision) {
	provider, _ := input["provider"].(string)
	if provider == "" {
		provider = "none"
	}
	return func(attempt int, decision connectors.RetryDecision) {
		fields := map[string]any{"task_id": job.taskID, "tenant_id": job.tenantID, "connector_type": job.connectorType, "provider": provider, "attempt": attempt}
		for _, attr := range decision.LogAttrs() {
			fields[attr.Key] = attr.Value.Any()
		}
		wlog.Warn("connector-worker: connector call failed", fields)
		if observability.ConnectorTaskFailuresTotal != nil {
			observability.ConnectorTaskFailuresTotal.WithLabelValues(job.connectorType, provider, decision.Class.String(), strconv.FormatBool(decision.Retry)).Inc()
		}
	}
}
