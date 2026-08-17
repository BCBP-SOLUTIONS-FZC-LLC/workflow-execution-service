package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/openbao"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkeystream"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/pkg/connectors"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/pkg/connectors/aliasconfig"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/pkg/registry"
)

const (
	initialBackoff = 500 * time.Millisecond
	maxBackoff     = 10 * time.Second
)

// runDispatchLoop is cmd/connector-worker's main consume loop — the
// equivalent of cmd/worker's pollQueueTopology goroutine. Ctx cancellation
// stops picking up new work; in-flight dispatches are tracked via wg and
// drained by main.go before the process exits.
func runDispatchLoop(ctx context.Context, d *deps, wg *sync.WaitGroup, wlog port.Logger) {
	go reclaimLoop(ctx, d, wg, wlog)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		entries, err := d.consumer.ReadGroup(ctx, d.streamKey, d.streamGroup, d.consumerName, d.batchSize, d.blockTimeout)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			wlog.Warn("connector-worker: read group failed", map[string]any{"error": err.Error()})
			time.Sleep(time.Second)
			continue
		}
		for _, entry := range entries {
			dispatchEntry(ctx, d, wg, entry, wlog)
		}
	}
}

// reclaimLoop periodically claims entries idle for at least claimMinIdle and
// still pending under the group — an entry whose original consumer died
// mid-dispatch becomes reclaimable this way rather than stuck forever.
func reclaimLoop(ctx context.Context, d *deps, wg *sync.WaitGroup, wlog port.Logger) {
	ticker := time.NewTicker(d.claimMinIdle)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			entries, err := d.consumer.ReclaimStale(ctx, d.streamKey, d.streamGroup, d.consumerName, d.claimMinIdle, d.batchSize)
			if err != nil {
				wlog.Warn("connector-worker: reclaim stale failed", map[string]any{"error": err.Error()})
				continue
			}
			for _, entry := range entries {
				dispatchEntry(ctx, d, wg, entry, wlog)
			}
		}
	}
}

// dispatchJob is one Stream entry, parsed into its typed fields
// (valkeystream.EventPublisher's own field names on the producer side).
type dispatchJob struct {
	tenantID       uuid.UUID
	taskID         uuid.UUID
	departmentID   uuid.UUID
	connectorType  string
	resolvedInputs map[string]any
}

func parseDispatchJob(entry valkeystream.Entry) (dispatchJob, error) {
	tenantID, err := uuid.Parse(entry.Fields["tenant_id"])
	if err != nil {
		return dispatchJob{}, fmt.Errorf("parse tenant_id: %w", err)
	}
	taskID, err := uuid.Parse(entry.Fields["task_id"])
	if err != nil {
		return dispatchJob{}, fmt.Errorf("parse task_id: %w", err)
	}
	departmentID, err := uuid.Parse(entry.Fields["department_id"])
	if err != nil {
		return dispatchJob{}, fmt.Errorf("parse department_id: %w", err)
	}
	var resolvedInputs map[string]any
	if raw := entry.Fields["resolved_inputs"]; raw != "" {
		if err := json.Unmarshal([]byte(raw), &resolvedInputs); err != nil {
			return dispatchJob{}, fmt.Errorf("parse resolved_inputs: %w", err)
		}
	}
	return dispatchJob{
		tenantID: tenantID, taskID: taskID, departmentID: departmentID,
		connectorType: entry.Fields["connector_type"], resolvedInputs: resolvedInputs,
	}, nil
}

// dispatchEntry routes entry to its connector type's bounded pool. A
// malformed entry (can't even identify the task) is acked and dropped —
// nothing else is possible. An unrecognized connector type still fails the
// task via the completion client before acking, since connector tasks are
// fully automation-only (no human fallback exists to leave it stuck on).
func dispatchEntry(ctx context.Context, d *deps, wg *sync.WaitGroup, entry valkeystream.Entry, wlog port.Logger) {
	job, err := parseDispatchJob(entry)
	if err != nil {
		wlog.Error("connector-worker: malformed stream entry, acking to avoid a poison-pill loop", map[string]any{"entry_id": entry.ID, "error": err.Error()})
		ackEntry(ctx, d, entry.ID, wlog)
		return
	}

	pool, ok := d.pools[job.connectorType]
	if !ok {
		wlog.Error("connector-worker: unknown connector type", map[string]any{"connector_type": job.connectorType, "task_id": job.taskID})
		if err := d.completion.Fail(ctx, job.tenantID, job.taskID, "unknown_connector_type"); err != nil {
			wlog.Warn("connector-worker: fail-signal call failed, leaving entry unacked for redelivery", map[string]any{"task_id": job.taskID, "error": err.Error()})
			return
		}
		ackEntry(ctx, d, entry.ID, wlog)
		return
	}

	wg.Add(1)
	pool.sem <- struct{}{}
	go func() {
		defer wg.Done()
		defer func() { <-pool.sem }()
		runJob(ctx, d, pool, job, entry.ID, wlog)
	}()
}

// runJob resolves secret_ref inputs, dispatches with the pool's own
// retry/backoff policy, and reports the outcome via the completion client —
// only acking the Stream entry once that call has actually succeeded (LLD
// §6.5 step 0: an unreported outcome must stay reclaimable, never silently
// dropped).
func runJob(ctx context.Context, d *deps, pool *typePool, job dispatchJob, entryID string, wlog port.Logger) {
	execCtx, cancel := context.WithTimeout(ctx, pool.timeout)
	defer cancel()

	input, err := resolveSecrets(execCtx, d.openbao, job.connectorType, job.resolvedInputs)
	if err != nil {
		finishFailed(ctx, d, job, entryID, classifyError(err), wlog)
		return
	}

	execCtx = connectors.WithDepartments(execCtx, []string{job.departmentID.String()})
	retryable := isRetryable(pool.retry, job.connectorType, input, d.aliases)

	output, err := runWithRetry(execCtx, pool.connector, retryable, input)
	if err != nil {
		finishFailed(ctx, d, job, entryID, classifyError(err), wlog)
		return
	}

	if err := d.completion.Complete(ctx, job.tenantID, job.taskID, output); err != nil {
		wlog.Warn("connector-worker: completion call failed, leaving entry unacked for redelivery", map[string]any{"task_id": job.taskID, "error": err.Error()})
		return
	}
	ackEntry(ctx, d, entryID, wlog)
}

func finishFailed(ctx context.Context, d *deps, job dispatchJob, entryID, errorClass string, wlog port.Logger) {
	if err := d.completion.Fail(ctx, job.tenantID, job.taskID, errorClass); err != nil {
		wlog.Warn("connector-worker: fail-signal call failed, leaving entry unacked for redelivery", map[string]any{"task_id": job.taskID, "error": err.Error()})
		return
	}
	ackEntry(ctx, d, entryID, wlog)
}

func ackEntry(ctx context.Context, d *deps, entryID string, wlog port.Logger) {
	if err := d.consumer.Ack(ctx, d.streamKey, d.streamGroup, entryID); err != nil {
		wlog.Warn("connector-worker: ack failed", map[string]any{"entry_id": entryID, "error": err.Error()})
	}
}

// resolveSecrets substitutes every registry.Field.IsSecretRef() input's
// value (an OpenBao path) with its real credential, immediately before
// Execute() is called — Connector implementations never see a path, only
// the resolved value (LLD §6.2 Decision #5; see this feature's design-doc
// follow-up for the full boundary rationale).
func resolveSecrets(ctx context.Context, reader *openbao.Reader, connectorType string, resolvedInputs map[string]any) (map[string]any, error) {
	def, ok := registry.All()[connectorType]
	if !ok {
		return resolvedInputs, nil
	}
	out := make(map[string]any, len(resolvedInputs))
	for k, v := range resolvedInputs {
		out[k] = v
	}
	for _, field := range def.Inputs {
		if !field.IsSecretRef() {
			continue
		}
		path, ok := out[field.Name].(string)
		if !ok || path == "" {
			continue
		}
		value, err := reader.Read(ctx, path, field.Name)
		if err != nil {
			return nil, fmt.Errorf("resolve secret %q: %w", field.Name, err)
		}
		out[field.Name] = value
	}
	return out, nil
}

// isRetryable resolves the ratified per-type retry policy (LLD Decision #6):
// safe types always retry, unsafe types never do, and rest-call's
// conditional policy depends on the resolved alias's own HTTP method.
func isRetryable(policy registry.RetryPolicy, connectorType string, input map[string]any, aliases aliasconfig.Config) bool {
	switch policy {
	case registry.RetryPolicySafe:
		return true
	case registry.RetryPolicyUnsafe:
		return false
	case registry.RetryPolicyConditional:
		if connectorType != registry.TypeRestCall {
			return false
		}
		alias, _ := input["endpointAlias"].(string)
		ep, err := aliasconfig.ResolveEndpoint(aliases, alias)
		if err != nil {
			return false
		}
		return registry.IsIdempotentMethod(ep.Method)
	default:
		return false
	}
}

// runWithRetry retries with capped exponential backoff until ctx's own
// deadline fires — the pool's timeout is the retry ceiling, not a fixed
// attempt count (LLD §6.5 step 2: "the per-type internal execution timeout
// covers total time including retries/backoff").
func runWithRetry(ctx context.Context, c connectors.Connector, retryable bool, input map[string]any) (map[string]any, error) {
	if !retryable {
		out, err := c.Execute(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("connector execute: %w", err)
		}
		return out, nil
	}
	backoff := initialBackoff
	for {
		out, err := c.Execute(ctx, input)
		if err == nil {
			return out, nil
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("connector execute: %w", ctx.Err())
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

// classifyError maps a dispatch failure to the small, closed error_class
// vocabulary the completion endpoint's Fail call carries — a real
// misconfiguration (secret_not_found, validation_error) vs. an infra blip
// (upstream_error, timeout).
func classifyError(err error) string {
	switch {
	case errors.Is(err, openbao.ErrSecretNotFound):
		return "secret_not_found"
	case errors.Is(err, openbao.ErrUnauthorized):
		return "secret_unauthorized"
	case errors.Is(err, connectors.ErrMissingInternalAuth):
		return "missing_internal_auth"
	case errors.Is(err, connectors.ErrValidation):
		return "validation_error"
	case errors.Is(err, connectors.ErrUpstream):
		return "upstream_error"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	default:
		return "unknown_error"
	}
}
