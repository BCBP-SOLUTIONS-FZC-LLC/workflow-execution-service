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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
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
// drained by main.go before the process exits. It returns only once the
// reclaim loop has too, so nothing adds to wg after main starts waiting.
func runDispatchLoop(ctx context.Context, d *deps, wg *sync.WaitGroup, wlog port.Logger) {
	var reclaim sync.WaitGroup
	reclaim.Add(1)
	go func() {
		defer reclaim.Done()
		reclaimLoop(ctx, d, wg, wlog)
	}()
	defer reclaim.Wait()

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
		dispatchBatch(ctx, d, wg, entries, wlog)
	}
}

// dispatchBatch marks the whole batch in flight before dispatching any of it,
// since later entries wait behind earlier ones for a pool slot. An entry this
// consumer already holds is skipped: its first copy acks it.
func dispatchBatch(ctx context.Context, d *deps, wg *sync.WaitGroup, entries []valkeystream.Entry, wlog port.Logger) {
	claimed := make([]valkeystream.Entry, 0, len(entries))
	for _, entry := range entries {
		if d.inflight.claim(entry.ID) {
			claimed = append(claimed, entry)
		}
	}
	for _, entry := range claimed {
		handleEntry(ctx, d, wg, entry, wlog)
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
			dispatchBatch(ctx, d, wg, entries, wlog)
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

// handleEntry routes entry to its connector type's bounded pool. A
// malformed entry (can't even identify the task) is acked and dropped —
// nothing else is possible. Everything that could stop the outcome being
// reported is settled before the connector runs, and a task that already has
// an outcome only re-sends it (workflow_connectors.md §6.5 step 0).
func handleEntry(ctx context.Context, d *deps, wg *sync.WaitGroup, entry valkeystream.Entry, wlog port.Logger) {
	handedOff := false
	defer func() {
		if !handedOff {
			d.inflight.remove(entry.ID)
		}
	}()

	job, err := parseDispatchJob(entry)
	if err != nil {
		wlog.Error("connector-worker: malformed stream entry, acking to avoid a poison-pill loop", map[string]any{"entry_id": entry.ID, "error": err.Error()})
		ackEntry(ctx, d, entry.ID, wlog)
		return
	}

	if d.maxDeliveries > 0 && entry.Deliveries > d.maxDeliveries {
		deadLetter(ctx, d, entry, job, wlog)
		return
	}

	if resendRecordedOutcome(ctx, d, job, entry.ID, wlog) {
		return
	}

	pool, ok := d.pools[job.connectorType]
	if !ok {
		wlog.Error("connector-worker: unknown connector type", map[string]any{"connector_type": job.connectorType, "task_id": job.taskID})
		report(ctx, d, job, dispatchOutcome{ErrorClass: "unknown_connector_type"}, entry.ID, wlog)
		return
	}

	wg.Add(1)
	pool.sem <- struct{}{}
	handedOff = true
	go func() {
		defer wg.Done()
		defer d.inflight.remove(entry.ID)
		defer func() { <-pool.sem }()
		runJob(ctx, d, pool, job, entry.ID, wlog)
	}()
}

// resendRecordedOutcome reports whether the entry has been handled without
// running anything: either the task's outcome was re-sent, or it could not
// be read and the entry is left pending rather than risk a second run.
func resendRecordedOutcome(ctx context.Context, d *deps, job dispatchJob, entryID string, wlog port.Logger) bool {
	outcome, found, err := recordedOutcome(ctx, d, job.taskID)
	if err != nil {
		wlog.Warn("connector-worker: recorded outcome unreadable, leaving entry pending", map[string]any{"task_id": job.taskID, "error": err.Error()})
		return true
	}
	if found {
		report(ctx, d, job, outcome, entryID, wlog)
	}
	return found
}

// runJob takes the task's execution lease, resolves secret_ref inputs,
// dispatches with the pool's own retry/backoff policy, records the outcome,
// and only then reports it. Once started, a run is detached from shutdown:
// its side effect may already have happened, so it must still be recorded
// and reported.
func runJob(ctx context.Context, d *deps, pool *typePool, job dispatchJob, entryID string, wlog port.Logger) {
	if ctx.Err() != nil {
		return
	}
	ctx = context.WithoutCancel(ctx)
	leaseTTL := pool.timeout + executionLeaseMargin
	if !takeLease(ctx, d, job.taskID, leaseTTL, wlog) {
		return
	}
	d.inflight.setDeadline(entryID, d.clock().Add(leaseTTL))
	// A previous lease holder may have finished between the check in
	// handleEntry and this lease.
	if resendRecordedOutcome(ctx, d, job, entryID, wlog) {
		return
	}

	outcome := execute(ctx, d, pool, job)
	recordOutcome(ctx, d, job.taskID, outcome, wlog)
	report(ctx, d, job, outcome, entryID, wlog)
}

func execute(ctx context.Context, d *deps, pool *typePool, job dispatchJob) dispatchOutcome {
	execCtx, cancel := context.WithTimeout(ctx, pool.timeout)
	defer cancel()

	input, err := resolveSecrets(execCtx, d.openbao, job)
	if err != nil {
		return dispatchOutcome{ErrorClass: classifyError(err)}
	}

	execCtx = connectors.WithDepartments(execCtx, []string{job.departmentID.String()})
	retryable := isRetryable(pool.retry, job.connectorType, input, d.aliases)

	output, err := runWithRetry(execCtx, pool.currentConnector(), retryable, input)
	if err != nil {
		return dispatchOutcome{ErrorClass: classifyError(err)}
	}
	return dispatchOutcome{Output: output}
}

func deadLetter(ctx context.Context, d *deps, entry valkeystream.Entry, job dispatchJob, wlog port.Logger) {
	if err := d.consumer.DeadLetter(ctx, d.streamKey, d.streamGroup, d.deadLetterKey, entry, "max_deliveries_exceeded"); err != nil {
		wlog.Warn("connector-worker: dead-letter failed, leaving entry pending", map[string]any{"entry_id": entry.ID, "task_id": job.taskID, "error": err.Error()})
		return
	}
	if observability.ConnectorDispatchDeadLetteredTotal != nil {
		observability.ConnectorDispatchDeadLetteredTotal.WithLabelValues(job.connectorType).Inc()
	}
	wlog.Error("connector-worker: entry exceeded its delivery cap and was dead-lettered; the task stays open", map[string]any{
		"entry_id": entry.ID, "task_id": job.taskID, "tenant_id": job.tenantID, "deliveries": entry.Deliveries,
	})
}

func ackEntry(ctx context.Context, d *deps, entryID string, wlog port.Logger) {
	if err := d.consumer.Ack(ctx, d.streamKey, d.streamGroup, entryID); err != nil {
		wlog.Warn("connector-worker: ack failed", map[string]any{"entry_id": entryID, "error": err.Error()})
	}
}

// resolveSecrets sets every registry.Field.IsSecretRef() input to the
// tenant's stored credential, immediately before Execute() is called —
// Connector implementations never see a path, only the resolved value (LLD
// §6.2 Decision #5). The path is always the job's own, and nothing a diagram
// says chooses it: the worker's OpenBao token spans every tenant, so a value
// an old plan still carries for a secret field is discarded unread. A field
// the tenant stored nothing for is left out: the registry does not say which
// of a type's fields a provider needs, so the connector refuses an absent
// credential itself (workflow_connectors.md §6.2).
func resolveSecrets(ctx context.Context, reader *openbao.Reader, job dispatchJob) (map[string]any, error) {
	def, ok := registry.All()[job.connectorType]
	if !ok {
		return job.resolvedInputs, nil
	}
	out := make(map[string]any, len(job.resolvedInputs))
	for k, v := range job.resolvedInputs {
		out[k] = v
	}
	for _, field := range def.Inputs {
		if !field.IsSecretRef() {
			continue
		}
		delete(out, field.Name)
		value, err := reader.Read(ctx, ownSecretPath(job, field.Name), field.Name)
		if errors.Is(err, openbao.ErrSecretNotFound) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("resolve secret %q: %w", field.Name, err)
		}
		out[field.Name] = value
	}
	return out, nil
}

// ownSecretPath mirrors definition_service's WriteCredential path.
func ownSecretPath(job dispatchJob, fieldName string) string {
	return fmt.Sprintf("connectors/%s/%s/%s", job.tenantID, job.connectorType, fieldName)
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
	case errors.Is(err, openbao.ErrInvalidPath):
		return "secret_ref_invalid"
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
