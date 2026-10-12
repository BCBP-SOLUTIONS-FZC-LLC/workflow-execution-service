package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

const (
	outcomeTTL           = 24 * time.Hour
	executionLeaseMargin = 10 * time.Second
)

type dispatchOutcome struct {
	Output     map[string]any `json:"output,omitempty"`
	ErrorClass string         `json:"error_class,omitempty"`
}

func outcomeKey(taskID uuid.UUID) string { return "connector-outcome:" + taskID.String() }
func leaseKey(taskID uuid.UUID) string   { return "connector-lease:" + taskID.String() }

func recordedOutcome(ctx context.Context, d *deps, taskID uuid.UUID) (dispatchOutcome, bool, error) {
	raw, err := d.dedup.Get(ctx, outcomeKey(taskID))
	if err != nil {
		return dispatchOutcome{}, false, fmt.Errorf("read recorded outcome: %w", err)
	}
	if raw == "" {
		return dispatchOutcome{}, false, nil
	}
	var outcome dispatchOutcome
	if err := json.Unmarshal([]byte(raw), &outcome); err != nil {
		return dispatchOutcome{}, false, fmt.Errorf("decode recorded outcome: %w", err)
	}
	return outcome, true, nil
}

func recordOutcome(ctx context.Context, d *deps, taskID uuid.UUID, outcome dispatchOutcome, wlog port.Logger) {
	raw, err := json.Marshal(outcome)
	if err == nil {
		err = d.dedup.Set(ctx, outcomeKey(taskID), string(raw), outcomeTTL)
	}
	if err != nil {
		wlog.Error("connector-worker: outcome not recorded, a redelivery will run the connector again", map[string]any{"task_id": taskID, "error": err.Error()})
	}
}

func takeLease(ctx context.Context, d *deps, taskID uuid.UUID, ttl time.Duration, wlog port.Logger) bool {
	ok, err := d.dedup.SetNX(ctx, leaseKey(taskID), "1", ttl)
	if err != nil {
		wlog.Warn("connector-worker: execution lease unavailable, leaving entry pending", map[string]any{"task_id": taskID, "error": err.Error()})
		return false
	}
	if !ok {
		wlog.Info("connector-worker: task is running under another lease, leaving entry pending", map[string]any{"task_id": taskID})
	}
	return ok
}

func report(ctx context.Context, d *deps, job dispatchJob, outcome dispatchOutcome, entryID string, wlog port.Logger) {
	var err error
	if outcome.ErrorClass != "" {
		err = d.completion.Fail(ctx, job.tenantID, job.taskID, outcome.ErrorClass, outcome.Output)
	} else {
		err = d.completion.Complete(ctx, job.tenantID, job.taskID, outcome.Output)
	}
	if err != nil {
		wlog.Warn("connector-worker: callback failed, leaving entry unacked for redelivery", map[string]any{"task_id": job.taskID, "error": err.Error()})
		return
	}
	ackEntry(ctx, d, entryID, wlog)
}
