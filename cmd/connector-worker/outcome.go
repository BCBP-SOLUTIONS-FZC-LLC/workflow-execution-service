package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
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

// report sends the outcome to the server and acks the entry on success; a failed callback leaves the entry
// pending and is returned.
func report(ctx context.Context, d *deps, job dispatchJob, outcome dispatchOutcome, entryID string, wlog port.Logger) error {
	var err error
	if outcome.ErrorClass != "" {
		err = d.completion.Fail(ctx, job.tenantID, job.taskID, outcome.ErrorClass, outcome.Output)
	} else {
		err = d.completion.Complete(ctx, job.tenantID, job.taskID, outcome.Output)
	}
	if err != nil {
		fields := map[string]any{"task_id": job.taskID, "error": err.Error()}
		var statusErr *callbackStatusError
		if errors.As(err, &statusErr) {
			fields["status"], fields["body"] = statusErr.Status, statusErr.Body
		}
		wlog.Warn("connector-worker: callback failed, leaving entry unacked for redelivery", fields)
		return err
	}
	ackEntry(ctx, d, entryID, wlog)
	return nil
}

// refusedForGood reports a callback the server answered with a 4xx that a retry cannot change (not a timeout
// or throttling answer).
func refusedForGood(err error) bool {
	var statusErr *callbackStatusError
	if !errors.As(err, &statusErr) {
		return false
	}
	return statusErr.Status >= 400 && statusErr.Status < 500 &&
		statusErr.Status != http.StatusRequestTimeout && statusErr.Status != http.StatusTooManyRequests
}
