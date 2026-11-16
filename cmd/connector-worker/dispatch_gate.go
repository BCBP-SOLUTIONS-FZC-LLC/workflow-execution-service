package main

import (
	"context"

	pgdomain "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// stillRuns decides whether the connector may run for the job: the task must still be open on an instance that
// has not ended, because a side effect such as an email cannot be undone by the callback refusing the result
// (execution LLD §6.2). A task that is no longer open is acknowledged and skipped (no run, no callback); a task
// whose state cannot be read is left pending, so a redelivery asks again.
func stillRuns(ctx context.Context, d *deps, job dispatchJob, entryID string, wlog port.Logger) bool {
	if d.gate == nil {
		return true
	}
	gateCtx := pgcommon.WithGUCSet(ctx, pgdomain.GUCSet{TenantID: job.tenantID.String()})
	dispatchable, err := d.gate.Dispatchable(gateCtx, job.tenantID, job.taskID)
	if err != nil {
		wlog.Warn("connector-worker: task state unreadable, leaving entry pending", map[string]any{"task_id": job.taskID, "tenant_id": job.tenantID, "error": err.Error()})
		return false
	}
	if dispatchable {
		return true
	}
	wlog.Info("connector-worker: task is finished or its instance has ended, skipping the connector", map[string]any{"task_id": job.taskID, "tenant_id": job.tenantID})
	ackEntry(ctx, d, entryID, wlog)
	return false
}
