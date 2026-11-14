package main

import (
	"context"
	"strings"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

const readFailureBackoff = time.Second

type groupEnsurer interface {
	EnsureGroup(ctx context.Context, streamKey, group string) error
}

// recreateLostGroup handles a read that failed because the stream or its consumer group no longer exists (a Valkey
// flush): it recreates the group so reading resumes without a restart (§S6.5 step 0). Entries lost with the stream
// are not recovered; their tasks stay open. It reports whether the group is back.
func recreateLostGroup(ctx context.Context, d *deps, readErr error, wlog port.Logger) bool {
	ensurer, ok := d.consumer.(groupEnsurer)
	if !ok || !strings.Contains(readErr.Error(), "NOGROUP") {
		return false
	}
	if err := ensurer.EnsureGroup(ctx, d.streamKey, d.streamGroup); err != nil {
		wlog.Error("connector-worker: consumer group lost and not recreated", map[string]any{"stream": d.streamKey, "group": d.streamGroup, "error": err.Error()})
		return false
	}
	wlog.Warn("connector-worker: consumer group was lost and has been recreated; tasks whose entries were lost with the stream stay open", map[string]any{"stream": d.streamKey, "group": d.streamGroup})
	return true
}

// waitReadBackoff pauses after a failed read and ends at once on shutdown.
func waitReadBackoff(ctx context.Context) {
	timer := time.NewTimer(readFailureBackoff)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
	}
}
