package main

import (
	"context"
	"fmt"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

const (
	auditPruneBatch    = 1000
	auditPruneInterval = 24 * time.Hour
	defaultResetPeriod = 5 * time.Minute

	streamTrimInterval = 15 * time.Minute
)

type streamTrimmer interface {
	TrimToOldestPending(ctx context.Context, streamKey, group string) (int64, error)
	TrimOlderThan(ctx context.Context, streamKey string, cutoff time.Time) (int64, error)
}

func trimStreams(ctx context.Context, d *deps, now time.Time, deadLetterRetention time.Duration, wlog port.Logger) {
	trimmer, ok := d.consumer.(streamTrimmer)
	if !ok {
		wlog.Warn("connector-worker: stream consumer cannot trim", nil)
		return
	}
	if _, err := trimmer.TrimToOldestPending(ctx, d.streamKey, d.streamGroup); err != nil {
		wlog.Warn("connector-worker: task stream trim failed", map[string]any{"stream": d.streamKey, "error": err.Error()})
	}
	if _, err := trimmer.TrimOlderThan(ctx, d.deadLetterKey, now.Add(-deadLetterRetention)); err != nil {
		wlog.Warn("connector-worker: dead-letter stream trim failed", map[string]any{"stream": d.deadLetterKey, "error": err.Error()})
	}
}

type attemptPruner interface {
	PruneAttempts(ctx context.Context, cutoff time.Time, batch int) (int64, error)
}

func pruneAttempts(ctx context.Context, store attemptPruner, now time.Time, retention time.Duration) error {
	cutoff := now.Add(-retention)
	for {
		deleted, err := store.PruneAttempts(ctx, cutoff, auditPruneBatch)
		if err != nil {
			return fmt.Errorf("prune connector document attempts: %w", err)
		}
		if deleted < auditPruneBatch {
			return nil
		}
	}
}

func resetProviderClients(d *deps) {
	for _, pool := range d.pools {
		if resettable, ok := pool.currentConnector().(interface{ ResetClients() }); ok {
			resettable.ResetClients()
		}
	}
}

func runMaintenanceLoop(ctx context.Context, d *deps, cfg *config.Config, wlog port.Logger) {
	resetPeriod := cfg.ConnectorAliasRefreshInterval
	if resetPeriod <= 0 {
		resetPeriod = defaultResetPeriod
	}
	reset := time.NewTicker(resetPeriod)
	defer reset.Stop()
	prune := time.NewTicker(auditPruneInterval)
	defer prune.Stop()
	trim := time.NewTicker(streamTrimInterval)
	defer trim.Stop()

	prunePastRetention(ctx, d, cfg, wlog)
	trimStreams(ctx, d, d.clock(), cfg.ConnectorStreamDeadLetterRetention, wlog)
	for {
		select {
		case <-ctx.Done():
			return
		case <-reset.C:
			resetProviderClients(d)
		case <-prune.C:
			prunePastRetention(ctx, d, cfg, wlog)
		case <-trim.C:
			trimStreams(ctx, d, d.clock(), cfg.ConnectorStreamDeadLetterRetention, wlog)
		}
	}
}

func prunePastRetention(ctx context.Context, d *deps, cfg *config.Config, wlog port.Logger) {
	if err := pruneAttempts(ctx, d.pruner, d.clock(), cfg.ConnectorAuditRetention); err != nil {
		wlog.Warn("connector-worker: audit pruning failed", map[string]any{"error": err.Error()})
	}
}
