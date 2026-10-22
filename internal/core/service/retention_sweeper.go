package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

const (
	retentionTableOutbox         = "outbox_events"
	retentionTableProcessedEvent = "processed_event"
)

type OutboxPruner interface {
	PrunePublished(ctx context.Context, olderThan time.Duration, limit int) (int64, error)
}

type ProcessedEventPruner interface {
	PruneOlderThan(ctx context.Context, olderThan time.Duration) (int64, error)
}

type RetentionSweeperDeps struct {
	Outbox                  OutboxPruner
	ProcessedEvents         ProcessedEventPruner
	OutboxRetention         time.Duration
	ProcessedEventRetention time.Duration
	Batch                   int
	Interval                time.Duration
	Record                  func(table string, deleted int64)
	Logger                  port.Logger
}

type RetentionSweeper struct {
	deps RetentionSweeperDeps
}

func NewRetentionSweeper(deps RetentionSweeperDeps) *RetentionSweeper {
	return &RetentionSweeper{deps: deps}
}

func (s *RetentionSweeper) Tick(ctx context.Context) error {
	return errors.Join(s.pruneOutbox(ctx), s.pruneProcessedEvents(ctx))
}

func (s *RetentionSweeper) pruneOutbox(ctx context.Context) error {
	for {
		deleted, err := s.deps.Outbox.PrunePublished(ctx, s.deps.OutboxRetention, s.deps.Batch)
		if err != nil {
			return fmt.Errorf("prune published outbox events: %w", err)
		}
		s.deps.Record(retentionTableOutbox, deleted)
		if deleted < int64(s.deps.Batch) {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("prune published outbox events: %w", err)
		}
	}
}

func (s *RetentionSweeper) pruneProcessedEvents(ctx context.Context) error {
	deleted, err := s.deps.ProcessedEvents.PruneOlderThan(ctx, s.deps.ProcessedEventRetention)
	if err != nil {
		return fmt.Errorf("prune processed events: %w", err)
	}
	s.deps.Record(retentionTableProcessedEvent, deleted)
	return nil
}

func (s *RetentionSweeper) Run(ctx context.Context) {
	ticker := time.NewTicker(s.deps.Interval)
	defer ticker.Stop()
	for {
		if err := s.Tick(ctx); err != nil {
			s.deps.Logger.Error("retention sweep failed", map[string]any{"error": err.Error()})
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
