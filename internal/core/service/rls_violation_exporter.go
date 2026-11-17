package service

import (
	"context"
	"fmt"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

const rlsViolationCommitLag = time.Minute

type RLSViolationExporterDeps struct {
	Log       port.RLSViolationLog
	Record    func(violationType string, n float64)
	Interval  time.Duration
	Retention time.Duration
	Logger    port.Logger
}

type RLSViolationExporter struct {
	deps     RLSViolationExporterDeps
	started  bool
	position time.Time
}

func NewRLSViolationExporter(deps RLSViolationExporterDeps) *RLSViolationExporter {
	return &RLSViolationExporter{deps: deps}
}

func (e *RLSViolationExporter) Start(ctx context.Context) error {
	now, err := e.deps.Log.Horizon(ctx, 0)
	if err != nil {
		return fmt.Errorf("rls violation log position: %w", err)
	}
	e.position = now
	e.started = true
	return nil
}

func (e *RLSViolationExporter) Tick(ctx context.Context) error {
	if err := e.countWindow(ctx); err != nil {
		return err
	}
	if _, err := e.deps.Log.PruneBefore(ctx, time.Now().Add(-e.deps.Retention)); err != nil {
		return fmt.Errorf("rls violation prune: %w", err)
	}
	return nil
}

func (e *RLSViolationExporter) countWindow(ctx context.Context) error {
	horizon, err := e.deps.Log.Horizon(ctx, rlsViolationCommitLag)
	if err != nil {
		return fmt.Errorf("rls violation counts: %w", err)
	}
	if !horizon.After(e.position) {
		return nil
	}
	counts, err := e.deps.Log.CountsBetween(ctx, e.position, horizon)
	if err != nil {
		return fmt.Errorf("rls violation counts: %w", err)
	}
	for _, c := range counts {
		e.deps.Record(c.ViolationType, float64(c.Count))
	}
	e.position = horizon
	return nil
}

func (e *RLSViolationExporter) Run(ctx context.Context) {
	ticker := time.NewTicker(e.deps.Interval)
	defer ticker.Stop()
	for {
		if !e.started {
			e.tryStart(ctx)
		} else if err := e.Tick(ctx); err != nil {
			e.deps.Logger.Error("rls violation export failed", map[string]any{"error": err.Error()})
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (e *RLSViolationExporter) tryStart(ctx context.Context) {
	if err := e.Start(ctx); err != nil {
		e.deps.Logger.Error("rls violation exporter not started, retrying", map[string]any{"error": err.Error()})
	}
}
