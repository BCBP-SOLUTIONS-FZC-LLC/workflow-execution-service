package service

import (
	"context"
	"fmt"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type RLSViolationExporterDeps struct {
	Log       port.RLSViolationLog
	Record    func(violationType string, n float64)
	Interval  time.Duration
	Retention time.Duration
	Logger    port.Logger
}

type RLSViolationExporter struct {
	deps   RLSViolationExporterDeps
	lastID int64
}

func NewRLSViolationExporter(deps RLSViolationExporterDeps) *RLSViolationExporter {
	return &RLSViolationExporter{deps: deps}
}

func (e *RLSViolationExporter) Start(ctx context.Context) error {
	id, err := e.deps.Log.LatestID(ctx)
	if err != nil {
		return fmt.Errorf("rls violation log position: %w", err)
	}
	e.lastID = id
	return nil
}

func (e *RLSViolationExporter) Tick(ctx context.Context) error {
	counts, newest, err := e.deps.Log.CountsAfter(ctx, e.lastID)
	if err != nil {
		return fmt.Errorf("rls violation counts: %w", err)
	}
	for _, c := range counts {
		e.deps.Record(c.ViolationType, float64(c.Count))
	}
	e.lastID = newest

	if _, err := e.deps.Log.PruneBefore(ctx, time.Now().Add(-e.deps.Retention)); err != nil {
		return fmt.Errorf("rls violation prune: %w", err)
	}
	return nil
}

func (e *RLSViolationExporter) Run(ctx context.Context) {
	if err := e.Start(ctx); err != nil {
		e.deps.Logger.Error("rls violation exporter not started", map[string]any{"error": err.Error()})
		return
	}
	ticker := time.NewTicker(e.deps.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := e.Tick(ctx); err != nil {
				e.deps.Logger.Error("rls violation export failed", map[string]any{"error": err.Error()})
			}
		}
	}
}
