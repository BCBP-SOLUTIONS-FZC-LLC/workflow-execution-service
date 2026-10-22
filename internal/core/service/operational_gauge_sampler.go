package service

import (
	"context"
	"fmt"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type OperationalGaugeSamplerDeps struct {
	Stats    port.OperationalStatsReader
	Record   func(domain.OperationalStats)
	Interval time.Duration
	Logger   port.Logger
}

type OperationalGaugeSampler struct {
	deps OperationalGaugeSamplerDeps
}

func NewOperationalGaugeSampler(deps OperationalGaugeSamplerDeps) *OperationalGaugeSampler {
	return &OperationalGaugeSampler{deps: deps}
}

func (s *OperationalGaugeSampler) Tick(ctx context.Context) error {
	stats, err := s.deps.Stats.Read(ctx)
	if err != nil {
		return fmt.Errorf("operational stats: %w", err)
	}
	s.deps.Record(stats)
	return nil
}

func (s *OperationalGaugeSampler) Run(ctx context.Context) {
	ticker := time.NewTicker(s.deps.Interval)
	defer ticker.Stop()
	for {
		if err := s.Tick(ctx); err != nil {
			s.deps.Logger.Error("operational gauge sampling failed", map[string]any{"error": err.Error()})
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
