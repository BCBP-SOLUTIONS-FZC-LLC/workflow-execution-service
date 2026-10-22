package main

import (
	"context"
	"sync"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type inflightSet struct {
	mu        sync.Mutex
	deadlines map[string]time.Time
}

func (s *inflightSet) claim(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, held := s.deadlines[id]; held {
		return false
	}
	if s.deadlines == nil {
		s.deadlines = make(map[string]time.Time)
	}
	s.deadlines[id] = time.Time{}
	return true
}

func (s *inflightSet) setDeadline(id string, deadline time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deadlines[id] = deadline
}

func (s *inflightSet) remove(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.deadlines, id)
}

func (s *inflightSet) live(now time.Time) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.deadlines))
	for id, deadline := range s.deadlines {
		if deadline.IsZero() || now.Before(deadline) {
			ids = append(ids, id)
		}
	}
	return ids
}

func heartbeatLoop(ctx context.Context, d *deps, wlog port.Logger) {
	ticker := time.NewTicker(d.claimMinIdle / 3)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			heartbeatOnce(ctx, d, wlog)
		}
	}
}

func heartbeatOnce(ctx context.Context, d *deps, wlog port.Logger) {
	ids := d.inflight.live(d.clock())
	if len(ids) == 0 {
		return
	}
	if err := d.consumer.Touch(ctx, d.streamKey, d.streamGroup, d.consumerName, ids); err != nil {
		wlog.Warn("connector-worker: heartbeat failed, in-flight entries may be reclaimed", map[string]any{"entries": len(ids), "error": err.Error()})
	}
}

const drainMargin = 10 * time.Second

func shutdownDrain(cfg *config.Config) time.Duration {
	longest := max(
		cfg.ConnectorTimeoutStorage, cfg.ConnectorTimeoutSendEmail, cfg.ConnectorTimeoutDocumentExtract,
		cfg.ConnectorTimeoutRestCall, cfg.ConnectorTimeoutSQLQuery, cfg.ConnectorTimeoutChatNotify,
	)
	return longest + cfg.ConnectorCompletionTimeout + drainMargin
}
