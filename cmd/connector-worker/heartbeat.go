package main

import (
	"context"
	"sync"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// inflightSet holds the stream entries this consumer has read and not yet
// finished with, each with the deadline of its run's lease once it has one.
// Its zero value is ready to use.
type inflightSet struct {
	mu        sync.Mutex
	deadlines map[string]time.Time
}

// claim reports whether id was not already in flight here. A second copy of
// an entry this consumer holds is left to the first.
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

// live lists the entries still worth keeping: queued, or running within their
// lease. A run past its lease is treated as hung and left to be reclaimed.
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

// heartbeatLoop keeps this consumer's entries from looking idle while they
// wait for a pool slot or run, so the reclaim wait detects a dead consumer
// and nothing else (workflow_connectors.md §6.5 step 0).
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

// shutdownDrain is how long shutdown waits for started runs: the slowest
// connector type's timeout, its callback, and a margin.
func shutdownDrain(cfg *config.Config) time.Duration {
	longest := max(
		cfg.ConnectorTimeoutStorage, cfg.ConnectorTimeoutSendEmail, cfg.ConnectorTimeoutDocumentExtract,
		cfg.ConnectorTimeoutRestCall, cfg.ConnectorTimeoutSQLQuery, cfg.ConnectorTimeoutChatNotify,
	)
	return longest + cfg.ConnectorCompletionTimeout + executionLeaseMargin
}
