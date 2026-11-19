package service_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

type rlsLeaderLock struct {
	mu       sync.Mutex
	acquired bool
	keys     []string
}

func (l *rlsLeaderLock) TryWithLock(ctx context.Context, key string, fn func(context.Context) error) (bool, error) {
	l.mu.Lock()
	l.keys = append(l.keys, key)
	l.mu.Unlock()
	if !l.acquired {
		return false, nil
	}
	return true, fn(ctx)
}

func (l *rlsLeaderLock) tried() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.keys...)
}

func runRLSExporterUntilHorizonCalls(t *testing.T, leader *rlsLeaderLock, calls int) (*w16bViolationLog, []w16bRecorded) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	base := time.Unix(1_000, 0)
	log := &w16bViolationLog{
		script: []w16bHorizonResult{{at: base}, {at: base.Add(time.Hour)}},
		counts: []domain.RLSViolationCount{{ViolationType: "cross_tenant_access", Count: 3}},
		onCall: func(call int) {
			if call >= calls {
				cancel()
			}
		},
	}
	var mu sync.Mutex
	var recorded []w16bRecorded
	exp := service.NewRLSViolationExporter(service.RLSViolationExporterDeps{
		Log: log,
		Record: func(typ string, n float64) {
			mu.Lock()
			defer mu.Unlock()
			recorded = append(recorded, w16bRecorded{typ, n})
		},
		Leader:    leader,
		Interval:  time.Millisecond,
		Retention: time.Hour,
		Logger:    &w16bLogger{},
	})

	done := make(chan struct{})
	go func() {
		exp.Run(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		cancel()
		t.Fatal("Run did not return after its context was cancelled")
	}
	mu.Lock()
	defer mu.Unlock()
	return log, append([]w16bRecorded(nil), recorded...)
}

func TestRLSViolationExporter_ReplicaThatDoesNotLeadNeitherCountsNorPrunes(t *testing.T) {
	leader := &rlsLeaderLock{acquired: false}

	log, recorded := runRLSExporterUntilHorizonCalls(t, leader, 4)

	_, windows, pruned := log.snapshot()
	assert.Empty(t, windows)
	assert.Empty(t, pruned)
	assert.Empty(t, recorded)
	assert.NotEmpty(t, leader.tried())
}

func TestRLSViolationExporter_ReplicaThatLeadsCountsAndPrunesUnderTheExporterLock(t *testing.T) {
	leader := &rlsLeaderLock{acquired: true}

	log, recorded := runRLSExporterUntilHorizonCalls(t, leader, 3)

	_, windows, pruned := log.snapshot()
	require.NotEmpty(t, windows)
	assert.NotEmpty(t, pruned)
	assert.Equal(t, []w16bRecorded{{"cross_tenant_access", 3}}, recorded[:1])
	assert.Equal(t, []string{port.RLSViolationExporterScope}, leader.tried())
}
