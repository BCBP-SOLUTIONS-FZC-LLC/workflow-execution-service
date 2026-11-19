//go:build integration

package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

// w29aHoldConnection keeps one connection of the pool busy until release is
// called, and returns once the connection is held.
func w29aHoldConnection(t *testing.T, pool *pgcommon.Pool, release <-chan struct{}, finished *sync.WaitGroup) {
	t.Helper()
	held := make(chan struct{})
	finished.Add(1)
	go func() {
		defer finished.Done()
		_ = pool.WithConn(context.Background(), func(_ context.Context, _ *pgxpool.Conn) error {
			close(held)
			<-release
			return nil
		})
	}()
	select {
	case <-held:
	case <-time.After(w9aWait):
		t.Fatal("the pool never handed out its connection")
	}
}

// One context bounds the whole stop. Once it has run out, the first step to
// need time (here, three pools with a busy connection each) reports it, and
// no step after a timed-out one is skipped.
func TestStopServers_OneExpiredDeadlineReachesEveryPoolDrainAndTheLaterStepsStillRun(t *testing.T) {
	_, pgDSN := fixtures.NewTestPoolAndDSN(t)
	a, log, order := w9aShutdownApp(t, pgDSN, "w29a_shutdown_deadline", nil)
	release := make(chan struct{})
	var finished sync.WaitGroup
	t.Cleanup(func() {
		close(release)
		finished.Wait()
	})
	for _, pool := range []*pgcommon.Pool{a.appPool, a.relayPool, a.lockPool} {
		w29aHoldConnection(t, pool, release, &finished)
	}
	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	a.stopServers(expired)

	var warned []string
	for _, r := range log.at("warn") {
		warned = append(warned, r.msg)
		assert.NotEmpty(t, r.fields["error"], "each warning carries the drain error")
	}
	assert.Equal(t, []string{
		"background loops still running at the shutdown deadline",
		"app db pool drain timed out",
		"relay db pool drain timed out",
		"lock db pool drain timed out",
	}, warned, "the loop wait and every pool drain ran under the same, already expired, deadline and in this order")
	assert.Empty(t, log.at("error"), "the idle HTTP and metrics servers and the relay stop cleanly")
	require.Equal(t, []string{"valkey close", "temporal close", "tracing shutdown"}, order.all(), "the steps after the timed-out drains still ran, in order")
}
