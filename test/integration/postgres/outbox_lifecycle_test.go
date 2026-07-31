//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	eventsmock "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events/mock"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/outbox"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

// testLogger structurally satisfies platform-events' internal port.Logger
// (Debug/Info/Warn/Error(string, map[string]interface{})) without importing
// it — Go interface satisfaction doesn't require naming the interface type.
type testLogger struct{ t *testing.T }

func (l testLogger) Debug(msg string, fields map[string]interface{}) {
	l.t.Logf("DEBUG %s %v", msg, fields)
}
func (l testLogger) Info(msg string, fields map[string]interface{}) {
	l.t.Logf("INFO %s %v", msg, fields)
}
func (l testLogger) Warn(msg string, fields map[string]interface{}) {
	l.t.Logf("WARN %s %v", msg, fields)
}
func (l testLogger) Error(msg string, fields map[string]interface{}) {
	l.t.Logf("ERROR %s %v", msg, fields)
}

// TestOutboxRelayLifecycle proves the two-pool + outbox-relay wiring this
// task's DoD calls for, without committing to cmd/server's eventual real
// bootstrap shape (see plan: other Tier-1 branches land cmd/server changes
// in parallel). App pool and relay pool are two separate pgcommon.Pool
// instances, matching LLD §9.2/§9.7's connection-role isolation.
func TestOutboxRelayLifecycle(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	relayPool := newOutboxRelayRolePool(t, superPool, superDSN)

	publisher := &eventsmock.Publisher{}
	runner, err := outbox.NewRunner(outbox.Config{
		Pool:         relayPool,
		Publisher:    publisher,
		PollInterval: 50 * time.Millisecond,
		BatchSize:    50,
		Logger:       testLogger{t},
	})
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- runner.Start(context.Background()) }()

	// runner.Ready() must be re-fetched each attempt: Start reassigns its
	// backing channel under its own lock shortly after launch, so caching one
	// Ready() call made immediately after `go runner.Start(...)` can race and
	// wait on a channel Start never closes.
	require.Eventually(t, func() bool {
		select {
		case <-runner.Ready():
			return true
		default:
			return false
		}
	}, 5*time.Second, 20*time.Millisecond, "outbox relay never became ready")

	tenantID := uuid.New()
	transactor := postgres.NewTransactor(appPool)
	outboxRepo := postgres.NewOutboxRepo(appPool)
	env := instanceEventEnvelope(tenantID, uuid.New())
	require.NoError(t, transactor.RunInTx(withGUC(context.Background(), tenantID), func(ctx context.Context) error {
		return outboxRepo.Enqueue(ctx, env)
	}))

	require.Eventually(t, func() bool {
		for _, published := range publisher.Published() {
			if published.ID == env.ID {
				return true
			}
		}
		return false
	}, 5*time.Second, 50*time.Millisecond, "enqueued event was never published")

	require.NoError(t, runner.Stop())
	require.NoError(t, <-done)

	require.NoError(t, appPool.DrainAndClose(context.Background()))
	require.NoError(t, relayPool.DrainAndClose(context.Background()))
}
