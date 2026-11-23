//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	eventsmock "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events/mock"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/outbox"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/signalrelay"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func signalFor(tenantID, instanceID uuid.UUID, name, payload string) port.WorkflowSignal {
	return port.WorkflowSignal{
		TenantID: tenantID, InstanceID: instanceID, TemporalWorkflowID: "wf-" + instanceID.String(),
		Name: name, Payload: json.RawMessage(payload),
	}
}

func TestOutboxRepo_EnqueueSignal_RequiresATransaction(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	outboxRepo := postgres.NewOutboxRepo(newAppRolePool(t, superPool, superDSN))

	err := outboxRepo.EnqueueSignal(context.Background(), signalFor(uuid.New(), uuid.New(), port.SignalInstanceReassign, `{}`))

	assert.Error(t, err)
}

func TestOutboxRepo_EnqueueSignal_IsCommittedOrRolledBackWithTheTransaction(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	outboxRepo := postgres.NewOutboxRepo(appPool)
	transactor := postgres.NewTransactor(appPool)
	tenantID, committed, rolledBack := uuid.New(), uuid.New(), uuid.New()
	ctx := withGUC(context.Background(), tenantID)

	require.NoError(t, transactor.RunInTx(ctx, func(ctx context.Context) error {
		return outboxRepo.EnqueueSignal(ctx, signalFor(tenantID, committed, port.SignalInstanceReassign, `{"TaskID":"t-1"}`))
	}))
	require.Error(t, transactor.RunInTx(ctx, func(ctx context.Context) error {
		if err := outboxRepo.EnqueueSignal(ctx, signalFor(tenantID, rolledBack, port.SignalInstanceReassign, `{}`)); err != nil {
			return err
		}
		return errors.New("the move failed after the signal was recorded")
	}))

	rows := map[string]string{}
	require.NoError(t, superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		r, err := conn.Query(ctx, "SELECT payload -> 'data' ->> 'target_instance_id', tenant_id FROM outbox_events WHERE event_type = $1", port.WorkflowSignalEventType)
		if err != nil {
			return err
		}
		defer r.Close()
		for r.Next() {
			var instance, tenant string
			if err := r.Scan(&instance, &tenant); err != nil {
				return err
			}
			rows[instance] = tenant
		}
		return r.Err()
	}))
	assert.Equal(t, map[string]string{committed.String(): tenantID.String()}, rows, "only the committed transaction's signal is recorded, under its tenant")

	listed, _, err := outboxRepo.ListByInstance(ctx, tenantID, committed, port.PageRequest{Limit: 10})
	require.NoError(t, err)
	assert.Empty(t, listed, "a signal record never shows up in the instance's event history")
}

type relaySignalTemporal struct {
	port.TemporalClient
	mu        sync.Mutex
	attempts  int
	failUntil int
	sent      []string
}

func (r *relaySignalTemporal) SignalWorkflow(_ context.Context, _ string, _ uuid.UUID, name string, payload any) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.attempts++
	if r.attempts <= r.failUntil {
		return errors.New("temporal unavailable")
	}
	raw, _ := json.Marshal(payload)
	r.sent = append(r.sent, name+string(raw))
	return nil
}

func (r *relaySignalTemporal) delivered() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.sent...)
}

func TestSignalRelay_AFailedDeliveryIsRetriedAndEachSignalArrivesOnce(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	relayPool := newOutboxRelayRolePool(t, superPool, superDSN)
	temporal := &relaySignalTemporal{failUntil: 2}
	runner, err := outbox.NewRunner(outbox.Config{
		Pool: relayPool, Publisher: signalrelay.New(&eventsmock.Publisher{}, temporal, nil),
		PollInterval: 20 * time.Millisecond, BatchSize: 10, Logger: testLogger{t},
		RetryBackoff: 20 * time.Millisecond, MaxRetryBackoff: 50 * time.Millisecond,
	})
	require.NoError(t, err)

	tenantID, instanceID := uuid.New(), uuid.New()
	outboxRepo := postgres.NewOutboxRepo(appPool)
	require.NoError(t, postgres.NewTransactor(appPool).RunInTx(withGUC(context.Background(), tenantID), func(ctx context.Context) error {
		if err := outboxRepo.EnqueueSignal(ctx, signalFor(tenantID, instanceID, port.SignalInstanceReassign, `{"TaskID":"first"}`)); err != nil {
			return err
		}
		return outboxRepo.EnqueueSignal(ctx, signalFor(tenantID, instanceID, port.SignalInstanceTaskVacated, `{"TaskID":"second"}`))
	}))

	done := make(chan error, 1)
	go func() { done <- runner.Start(context.Background()) }()
	t.Cleanup(func() {
		require.NoError(t, runner.Stop())
		<-done
	})

	require.Eventually(t, func() bool { return len(temporal.delivered()) == 2 }, 30*time.Second, 20*time.Millisecond,
		"both signals are delivered once Temporal answers again")
	assert.ElementsMatch(t, []string{
		port.SignalInstanceReassign + `{"TaskID":"first"}`,
		port.SignalInstanceTaskVacated + `{"TaskID":"second"}`,
	}, temporal.delivered(), "each signal arrives once, although Temporal refused the first attempts")

	require.Eventually(t, func() bool {
		var pending int
		require.NoError(t, superPool.WithConn(context.Background(), func(ctx context.Context, conn *pgxpool.Conn) error {
			return conn.QueryRow(ctx, "SELECT count(*) FROM outbox_events WHERE event_type = $1 AND published_at IS NULL", port.WorkflowSignalEventType).Scan(&pending)
		}))
		return pending == 0
	}, 30*time.Second, 20*time.Millisecond, "a delivered signal is marked published, so it is not sent again")
	assert.Len(t, temporal.delivered(), 2)
}
