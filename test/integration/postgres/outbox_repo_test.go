//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func instanceEventEnvelope(tenantID, instanceID uuid.UUID) events.Envelope[json.RawMessage] {
	payload, _ := json.Marshal(map[string]any{"workflow_instance_id": instanceID.String()})
	return events.NewEnvelope("workflow.task.created", "execution-service", json.RawMessage(payload), events.WithTenantID(tenantID.String()))
}

func taskEventEnvelope(tenantID, taskID uuid.UUID, eventType string) events.Envelope[json.RawMessage] {
	payload, _ := json.Marshal(map[string]any{"task_id": taskID.String()})
	return events.NewEnvelope(eventType, "execution-service", json.RawMessage(payload), events.WithTenantID(tenantID.String()))
}

func TestOutboxRepo_EnqueueRequiresTransaction(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	outboxRepo := postgres.NewOutboxRepo(appPool)
	ctx := context.Background()

	t.Run("outside a transaction, it errors", func(t *testing.T) {
		err := outboxRepo.Enqueue(ctx, instanceEventEnvelope(uuid.New(), uuid.New()))
		assert.Error(t, err)
	})

	t.Run("inside Transactor.RunInTx, it succeeds and the row is visible", func(t *testing.T) {
		transactor := postgres.NewTransactor(appPool)
		tenantID := uuid.New()
		instanceID := uuid.New()
		env := instanceEventEnvelope(tenantID, instanceID)

		require.NoError(t, transactor.RunInTx(withGUC(ctx, tenantID), func(ctx context.Context) error {
			return outboxRepo.Enqueue(ctx, env)
		}))

		var count int
		require.NoError(t, superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
			return conn.QueryRow(ctx, "SELECT count(*) FROM outbox_events WHERE id = $1", uuid.MustParse(env.ID)).Scan(&count)
		}))
		assert.Equal(t, 1, count)
	})
}

// TestOutboxRepo_ExistsForTask is the regression test for
// RecordSLAWarning/RecordSLABreach's own idempotency check (they mutate no
// status a retry could gate on, so they query the outbox directly instead).
func TestOutboxRepo_ExistsForTask(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	outboxRepo := postgres.NewOutboxRepo(appPool)
	transactor := postgres.NewTransactor(appPool)
	ctx := context.Background()

	tenantA := uuid.New()
	taskID := uuid.New()

	t.Run("false before anything is enqueued", func(t *testing.T) {
		exists, err := outboxRepo.ExistsForTask(ctx, "workflow.task.sla-warning", taskID)
		require.NoError(t, err)
		assert.False(t, exists)
	})

	env := taskEventEnvelope(tenantA, taskID, "workflow.task.sla-warning")
	require.NoError(t, transactor.RunInTx(withGUC(ctx, tenantA), func(ctx context.Context) error {
		return outboxRepo.Enqueue(ctx, env)
	}))

	t.Run("true for the exact type+task recorded", func(t *testing.T) {
		exists, err := outboxRepo.ExistsForTask(ctx, "workflow.task.sla-warning", taskID)
		require.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("false for a different event type on the same task", func(t *testing.T) {
		exists, err := outboxRepo.ExistsForTask(ctx, "workflow.task.sla-breached", taskID)
		require.NoError(t, err)
		assert.False(t, exists)
	})

	t.Run("false for the same event type on a different task", func(t *testing.T) {
		exists, err := outboxRepo.ExistsForTask(ctx, "workflow.task.sla-warning", uuid.New())
		require.NoError(t, err)
		assert.False(t, exists)
	})
}

func TestOutboxRepo_ListByInstance_KeysetPagination(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	outboxRepo := postgres.NewOutboxRepo(appPool)
	transactor := postgres.NewTransactor(appPool)
	ctx := context.Background()

	tenantA := uuid.New()
	instanceID := uuid.New()
	otherInstanceID := uuid.New()

	base := time.Now().UTC().Add(-time.Hour)
	var seeded []uuid.UUID
	for i := 0; i < 4; i++ {
		env := instanceEventEnvelope(tenantA, instanceID)
		require.NoError(t, transactor.RunInTx(withGUC(ctx, tenantA), func(ctx context.Context) error {
			return outboxRepo.Enqueue(ctx, env)
		}))
		require.NoError(t, superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
			_, err := conn.Exec(ctx, "UPDATE outbox_events SET created_at = $2 WHERE id = $1",
				uuid.MustParse(env.ID), base.Add(time.Duration(i)*time.Minute))
			return err
		}))
		seeded = append(seeded, uuid.MustParse(env.ID))
	}

	// A row for a different instance must never leak into this instance's timeline.
	other := instanceEventEnvelope(tenantA, otherInstanceID)
	require.NoError(t, transactor.RunInTx(withGUC(ctx, tenantA), func(ctx context.Context) error {
		return outboxRepo.Enqueue(ctx, other)
	}))

	var collected []uuid.UUID
	var cursor *port.Cursor
	for {
		page, next, err := outboxRepo.ListByInstance(ctx, tenantA, instanceID, port.PageRequest{After: cursor, Limit: 2})
		require.NoError(t, err)
		for _, rec := range page {
			collected = append(collected, rec.ID)
		}
		if next == nil {
			break
		}
		cursor = next
	}

	require.Len(t, collected, 4, fmt.Sprintf("expected exactly the 4 seeded events for %s, got %v", instanceID, collected))
	for i, id := range collected {
		assert.Equal(t, seeded[3-i], id, "page order mismatch at position %d", i)
	}
}
