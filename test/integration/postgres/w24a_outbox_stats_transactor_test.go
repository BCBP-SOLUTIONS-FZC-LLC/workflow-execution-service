//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestTransactor_RunInTxWithRetry_ASerializationFailure_IsRetriedUpToFiveAttempts(t *testing.T) {
	w := w24aNewWorld(t)
	tenantID := uuid.New()
	ctx := withGUC(context.Background(), tenantID)
	serializationFailure := &pgconn.PgError{Code: "40001", Message: "could not serialize access"}

	t.Run("a transaction that conflicts twice commits on the third attempt and keeps one copy of its write", func(t *testing.T) {
		inst := newInstance(tenantID, time.Now().UTC())
		attempts := 0

		err := w.transactor.RunInTxWithRetry(ctx, func(txCtx context.Context) error {
			attempts++
			if err := w.instances.Create(txCtx, inst); err != nil {
				return err
			}
			if attempts < 3 {
				return serializationFailure
			}
			return nil
		})

		require.NoError(t, err)
		assert.Equal(t, 3, attempts)
		got, getErr := w.instances.GetByID(context.Background(), tenantID, inst.ID)
		require.NoError(t, getErr)
		assert.Equal(t, inst.BusinessKey, got.BusinessKey)
	})

	t.Run("a transaction that conflicts every time gives up after five attempts and commits nothing", func(t *testing.T) {
		inst := newInstance(tenantID, time.Now().UTC())
		attempts := 0

		err := w.transactor.RunInTxWithRetry(ctx, func(txCtx context.Context) error {
			attempts++
			if err := w.instances.Create(txCtx, inst); err != nil {
				return err
			}
			return serializationFailure
		})

		require.Error(t, err)
		assert.True(t, pgcommon.IsSerializationFailure(err), "the last conflict stays reachable: %v", err)
		assert.ErrorContains(t, err, "after 5 attempts")
		assert.Equal(t, 5, attempts)
		_, getErr := w.instances.GetByID(context.Background(), tenantID, inst.ID)
		assert.ErrorIs(t, getErr, domain.ErrNotFound, "every attempt rolled back")
	})

	t.Run("RunInTx does not retry the same failure", func(t *testing.T) {
		attempts := 0

		err := w.transactor.RunInTx(ctx, func(context.Context) error {
			attempts++
			return serializationFailure
		})

		require.Error(t, err)
		assert.Equal(t, 1, attempts)
	})
}

func TestOutboxRepo_EnqueueSignal_RecordsTheSignalWithItsTargetNamePayloadSubjectAndTenant(t *testing.T) {
	w := w24aNewWorld(t)
	outboxRepo := postgres.NewOutboxRepo(w.appPool)
	tenantID, instanceID := uuid.New(), uuid.New()
	ctx := withGUC(context.Background(), tenantID)
	payload := `{"TaskID":"t-9","note":"x"}`
	sig := port.WorkflowSignal{
		TenantID: tenantID, InstanceID: instanceID, TemporalWorkflowID: "wf-custom-id",
		Name: port.SignalInstanceReassign, Payload: json.RawMessage(payload),
	}

	require.NoError(t, w.transactor.RunInTx(ctx, func(txCtx context.Context) error {
		return outboxRepo.EnqueueSignal(txCtx, sig)
	}))

	type stored struct {
		eventType, tenant, subject string
		data                       []byte
		count                      int
	}
	var got stored
	require.NoError(t, w.superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		if err := conn.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE tenant_id = $1`, tenantID.String()).Scan(&got.count); err != nil {
			return err
		}
		return conn.QueryRow(ctx,
			`SELECT event_type, tenant_id, payload ->> 'subject', (payload -> 'data')::text
			 FROM outbox_events WHERE tenant_id = $1`, tenantID.String()).Scan(&got.eventType, &got.tenant, &got.subject, &got.data)
	}))
	assert.Equal(t, 1, got.count, "one record per signal")
	assert.Equal(t, "WorkflowSignalPending", got.eventType)
	assert.Equal(t, tenantID.String(), got.tenant)
	assert.Equal(t, "instances/"+instanceID.String(), got.subject)
	var wire map[string]any
	require.NoError(t, json.Unmarshal(got.data, &wire))
	for _, key := range []string{"target_instance_id", "temporal_workflow_id", "signal_name", "signal_payload"} {
		assert.Contains(t, wire, key, "the relay reads this key from the stored record")
	}
	var msg port.WorkflowSignalMessage
	require.NoError(t, json.Unmarshal(got.data, &msg))
	assert.Equal(t, instanceID.String(), msg.TargetInstanceID)
	assert.Equal(t, "wf-custom-id", msg.TemporalWorkflowID)
	assert.Equal(t, port.SignalInstanceReassign, msg.SignalName)
	assert.JSONEq(t, payload, string(msg.SignalPayload))
}

func TestOutboxRepo_ExistsByID_FindsTheEnqueuedIdAndReturnsAFailingQueryUnmapped(t *testing.T) {
	w := w24aNewWorld(t)
	outboxRepo := postgres.NewOutboxRepo(w.appPool)
	tenantID := uuid.New()
	env := instanceEventEnvelope(tenantID, uuid.New())
	eventID := uuid.MustParse(env.ID)
	require.NoError(t, w.transactor.RunInTx(withGUC(context.Background(), tenantID), func(ctx context.Context) error {
		return outboxRepo.Enqueue(ctx, env)
	}))

	t.Run("the enqueued id is found and one never enqueued is not", func(t *testing.T) {
		found, err := outboxRepo.ExistsByID(context.Background(), tenantID, eventID)
		require.NoError(t, err)
		assert.True(t, found)

		found, err = outboxRepo.ExistsByID(context.Background(), tenantID, uuid.New())
		require.NoError(t, err)
		assert.False(t, found)
	})

	t.Run("a query that fails comes back unmapped", func(t *testing.T) {
		err := w.withCancelledTx(tenantID, func(ctx context.Context) error {
			_, existsErr := outboxRepo.ExistsByID(ctx, tenantID, eventID)
			return existsErr
		})

		require.ErrorIs(t, err, context.Canceled)
		w24aRequireNoDomainAnswer(t, err)
	})
}

func TestRLSViolationRepo_CountsBetween_TheStartIsExclusiveTheHorizonInclusiveAndPruneKeepsTheCutoff(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	relayPool := newOutboxRelayRolePool(t, superPool, superDSN)
	repo := postgres.NewRLSViolationRepo(relayPool)
	ctx := context.Background()
	base := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	insert := func(violationType string, at time.Time) {
		t.Helper()
		require.NoError(t, superPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
			_, err := conn.Exec(ctx,
				`INSERT INTO rls_violation_log (violation_type, row_tenant_id, table_name, occurred_at)
				 VALUES ($1, $2, 'workflow_instance', $3)`, violationType, uuid.New(), at)
			return err
		}))
	}
	insert("at_start", base)
	insert("inside", base.Add(time.Second))
	insert("inside", base.Add(time.Second+time.Millisecond))
	insert("at_horizon", base.Add(2*time.Second))
	insert("after_horizon", base.Add(2*time.Second+time.Millisecond))

	t.Run("a row at the start is left out and a row at the horizon is counted", func(t *testing.T) {
		counts, err := repo.CountsBetween(ctx, base, base.Add(2*time.Second))

		require.NoError(t, err)
		got := map[string]int64{}
		for _, c := range counts {
			got[c.ViolationType] = c.Count
		}
		assert.Equal(t, map[string]int64{"inside": 2, "at_horizon": 1}, got,
			"rows from every tenant are counted, grouped by violation type")
	})

	t.Run("a window with nothing in it counts nothing", func(t *testing.T) {
		counts, err := repo.CountsBetween(ctx, base.Add(time.Hour), base.Add(2*time.Hour))

		require.NoError(t, err)
		assert.Empty(t, counts)
	})

	t.Run("the horizon is the database clock minus the lag", func(t *testing.T) {
		noLag, err := repo.Horizon(ctx, 0)
		require.NoError(t, err)
		lagged, err := repo.Horizon(ctx, time.Hour)
		require.NoError(t, err)

		gap := noLag.Sub(lagged)
		assert.Greater(t, gap, 59*time.Minute)
		assert.Less(t, gap, 61*time.Minute)
	})

	t.Run("pruning deletes only rows before the cutoff", func(t *testing.T) {
		pruned, err := repo.PruneBefore(ctx, base.Add(time.Second))

		require.NoError(t, err)
		assert.EqualValues(t, 1, pruned, "only the row before the cutoff goes")
		counts, err := repo.CountsBetween(ctx, base.Add(-time.Second), base.Add(time.Hour))
		require.NoError(t, err)
		got := map[string]int64{}
		for _, c := range counts {
			got[c.ViolationType] = c.Count
		}
		assert.Equal(t, map[string]int64{"inside": 2, "at_horizon": 1, "after_horizon": 1}, got,
			"the row exactly at the cutoff is kept")
	})
}

func TestUserAvailabilityRepo_Record_WhenTheWriteFails_ReturnsTheErrorUnmappedAndStoresNothing(t *testing.T) {
	w := w24aNewWorld(t)
	repo := postgres.NewUserAvailabilityRepo(w.appPool)
	tenantID, userID := uuid.New(), uuid.New()
	ctx := context.Background()

	cases := []struct {
		name  string
		write func(ctx context.Context) (bool, error)
		want  error
	}{
		{
			name: "the query is cancelled",
			write: func(ctx context.Context) (bool, error) {
				return repo.Record(ctx, tenantID, userID, "ooo", time.Now().UTC())
			},
			want: context.Canceled,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var written bool

			err := w.withCancelledTx(tenantID, func(ctx context.Context) error {
				var writeErr error
				written, writeErr = tc.write(ctx)
				return writeErr
			})

			require.ErrorIs(t, err, tc.want)
			w24aRequireNoDomainAnswer(t, err)
			assert.False(t, written, "a failed write must not report that the change was stored")
			_, found, getErr := repo.Get(ctx, tenantID, userID)
			require.NoError(t, getErr)
			assert.False(t, found, "nothing was recorded")
		})
	}

	t.Run("a status the database does not know is refused", func(t *testing.T) {
		written, err := repo.Record(ctx, tenantID, userID, "not-a-status", time.Now().UTC())

		require.Error(t, err)
		w24aRequireNoDomainAnswer(t, err)
		assert.False(t, written)
		_, found, getErr := repo.Get(ctx, tenantID, userID)
		require.NoError(t, getErr)
		assert.False(t, found)
	})
}
