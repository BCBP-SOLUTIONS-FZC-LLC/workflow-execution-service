//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestOutboxRepo_EnqueueSignal_WithAPayloadThatIsNotJSON_FailsNamingTheSignalAndWritesNothing(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	outboxRepo := postgres.NewOutboxRepo(appPool)
	transactor := postgres.NewTransactor(appPool)

	cases := []struct {
		name    string
		payload string
	}{
		{name: "plain text", payload: `not json`},
		{name: "unterminated object", payload: `{"user":`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tenantID := uuid.New()
			ctx := withGUC(context.Background(), tenantID)

			err := transactor.RunInTx(ctx, func(ctx context.Context) error {
				if err := outboxRepo.EnqueueSignal(ctx, signalFor(tenantID, uuid.New(), port.SignalInstanceReassign, `{}`)); err != nil {
					return err
				}
				return outboxRepo.EnqueueSignal(ctx, signalFor(tenantID, uuid.New(), port.SignalInstanceReassign, tc.payload))
			})

			var marshalErr *json.MarshalerError
			require.ErrorAs(t, err, &marshalErr)
			require.ErrorContains(t, err, "outbox: marshal "+port.SignalInstanceReassign+" signal")
			var stored int
			require.NoError(t, superPool.WithConn(context.Background(), func(ctx context.Context, conn *pgxpool.Conn) error {
				return conn.QueryRow(ctx, "SELECT count(*) FROM outbox_events WHERE tenant_id = $1", tenantID.String()).Scan(&stored)
			}))
			assert.Zero(t, stored, "the transaction rolled back, so not even the earlier valid signal is recorded")
		})
	}
}
