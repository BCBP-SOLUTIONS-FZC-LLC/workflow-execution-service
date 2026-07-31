package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/outbox"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"
	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres/db"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var _ port.OutboxRepository = (*OutboxRepo)(nil)

type OutboxRepo struct {
	pool *pgcommon.Pool
}

func NewOutboxRepo(pool *pgcommon.Pool) *OutboxRepo {
	return &OutboxRepo{pool: pool}
}

// Enqueue writes env to the outbox_events table within the transaction
// stored in ctx. The caller must be inside a Transactor.RunInTx callback;
// enqueueing outside a transaction is rejected because the outbox insert and
// the business write must be atomic.
func (r *OutboxRepo) Enqueue(ctx context.Context, env events.Envelope[json.RawMessage]) error {
	tx, ok := txFromContext(ctx)
	if !ok {
		return errors.New("outbox: Enqueue must be called inside a transaction — use Transactor.RunInTx")
	}
	return outbox.Enqueue(ctx, tx, env) //nolint:wrapcheck
}

// ListByInstance reads the instance-timeline audit trail (LLD §4.5, §4.10).
func (r *OutboxRepo) ListByInstance(
	ctx context.Context,
	tenantID, instanceID uuid.UUID,
	page port.PageRequest,
) ([]*domain.OutboxEventRecord, *port.Cursor, error) {
	ctx = withTenantGUC(ctx, tenantID)
	limit := clampLimit(page.Limit)

	params := db.ListOutboxEventsByInstanceParams{
		TenantID:           tenantID.String(),
		WorkflowInstanceID: instanceID.String(),
		PageLimit:          int32(limit + 1), //nolint:gosec
	}
	if page.After != nil {
		params.CursorCreatedAt = toPgtypeTimestamptz(&page.After.CreatedAt)
		params.CursorID = toPgtypeUUID(&page.After.ID)
	}

	var records []*domain.OutboxEventRecord
	var next *port.Cursor
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		rows, err := db.New(dbtx).ListOutboxEventsByInstance(ctx, params)
		if err != nil {
			return mapErr(err)
		}
		trimmed, cursor := paginate(rows, limit, func(row db.OutboxEvent) (time.Time, uuid.UUID) {
			return row.CreatedAt, row.ID
		})
		next = cursor
		records = make([]*domain.OutboxEventRecord, len(trimmed))
		for i, row := range trimmed {
			records[i] = outboxEventRecordFromDB(row)
		}
		return nil
	})
	return records, next, err
}
