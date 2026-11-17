package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/outbox"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"
	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres/db"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var (
	_ port.OutboxRepository     = (*OutboxRepo)(nil)
	_ port.WorkflowSignalOutbox = (*OutboxRepo)(nil)
)

type OutboxRepo struct {
	pool *pgcommon.Pool
}

func NewOutboxRepo(pool *pgcommon.Pool) *OutboxRepo {
	return &OutboxRepo{pool: pool}
}

func (r *OutboxRepo) Enqueue(ctx context.Context, env events.Envelope[json.RawMessage]) error {
	tx, ok := txFromContext(ctx)
	if !ok {
		return errors.New("outbox: Enqueue must be called inside a transaction — use Transactor.RunInTx")
	}
	return mapErr(outbox.Enqueue(ctx, tx, env))
}

// EnqueueSignal records the signal as an outbox record for the relay to deliver.
// The record is not ordered (ordering needs a sequence the application role is
// not granted), so the workflow reads the time each signal carries.
func (r *OutboxRepo) EnqueueSignal(ctx context.Context, sig port.WorkflowSignal) error {
	tx, ok := txFromContext(ctx)
	if !ok {
		return errors.New("outbox: EnqueueSignal must be called inside a transaction — use Transactor.RunInTx")
	}
	data, err := json.Marshal(port.WorkflowSignalMessage{
		TargetInstanceID: sig.InstanceID.String(), TemporalWorkflowID: sig.TemporalWorkflowID,
		SignalName: sig.Name, SignalPayload: sig.Payload,
	})
	if err != nil {
		return fmt.Errorf("outbox: marshal %s signal: %w", sig.Name, err)
	}
	subject := "instances/" + sig.InstanceID.String()
	env := events.NewEnvelope[json.RawMessage](port.WorkflowSignalEventType, domain.EventSource, data,
		events.WithTenantID(sig.TenantID.String()), events.WithSubject(subject))
	return mapErr(outbox.Enqueue(ctx, tx, env))
}

func (r *OutboxRepo) ExistsForTask(ctx context.Context, eventType string, taskID uuid.UUID) (bool, error) {
	var exists bool
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		var err error
		exists, err = db.New(dbtx).OutboxEventExistsForTask(ctx, db.OutboxEventExistsForTaskParams{
			EventType: eventType,
			TaskID:    taskID.String(),
		})
		if err != nil {
			return mapErr(err)
		}
		return nil
	})
	return exists, err
}

func (r *OutboxRepo) ExistsByID(ctx context.Context, tenantID, id uuid.UUID) (bool, error) {
	ctx = withTenantGUC(ctx, tenantID)
	var exists bool
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		var err error
		exists, err = db.New(dbtx).OutboxEventExistsByID(ctx, db.OutboxEventExistsByIDParams{
			TenantID: tenantID.String(),
			ID:       id,
		})
		return mapErr(err)
	})
	return exists, err
}

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
		PageLimit:          pageLimit(limit),
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
