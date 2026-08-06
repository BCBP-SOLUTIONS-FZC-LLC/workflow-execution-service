package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres/db"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var _ port.ProcessedEventRepository = (*ProcessedEventRepo)(nil)

type ProcessedEventRepo struct {
	pool *pgcommon.Pool
}

func NewProcessedEventRepo(pool *pgcommon.Pool) *ProcessedEventRepo {
	return &ProcessedEventRepo{pool: pool}
}

func (r *ProcessedEventRepo) IsProcessed(ctx context.Context, eventID uuid.UUID, consumer string) (bool, error) {
	var processed bool
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		_, err := db.New(dbtx).GetProcessedEvent(ctx, db.GetProcessedEventParams{EventID: eventID, Consumer: consumer})
		if errors.Is(err, pgx.ErrNoRows) {
			processed = false
			return nil
		}
		if err != nil {
			return mapErr(err)
		}
		processed = true
		return nil
	})
	return processed, err
}

func (r *ProcessedEventRepo) RecordIfNew(
	ctx context.Context,
	eventID uuid.UUID,
	consumer, eventType string,
) (bool, error) {
	var isNew bool
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		rows, err := db.New(dbtx).InsertProcessedEvent(ctx, db.InsertProcessedEventParams{
			EventID:   eventID,
			Consumer:  consumer,
			EventType: toNullableText(eventType),
		})
		if err != nil {
			return mapErr(err)
		}
		isNew = rows > 0
		return nil
	})
	return isNew, err
}

func (r *ProcessedEventRepo) PruneOlderThan(ctx context.Context, olderThan time.Duration) (int64, error) {
	var deleted int64
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		rows, err := db.New(dbtx).PruneProcessedEventsOlderThan(ctx, time.Now().UTC().Add(-olderThan))
		if err != nil {
			return mapErr(err)
		}
		deleted = rows
		return nil
	})
	return deleted, err
}
