package port

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type ProcessedEventRepository interface {
	IsProcessed(ctx context.Context, eventID uuid.UUID, consumer string) (bool, error)
	RecordIfNew(ctx context.Context, eventID uuid.UUID, consumer, eventType string) (bool, error)
	PruneOlderThan(ctx context.Context, olderThan time.Duration) (int64, error)
}
