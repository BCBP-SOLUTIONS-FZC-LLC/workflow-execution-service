package port

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// ProcessedEventRepository is the consumer-idempotency dedup table (LLD
// §4.7, §6.3). PruneOlderThan is a distinct retention concern from
// OutboxRepository/outbox.Runner's own PrunePublished — the two must never
// share a constant (processed_event's dedup state has no audit value past a
// week; outbox_events doubles as the 7-year audit trail, LLD §4.11/§9.5).
type ProcessedEventRepository interface {
	IsProcessed(ctx context.Context, eventID uuid.UUID, consumer string) (bool, error)
	RecordIfNew(ctx context.Context, eventID uuid.UUID, consumer, eventType string) (bool, error)
	PruneOlderThan(ctx context.Context, olderThan time.Duration) (int64, error)
}
