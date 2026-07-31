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
	// RecordIfNew inserts the (event_id, consumer) pair and returns true if it
	// was new, false if already processed (ON CONFLICT DO NOTHING).
	RecordIfNew(ctx context.Context, eventID uuid.UUID, consumer, eventType string) (bool, error)

	// PruneOlderThan deletes rows whose processed_at predates the cutoff and
	// returns the number of rows removed.
	PruneOlderThan(ctx context.Context, olderThan time.Duration) (int64, error)
}
