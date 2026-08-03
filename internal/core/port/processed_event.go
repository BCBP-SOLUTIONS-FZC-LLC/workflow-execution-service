package port

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// ProcessedEventRepository is the consumer-idempotency dedup table (LLD
// §4.7, §6.3). Ported from feat/persistence-layer's real interface
// (RecordIfNew/PruneOlderThan) plus one additive method, IsProcessed.
type ProcessedEventRepository interface {
	// IsProcessed is the §6.3-permitted cheap up-front read — a pure
	// optimization to skip an obvious replay before doing any work. Never a
	// substitute for RecordIfNew, which is always the true last step.
	IsProcessed(ctx context.Context, eventID uuid.UUID, consumer string) (bool, error)

	// RecordIfNew inserts the (event_id, consumer) pair and returns true if
	// it was new, false if already processed (ON CONFLICT DO NOTHING).
	RecordIfNew(ctx context.Context, eventID uuid.UUID, consumer, eventType string) (bool, error)

	// PruneOlderThan deletes rows whose processed_at predates the cutoff and
	// returns the number of rows removed.
	PruneOlderThan(ctx context.Context, olderThan time.Duration) (int64, error)
}
