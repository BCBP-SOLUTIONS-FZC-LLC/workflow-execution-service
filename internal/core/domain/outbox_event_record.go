package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// OutboxEventRecord is a read-side projection of one outbox_events row —
// the merged audit trail (LLD §4.5, §4.10) — used for instance-timeline
// reads. outbox_events itself is platform-events-owned; this type exists so
// callers of OutboxRepository.ListByInstance don't need to import the
// sqlc-generated db package.
type OutboxEventRecord struct {
	ID        uuid.UUID
	EventType string
	Payload   json.RawMessage
	CreatedAt time.Time
}
