package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type OutboxEventRecord struct {
	ID        uuid.UUID
	EventType string
	Payload   json.RawMessage
	CreatedAt time.Time
}
