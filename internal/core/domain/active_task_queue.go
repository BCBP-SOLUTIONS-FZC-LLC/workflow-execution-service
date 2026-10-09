package domain

import (
	"time"

	"github.com/google/uuid"
)

type ActiveTaskQueue struct {
	ID           uuid.UUID
	TenantID     uuid.UUID
	QueueName    string
	RegisteredAt time.Time
	UpdatedAt    time.Time
}
