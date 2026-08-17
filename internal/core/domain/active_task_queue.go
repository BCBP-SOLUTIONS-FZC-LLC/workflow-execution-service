package domain

import (
	"time"

	"github.com/google/uuid"
)

// ActiveTaskQueue is a row in the Worker-topology registry (LLD §4.6):
// which Temporal task queues are currently being served, and which tenant
// each isolated one belongs to.
type ActiveTaskQueue struct {
	ID           uuid.UUID
	TenantID     uuid.UUID
	QueueName    string
	RegisteredAt time.Time
	UpdatedAt    time.Time
}
