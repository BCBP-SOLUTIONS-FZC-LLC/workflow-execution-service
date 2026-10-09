package port

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
)

type OutboxRepository interface {
	Enqueue(ctx context.Context, env events.Envelope[json.RawMessage]) error

	ListByInstance(
		ctx context.Context,
		tenantID, instanceID uuid.UUID,
		page PageRequest,
	) ([]*domain.OutboxEventRecord, *Cursor, error)

	ExistsForTask(ctx context.Context, eventType string, taskID uuid.UUID) (bool, error)
}
