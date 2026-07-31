package port

import (
	"context"
	"encoding/json"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
)

// OutboxRepository enqueues events inside a business transaction. The outbox
// relay (fetch → publish → mark-sent/failed) is handled entirely by
// platform-events' own outbox.Runner, started only in cmd/server (LLD §6.6).
type OutboxRepository interface {
	Enqueue(ctx context.Context, env events.Envelope[json.RawMessage]) error

	// ListByInstance reads the instance-timeline audit trail (LLD §4.5, §4.10).
	ListByInstance(
		ctx context.Context,
		tenantID, instanceID uuid.UUID,
		page PageRequest,
	) ([]*domain.OutboxEventRecord, *Cursor, error)
}
