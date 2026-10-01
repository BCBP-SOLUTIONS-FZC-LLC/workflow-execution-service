package port

import (
	"context"

	"github.com/google/uuid"
)

// ArchiveGuard and UserTaskPauser serve the Definition Service's calls on the
// internal API (LLD §5.3).
type ArchiveGuard interface {
	CheckActiveInstances(ctx context.Context, tenantID, workflowID uuid.UUID) (hasActive bool, count int32, err error)
}

type UserTaskPauser interface {
	PauseUserTasks(ctx context.Context, tenantID, userID uuid.UUID) error
}
