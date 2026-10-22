package port

import (
	"context"

	"github.com/google/uuid"
)

type ArchiveGuard interface {
	CheckActiveInstances(ctx context.Context, tenantID, workflowID uuid.UUID) (hasActive bool, count int32, err error)
}

type UserTaskPauser interface {
	PauseUserTasks(ctx context.Context, tenantID, userID uuid.UUID) error
}
