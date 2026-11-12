package port

import (
	"context"
	"errors"

	"github.com/google/uuid"
)

type ArchiveGuard interface {
	CheckActiveInstances(ctx context.Context, tenantID, workflowID uuid.UUID) (hasActive bool, count int32, err error)
}

// ErrUserPauseIncomplete marks a pause that left at least one of the user's
// running instances unpaused. The caller is expected to retry the whole call.
var ErrUserPauseIncomplete = errors.New("some of the user's instances could not be paused")

type UserTaskPauser interface {
	PauseUserTasks(ctx context.Context, tenantID, userID uuid.UUID, departmentID *uuid.UUID) error
}
