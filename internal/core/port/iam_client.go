package port

import (
	"context"

	"github.com/google/uuid"
)

type UserStatus struct {
	IsDeleted      bool
	IsOOO          bool
	DelegateUserID *uuid.UUID
}

type IAMClient interface {
	GetUserStatus(ctx context.Context, tenantID, userID uuid.UUID) (UserStatus, error)
}
