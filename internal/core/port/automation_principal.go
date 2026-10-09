package port

import (
	"context"

	"github.com/google/uuid"
)

type AutomationPrincipalResolver interface {
	AutomationPrincipal(ctx context.Context, tenantID uuid.UUID) (uuid.UUID, error)
}
