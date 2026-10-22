package port

import (
	"context"

	"github.com/google/uuid"
)

type AutomationPrincipal struct {
	ID      uuid.UUID
	Subject string
}

type AutomationPrincipalResolver interface {
	AutomationPrincipal(ctx context.Context, tenantID uuid.UUID) (AutomationPrincipal, error)
}
