package port

import (
	"context"

	"github.com/google/uuid"
)

// AutomationPrincipalResolver names the actor a connector-driven step is
// recorded under: the tenant's platform-automation principal, held by the
// identity subsystem. It returns the principal's stable identifier, and
// ErrNoAutomationPrincipal when the tenant has none.
type AutomationPrincipalResolver interface {
	AutomationPrincipal(ctx context.Context, tenantID uuid.UUID) (uuid.UUID, error)
}
