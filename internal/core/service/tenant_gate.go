package service

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var _ port.TenantGate = (*TenantStartGate)(nil)

type TenantStartGate struct {
	states  port.TenantStateRepository
	refused map[string]bool
}

func NewTenantStartGate(states port.TenantStateRepository, refusedStatuses []string) *TenantStartGate {
	refused := make(map[string]bool, len(refusedStatuses))
	for _, s := range refusedStatuses {
		refused[strings.ToLower(strings.TrimSpace(s))] = true
	}
	return &TenantStartGate{states: states, refused: refused}
}

func (g *TenantStartGate) Allow(ctx context.Context, tenantID uuid.UUID) error {
	if len(g.refused) == 0 {
		return nil
	}
	status, found, err := g.states.Get(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("read the tenant status: %w", err)
	}
	if found && g.refused[status] {
		return fmt.Errorf("tenant status %q: %w", status, port.ErrTenantNotActive)
	}
	return nil
}
