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

const offboardedStatus = "offboarded"

func NewTenantStartGate(states port.TenantStateRepository, refusedStatuses []string) *TenantStartGate {
	refused := map[string]bool{offboardedStatus: true}
	for _, s := range refusedStatuses {
		refused[strings.ToLower(strings.TrimSpace(s))] = true
	}
	return &TenantStartGate{states: states, refused: refused}
}

func (g *TenantStartGate) Allow(ctx context.Context, tenantID uuid.UUID) error {
	status, found, err := g.states.Get(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("read the tenant status: %w", err)
	}
	if found && g.refused[status] {
		return fmt.Errorf("tenant status %q: %w", status, port.ErrTenantNotActive)
	}
	return nil
}
