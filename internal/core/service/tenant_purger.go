package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

const tenantsPerPurgeTick = 100

type TenantPurgerDeps struct {
	Retention port.TenantRetentionRepository
	Data      port.TenantDataPurger
	Batch     int
	Interval  time.Duration
	Now       func() time.Time
	Record    func(table string, deleted int64)
	Failed    func()
	Logger    port.Logger
}

type TenantPurger struct {
	deps TenantPurgerDeps
}

func NewTenantPurger(deps TenantPurgerDeps) *TenantPurger {
	if deps.Now == nil {
		deps.Now = time.Now
	}
	if deps.Logger == nil {
		deps.Logger = noopLogger{}
	}
	return &TenantPurger{deps: deps}
}

func (p *TenantPurger) Tick(ctx context.Context) error {
	due, err := p.deps.Retention.ListDue(ctx, p.deps.Now(), tenantsPerPurgeTick)
	if err != nil {
		return fmt.Errorf("list tenants due for purge: %w", err)
	}
	var failures []error
	for _, tenantID := range due {
		if err := p.purgeTenant(ctx, tenantID); err != nil {
			p.deps.Failed()
			p.deps.Logger.Error("tenant purge failed", map[string]any{"tenant_id": tenantID, "error": err.Error()})
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

func (p *TenantPurger) purgeTenant(ctx context.Context, tenantID uuid.UUID) error {
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("purge tenant %s: %w", tenantID, err)
		}
		deleted, err := p.deps.Data.PurgeBatch(ctx, tenantID, p.deps.Batch)
		if err != nil {
			return fmt.Errorf("purge tenant %s: %w", tenantID, err)
		}
		if len(deleted) == 0 {
			remaining, err := p.deps.Data.HasRows(ctx, tenantID)
			if err != nil {
				return fmt.Errorf("check tenant %s for remaining rows: %w", tenantID, err)
			}
			if remaining {
				return fmt.Errorf("purge tenant %s: rows remain after a pass that deleted none, another replica may be purging it", tenantID)
			}
			break
		}
		for table, n := range deleted {
			p.deps.Record(table, n)
		}
	}
	if err := p.deps.Retention.MarkPurged(ctx, tenantID, p.deps.Now()); err != nil {
		return fmt.Errorf("mark tenant %s purged: %w", tenantID, err)
	}
	p.deps.Logger.Info("tenant purged", map[string]any{"tenant_id": tenantID})
	return nil
}

func (p *TenantPurger) Run(ctx context.Context) {
	ticker := time.NewTicker(p.deps.Interval)
	defer ticker.Stop()
	for {
		if err := p.Tick(ctx); err != nil {
			p.deps.Logger.Error("tenant purge sweep failed", map[string]any{"error": err.Error()})
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
