package port

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type TenantRetentionRepository interface {
	RecordOffboarded(ctx context.Context, tenantID uuid.UUID, offboardedAt, purgeAfter time.Time) error
	ListDue(ctx context.Context, now time.Time, limit int) ([]uuid.UUID, error)
	MarkPurged(ctx context.Context, tenantID uuid.UUID, at time.Time) error
}

type TenantDataPurger interface {
	PurgeBatch(ctx context.Context, tenantID uuid.UUID, limit int) (map[string]int64, error)
	HasRows(ctx context.Context, tenantID uuid.UUID) (bool, error)
}

type TenantStateRepository interface {
	Record(ctx context.Context, tenantID uuid.UUID, status string, changedAt time.Time) error
	Get(ctx context.Context, tenantID uuid.UUID) (status string, found bool, err error)
}

type TenantGate interface {
	Allow(ctx context.Context, tenantID uuid.UUID) error
}
