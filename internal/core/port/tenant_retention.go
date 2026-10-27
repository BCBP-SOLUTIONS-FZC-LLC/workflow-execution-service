package port

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

var ErrTenantRetentionChanged = errors.New("tenant retention record changed since it was read")

type TenantPurgeDue struct {
	TenantID      uuid.UUID
	Attempts      int
	RecordVersion int64
}

type TenantRetentionRepository interface {
	RecordOffboarded(ctx context.Context, tenantID uuid.UUID, offboardedAt, purgeAfter time.Time) error
	RecordReturned(ctx context.Context, tenantID uuid.UUID, at time.Time) error
	ListDue(ctx context.Context, now time.Time, limit int) ([]TenantPurgeDue, error)
	MarkPurged(ctx context.Context, tenantID uuid.UUID, version int64, at time.Time) (int64, error)
	RecordFailure(ctx context.Context, tenantID uuid.UUID, version int64, attempts int, next time.Time) error
	Current(ctx context.Context, tenantID uuid.UUID, version int64) error
}

type TenantDataPurger interface {
	PurgeBatch(ctx context.Context, tenantID uuid.UUID, version int64, limit int) (map[string]int64, error)
	HasRows(ctx context.Context, tenantID uuid.UUID) (bool, error)
}

type TenantStateRepository interface {
	Record(ctx context.Context, tenantID uuid.UUID, status string, changedAt time.Time) error
	Get(ctx context.Context, tenantID uuid.UUID) (status string, found bool, err error)
}

type TenantGate interface {
	Allow(ctx context.Context, tenantID uuid.UUID) error
}
