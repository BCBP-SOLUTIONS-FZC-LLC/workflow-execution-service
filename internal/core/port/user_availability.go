package port

import (
	"context"
	"time"

	"github.com/google/uuid"
)

const (
	AvailabilityAvailable = "available"
	AvailabilityBusy      = "busy"
	AvailabilityOOO       = "ooo"
)

type UserAvailabilityRepository interface {
	Record(ctx context.Context, tenantID, userID uuid.UUID, status string, changedAt time.Time) error
	Get(ctx context.Context, tenantID, userID uuid.UUID) (status string, found bool, err error)
}
