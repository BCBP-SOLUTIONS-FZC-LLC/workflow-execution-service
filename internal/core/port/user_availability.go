package port

import (
	"context"
	"time"

	"github.com/google/uuid"
)

func UserAvailabilityScope(tenantID, userID uuid.UUID) string {
	return "user_availability:" + tenantID.String() + ":" + userID.String()
}

const DelegationEndedReassigned = "reassigned"

const (
	AvailabilityAvailable = "available"
	AvailabilityBusy      = "busy"
	AvailabilityOOO       = "ooo"
)

type UserAvailabilityRepository interface {
	Record(ctx context.Context, tenantID, userID uuid.UUID, status string, changedAt time.Time) (written bool, err error)
	Get(ctx context.Context, tenantID, userID uuid.UUID) (status string, found bool, err error)
}
