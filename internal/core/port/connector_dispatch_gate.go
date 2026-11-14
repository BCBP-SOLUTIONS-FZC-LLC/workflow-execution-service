package port

import (
	"context"

	"github.com/google/uuid"
)

// ConnectorDispatchGate says whether a connector may still run for a task: the
// task must be open and its instance must not have ended (execution LLD §6.2).
type ConnectorDispatchGate interface {
	Dispatchable(ctx context.Context, tenantID, taskID uuid.UUID) (bool, error)
}
