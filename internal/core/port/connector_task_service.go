package port

import (
	"context"

	"github.com/google/uuid"
)

type ConnectorTaskService interface {
	Complete(ctx context.Context, tenantID, taskID uuid.UUID, output map[string]any) error
	Fail(ctx context.Context, tenantID, taskID uuid.UUID, errorClass string, output map[string]any) error
}
