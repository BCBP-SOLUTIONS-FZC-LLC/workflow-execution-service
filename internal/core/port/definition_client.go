package port

import (
	"context"
	"errors"

	"github.com/google/uuid"
)

var (
	ErrDefinitionRejected    = errors.New("definition service rejected the request")
	ErrDefinitionUnavailable = errors.New("definition service unavailable")
)

type CompiledWorkflow struct {
	WorkflowID       uuid.UUID
	VersionID        uuid.UUID
	VersionNumber    int32
	Status           string
	IsValid          bool
	CompiledPlanJSON string
}

type DefinitionServiceClient interface {
	GetCompiledWorkflow(ctx context.Context, tenantID, workflowVersionID uuid.UUID) (*CompiledWorkflow, error)
}
