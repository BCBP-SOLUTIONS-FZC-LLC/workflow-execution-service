package port

import (
	"context"
	"errors"

	"github.com/google/uuid"
)

// ErrDefinitionRejected is a Definition Service answer no retry can change: a
// 4xx, or a response that breaks the contract. ErrDefinitionUnavailable is
// a transport failure or a 5xx after the client's own retries (LLD §5.3).
var (
	ErrDefinitionRejected    = errors.New("definition service rejected the request")
	ErrDefinitionUnavailable = errors.New("definition service unavailable")
)

// CompiledWorkflow is a workflow version and its stored compiled plan, in any
// status. CompiledPlanJSON is the plan's JSON, or empty for a version with
// none; VersionNumber is zero for a DRAFT.
type CompiledWorkflow struct {
	WorkflowID       uuid.UUID
	VersionID        uuid.UUID
	VersionNumber    int32
	Status           string
	IsValid          bool
	CompiledPlanJSON string
}

// DefinitionServiceClient reads a workflow version's compiled plan from the
// Definition Service's internal API (LLD §5.3).
type DefinitionServiceClient interface {
	GetCompiledWorkflow(ctx context.Context, tenantID, workflowVersionID uuid.UUID) (*CompiledWorkflow, error)
}
