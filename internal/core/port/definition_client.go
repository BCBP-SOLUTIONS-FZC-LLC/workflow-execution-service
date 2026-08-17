package port

import (
	"context"

	"github.com/google/uuid"
)

// CompiledWorkflow mirrors outboundgrpc.DefinitionClient's own CompiledWorkflow
// struct field-for-field. That package predates this port and keeps its own
// copy for its existing (Worker-side) callers; this one is what
// internal/core/service's InstanceService/TemplateCachePrewarmer code
// against, since service may not import adapter.
type CompiledWorkflow struct {
	WorkflowID       uuid.UUID
	VersionID        uuid.UUID
	VersionNumber    int32
	Status           string
	IsValid          bool
	CompiledPlanJSON string
}

// DefinitionServiceClient is the outbound gRPC contract for fetching a
// published workflow version's compiled plan (LLD §5.1, §5.3). Its real
// implementation, outboundgrpc.DefinitionClient, predates this port and
// returns its own package-local outboundgrpc.ErrUpstreamUnavailable/
// ErrUpstreamRejected sentinels on failure — a caller needing to distinguish
// the two still imports outboundgrpc for those two sentinels, the same way
// internal/adapter/outbound/temporal/plan.go already does.
type DefinitionServiceClient interface {
	GetCompiledWorkflow(ctx context.Context, tenantID, workflowVersionID uuid.UUID) (*CompiledWorkflow, error)
}
