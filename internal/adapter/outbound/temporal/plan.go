package temporal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"

	outboundgrpc "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/grpc"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

// GetCompiledPlan is GetCompiledPlanActivity (LLD §3.1): the workflow
// function's first activity, always — a single read-only gRPC call to
// Definition Service, no DB write. Any error here is folded into
// "DefinitionServiceClientError", the one NonRetryableErrorTypes entry
// externalCallActivityOptions already declares (internal/workflow/activities.go)
// — a malformed ID is exactly as permanent as an upstream rejection, neither
// is worth retrying.
func (d *Deps) GetCompiledPlan(ctx context.Context, in port.GetCompiledPlanInput) (port.GetCompiledPlanOutput, error) {
	tenantID, err := uuid.Parse(in.TenantID)
	if err != nil {
		return port.GetCompiledPlanOutput{}, nonRetryable("DefinitionServiceClientError", fmt.Errorf("parse tenant_id: %w", err))
	}
	versionID, err := uuid.Parse(in.VersionID)
	if err != nil {
		return port.GetCompiledPlanOutput{}, nonRetryable("DefinitionServiceClientError", fmt.Errorf("parse version_id: %w", err))
	}

	cw, err := d.Definitions.GetCompiledWorkflow(ctx, tenantID, versionID)
	if err != nil {
		if errors.Is(err, outboundgrpc.ErrUpstreamRejected) {
			return port.GetCompiledPlanOutput{}, nonRetryable("DefinitionServiceClientError", err)
		}
		return port.GetCompiledPlanOutput{}, fmt.Errorf("get compiled workflow: %w", err)
	}

	var collab dsl.CompiledCollaboration
	if err := json.Unmarshal([]byte(cw.CompiledPlanJSON), &collab); err != nil {
		return port.GetCompiledPlanOutput{}, nonRetryable("DefinitionServiceClientError", fmt.Errorf("unmarshal compiled plan: %w", err))
	}
	return port.GetCompiledPlanOutput{Collaboration: collab}, nil
}
