package temporal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

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
		if errors.Is(err, port.ErrDefinitionRejected) {
			return port.GetCompiledPlanOutput{}, nonRetryable("DefinitionServiceClientError", err)
		}
		return port.GetCompiledPlanOutput{}, fmt.Errorf("get compiled workflow: %w", err)
	}
	if cw.Status != "PUBLISHED" {
		return port.GetCompiledPlanOutput{}, nonRetryable("DefinitionServiceClientError", fmt.Errorf("version %s is %s: %w", versionID, cw.Status, port.ErrVersionNotPublished))
	}
	if !cw.IsValid {
		return port.GetCompiledPlanOutput{}, nonRetryable("DefinitionServiceClientError", fmt.Errorf("version %s: %w", versionID, port.ErrVersionInvalid))
	}

	var collab dsl.CompiledCollaboration
	if err := json.Unmarshal([]byte(cw.CompiledPlanJSON), &collab); err != nil {
		return port.GetCompiledPlanOutput{}, nonRetryable("DefinitionServiceClientError", fmt.Errorf("unmarshal compiled plan: %w", err))
	}
	if collab.SchemaVersion != dsl.CurrentSchemaVersion {
		return port.GetCompiledPlanOutput{Collaboration: collab}, nil
	}
	expanded, err := dsl.ExpandCalls(&collab, domain.MaxPlanStages)
	if err != nil {
		return port.GetCompiledPlanOutput{}, nonRetryable("DefinitionServiceClientError", fmt.Errorf("expand compiled plan: %w", err))
	}
	return port.GetCompiledPlanOutput{Collaboration: *expanded}, nil
}
