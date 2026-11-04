package observability

import (
	"fmt"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

var (
	TenantIDKey          = temporal.NewSearchAttributeKeyKeyword("TenantId")
	InstanceStatusKey    = temporal.NewSearchAttributeKeyKeyword("InstanceStatus")
	WorkflowVersionIDKey = temporal.NewSearchAttributeKeyKeyword("WorkflowVersionId")
	SubjectIDKey         = temporal.NewSearchAttributeKeyKeyword("SubjectId")
	BusinessKeyKey       = temporal.NewSearchAttributeKeyKeyword("BusinessKey")
)

func UpsertInstanceSearchAttributes(ctx workflow.Context, tenantID, instanceStatus, workflowVersionID, subjectID, businessKey string) error {
	if err := workflow.UpsertTypedSearchAttributes(ctx,
		TenantIDKey.ValueSet(tenantID),
		InstanceStatusKey.ValueSet(instanceStatus),
		WorkflowVersionIDKey.ValueSet(workflowVersionID),
		SubjectIDKey.ValueSet(subjectID),
		BusinessKeyKey.ValueSet(businessKey),
	); err != nil {
		return fmt.Errorf("upsert instance search attributes: %w", err)
	}
	return nil
}
