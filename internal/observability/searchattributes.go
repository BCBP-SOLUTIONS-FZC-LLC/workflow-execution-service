package observability

import (
	"fmt"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// The four custom Search Attributes execution LLD §3.6 names — all Keyword
// type, registered against a provisioned Temporal Advanced Visibility
// (Elasticsearch-backed) store. Not called from anywhere in this package:
// Temporal only allows Search Attribute upserts from inside workflow-context
// code, so the real call site belongs to whichever task wires this into the
// workflow function itself.
var (
	TenantIDKey          = temporal.NewSearchAttributeKeyKeyword("TenantId")
	InstanceStatusKey    = temporal.NewSearchAttributeKeyKeyword("InstanceStatus")
	WorkflowVersionIDKey = temporal.NewSearchAttributeKeyKeyword("WorkflowVersionId")
	SubjectIDKey         = temporal.NewSearchAttributeKeyKeyword("SubjectId")
	BusinessKeyKey       = temporal.NewSearchAttributeKeyKeyword("BusinessKey")
)

// UpsertInstanceSearchAttributes sets the LLD §3.6 Search Attributes at once.
// Must be called from workflow-context code (workflow start and on every status
// transition, including DEGRADED, per §3.6) — Temporal only allows Search
// Attribute upserts from inside a running workflow.
//
// Subject and key are both set: the key identifies an instance within its
// subject, so searching on it alone finds every subject's instance of that
// stage. An operator looking for one entity's runs filters on the subject.
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
