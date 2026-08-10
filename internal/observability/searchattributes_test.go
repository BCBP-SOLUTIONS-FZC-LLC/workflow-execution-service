package observability_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

type searchAttributeReadback struct {
	TenantID          string
	InstanceStatus    string
	WorkflowVersionID string
	BusinessKey       string
}

func upsertAndReadBackWorkflow(ctx workflow.Context) (searchAttributeReadback, error) {
	if err := observability.UpsertInstanceSearchAttributes(ctx, "tenant-1", "DEGRADED", "wf-version-42", "order-123"); err != nil {
		return searchAttributeReadback{}, err
	}

	current := workflow.GetTypedSearchAttributes(ctx)
	var out searchAttributeReadback
	out.TenantID, _ = current.GetKeyword(observability.TenantIDKey)
	out.InstanceStatus, _ = current.GetKeyword(observability.InstanceStatusKey)
	out.WorkflowVersionID, _ = current.GetKeyword(observability.WorkflowVersionIDKey)
	out.BusinessKey, _ = current.GetKeyword(observability.BusinessKeyKey)
	return out, nil
}

func TestUpsertInstanceSearchAttributes_SetsAllFourTypedKeys(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflow(upsertAndReadBackWorkflow)

	env.ExecuteWorkflow(upsertAndReadBackWorkflow)

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var got searchAttributeReadback
	require.NoError(t, env.GetWorkflowResult(&got))

	require.Equal(t, searchAttributeReadback{
		TenantID:          "tenant-1",
		InstanceStatus:    "DEGRADED",
		WorkflowVersionID: "wf-version-42",
		BusinessKey:       "order-123",
	}, got)
}
