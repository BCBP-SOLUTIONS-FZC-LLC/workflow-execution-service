package service_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestWorkflowClient_DelegateImpact_WithNoDelegationNamedCountsOnlyDelegatedWork(t *testing.T) {
	h := csaNewClientHarness()
	tenantID, delegate := uuid.New(), uuid.New()
	tagged := h.seed(tenantID, delegate, "delegation:"+uuid.NewString(), domain.TaskStatusReady, domain.InstanceStatusRunning)
	chained := h.seed(tenantID, delegate, "delegation:"+uuid.NewString()+":"+uuid.NewString(), domain.TaskStatusInProgress, domain.InstanceStatusRunning)
	untagged := []csaHeld{
		h.seed(tenantID, delegate, "manual-assign", domain.TaskStatusReady, domain.InstanceStatusRunning),
		h.seed(tenantID, delegate, "", domain.TaskStatusReady, domain.InstanceStatusRunning),
	}

	got, err := h.svc.DelegateImpact(context.Background(), port.DelegateImpactInput{TenantID: tenantID, DelegateUserID: delegate, Page: port.Page{Limit: 10}})

	require.NoError(t, err)
	assert.Equal(t, 2, got.ActiveAssignments, "manual and untagged assignments are not a delegation's")
	assert.Equal(t, 2, got.ActiveWorkflows)
	assert.ElementsMatch(t, []uuid.UUID{tagged.instance.ID, chained.instance.ID}, got.WorkflowIDs.Items)
	for _, held := range untagged {
		assert.NotContains(t, got.WorkflowIDs.Items, held.instance.ID)
	}
	assert.ElementsMatch(t, []csaIDCall{{tenantID, tagged.task.ID}, {tenantID, chained.task.ID}}, h.tasks.reads, "only delegated assignments' tasks are read")
}
