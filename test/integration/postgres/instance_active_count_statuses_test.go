//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestInstanceRepo_CountActiveByWorkflow_CountsEveryNonTerminalStatus(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	repo := postgres.NewInstanceRepo(appPool)
	ctx := context.Background()
	tenantID, workflowID := uuid.New(), uuid.New()

	statuses := []struct {
		status  domain.InstanceStatus
		counted bool
	}{
		{domain.InstanceStatusRunning, true},
		{domain.InstanceStatusPaused, true},
		{domain.InstanceStatusDegraded, true},
		{domain.InstanceStatusCompleted, false},
		{domain.InstanceStatusTerminated, false},
		{domain.InstanceStatusFailed, false},
	}
	want := int64(0)
	for _, tc := range statuses {
		inst := newInstance(tenantID, time.Now().UTC())
		inst.WorkflowID = workflowID
		require.NoError(t, repo.Create(ctx, inst))
		if tc.status != domain.InstanceStatusRunning {
			_, err := repo.UpdateStatus(ctx, tenantID, inst.ID, tc.status, inst.RecordVersion)
			require.NoError(t, err, tc.status)
		}
		if tc.counted {
			want++
		}
	}

	count, err := repo.CountActiveByWorkflow(ctx, tenantID, workflowID)

	require.NoError(t, err)
	assert.Equal(t, want, count, "RUNNING, PAUSED and DEGRADED instances keep a workflow from being archived; COMPLETED, TERMINATED and FAILED do not")
}
