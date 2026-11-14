package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestInstanceService_Start_ContextEndedBetweenStartAttempts(t *testing.T) {
	svc, instances, _, _, _, temporal, definitions, _ := newInstanceServiceHarness()
	tenantID, versionID, caller := uuid.New(), uuid.New(), uuid.New()
	definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	attempts := 0
	temporal.startFunc = func(_ context.Context, _ port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
		attempts++
		cancel()
		return port.StartWorkflowOutput{}, errors.New("temporal frontend unreachable")
	}

	_, err := svc.Start(ctx, port.StartInstanceInput{
		TenantID: tenantID, WorkflowVersionID: versionID, SubjectID: "SUBJ-1", BusinessKey: "TND-001", StartedByUserID: caller,
	})

	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
	assert.ErrorContains(t, err, "start workflow retry: context canceled")
	assert.Equal(t, 1, attempts, "no further attempt is made once the context has ended")
	require.Len(t, instances.byID, 1, "the committed row stays so the same request can resume the start")
	for _, inst := range instances.byID {
		assert.Equal(t, tenantID, inst.TenantID)
		assert.Equal(t, domain.InstanceStatusRunning, inst.Status)
		assert.Empty(t, inst.TemporalRunID)
	}
}
