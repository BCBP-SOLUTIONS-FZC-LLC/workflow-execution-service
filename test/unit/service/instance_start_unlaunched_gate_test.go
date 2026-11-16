package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

func startLeftUnlaunched(t *testing.T) (*service.InstanceService, port.StartInstanceInput, *int) {
	t.Helper()
	svc, instances, _, _, _, temporal, definitions, _ := newInstanceServiceHarness()
	tenantID, versionID := uuid.New(), uuid.New()
	definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))
	starts := 0
	temporal.startFunc = func(context.Context, port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
		starts++
		return port.StartWorkflowOutput{}, errors.New("temporal frontend unreachable")
	}
	in := port.StartInstanceInput{
		TenantID: tenantID, WorkflowVersionID: versionID, SubjectID: "SUBJ-1", BusinessKey: "TND-001",
		ContextJSON: json.RawMessage(`{}`), StartedByUserID: uuid.New(),
	}
	_, err := svc.Start(context.Background(), in)
	require.Error(t, err)
	require.Len(t, instances.byID, 1, "the row is committed even though no workflow started")
	starts = 0
	return svc, in, &starts
}

func TestInstanceService_Start_ARepeatOfAStartThatNeverLaunchedIsStillGatedForASuspendedTenant(t *testing.T) {
	svc, in, starts := startLeftUnlaunched(t)
	svc.Gate = service.NewTenantStartGate(&fakeTenantStates{status: map[uuid.UUID]string{in.TenantID: "suspended"}}, []string{"suspended"})

	_, err := svc.Start(context.Background(), in)

	require.ErrorIs(t, err, port.ErrTenantNotActive)
	assert.Zero(t, *starts, "no workflow is launched for a suspended tenant")
}

func TestInstanceService_Start_ARepeatOfAStartThatNeverLaunchedFailsWhenTheStatusReadFails(t *testing.T) {
	svc, in, starts := startLeftUnlaunched(t)
	svc.Gate = service.NewTenantStartGate(&fakeTenantStates{getErr: errors.New("db down")}, []string{"suspended"})

	_, err := svc.Start(context.Background(), in)

	require.Error(t, err)
	assert.Zero(t, *starts)
}
