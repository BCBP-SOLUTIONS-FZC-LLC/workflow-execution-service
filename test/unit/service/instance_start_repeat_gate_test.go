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

func startOnce(t *testing.T) (*service.InstanceService, port.StartInstanceInput, *port.Instance) {
	t.Helper()
	svc, _, _, _, _, _, definitions, _ := newInstanceServiceHarness()
	tenantID, versionID := uuid.New(), uuid.New()
	definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))
	in := port.StartInstanceInput{
		TenantID: tenantID, WorkflowVersionID: versionID, SubjectID: "SUBJ-1", BusinessKey: "TND-001",
		ContextJSON: json.RawMessage(`{}`), StartedByUserID: uuid.New(),
	}
	first, err := svc.Start(context.Background(), in)
	require.NoError(t, err)
	return svc, in, first
}

func TestInstanceService_Start_ARepeatOfASuccessfulStartReplaysAfterTheTenantIsSuspended(t *testing.T) {
	svc, in, first := startOnce(t)
	svc.Gate = service.NewTenantStartGate(&fakeTenantStates{status: map[uuid.UUID]string{in.TenantID: "suspended"}}, []string{"suspended"})

	again, err := svc.Start(context.Background(), in)

	require.NoError(t, err)
	assert.Equal(t, first.ID, again.ID, "the repeat returns the instance the first start made")
}

func TestInstanceService_Start_ARepeatOfASuccessfulStartDoesNotNeedTheStatusRead(t *testing.T) {
	svc, in, first := startOnce(t)
	svc.Gate = service.NewTenantStartGate(&fakeTenantStates{getErr: errors.New("db down")}, []string{"suspended"})

	again, err := svc.Start(context.Background(), in)

	require.NoError(t, err)
	assert.Equal(t, first.ID, again.ID)
}

func TestInstanceService_Start_ANewStartIsStillRefusedForASuspendedTenant(t *testing.T) {
	svc, in, _ := startOnce(t)
	svc.Gate = service.NewTenantStartGate(&fakeTenantStates{status: map[uuid.UUID]string{in.TenantID: "suspended"}}, []string{"suspended"})
	in.BusinessKey = "TND-002"

	_, err := svc.Start(context.Background(), in)

	require.ErrorIs(t, err, port.ErrTenantNotActive)
}
