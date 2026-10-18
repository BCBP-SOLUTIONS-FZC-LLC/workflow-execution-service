package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

type fakeTenantStates struct {
	status   map[uuid.UUID]string
	getErr   error
	recorded []recordedState
	recErr   error
}

type recordedState struct {
	tenantID  uuid.UUID
	status    string
	changedAt time.Time
}

func (f *fakeTenantStates) Record(_ context.Context, tenantID uuid.UUID, status string, changedAt time.Time) error {
	f.recorded = append(f.recorded, recordedState{tenantID, status, changedAt})
	return f.recErr
}

func (f *fakeTenantStates) Get(_ context.Context, tenantID uuid.UUID) (string, bool, error) {
	s, ok := f.status[tenantID]
	return s, ok, f.getErr
}

func TestTenantStartGate_RefusesOnlyTheConfiguredStatuses(t *testing.T) {
	tenant := uuid.New()
	for _, tc := range []struct {
		name    string
		refused []string
		status  map[uuid.UUID]string
		want    error
	}{
		{"nothing is refused by default", nil, map[uuid.UUID]string{tenant: "suspended"}, nil},
		{"a configured status is refused", []string{"suspended", "trial_expired"}, map[uuid.UUID]string{tenant: "suspended"}, port.ErrTenantNotActive},
		{"another configured status is refused", []string{"suspended", "trial_expired"}, map[uuid.UUID]string{tenant: "trial_expired"}, port.ErrTenantNotActive},
		{"a status that is not configured is allowed", []string{"suspended"}, map[uuid.UUID]string{tenant: "past_due"}, nil},
		{"a tenant with no recorded status is allowed", []string{"suspended"}, map[uuid.UUID]string{}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gate := service.NewTenantStartGate(&fakeTenantStates{status: tc.status}, tc.refused)
			err := gate.Allow(context.Background(), tenant)
			if tc.want == nil {
				assert.NoError(t, err)
			} else {
				assert.ErrorIs(t, err, tc.want)
			}
		})
	}

	t.Run("a failed status read is returned, not read as allowed", func(t *testing.T) {
		gate := service.NewTenantStartGate(&fakeTenantStates{getErr: errors.New("db down")}, []string{"suspended"})
		assert.Error(t, gate.Allow(context.Background(), tenant))
	})

	t.Run("with nothing refused the status is not even read", func(t *testing.T) {
		gate := service.NewTenantStartGate(&fakeTenantStates{getErr: errors.New("db down")}, nil)
		assert.NoError(t, gate.Allow(context.Background(), tenant))
	})
}

func TestInstanceService_Start_ARefusedTenantStartsNothing(t *testing.T) {
	svc, instances, _, _, outbox, temporal, definitions, _ := newInstanceServiceHarness()
	tenantID, versionID := uuid.New(), uuid.New()
	definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))
	svc.Gate = service.NewTenantStartGate(&fakeTenantStates{status: map[uuid.UUID]string{tenantID: "suspended"}}, []string{"suspended"})

	_, err := svc.Start(context.Background(), port.StartInstanceInput{
		TenantID: tenantID, WorkflowVersionID: versionID, SubjectID: "SUBJ-1", BusinessKey: "TND-001",
		ContextJSON: json.RawMessage(`{}`), StartedByUserID: uuid.New(),
	})

	require.ErrorIs(t, err, port.ErrTenantNotActive)
	assert.Empty(t, instances.byID, "no instance row")
	assert.Empty(t, outbox.enqueued)
	assert.Empty(t, temporal.signals)
}

func TestInstanceService_Start_ATenantTheGateAllowsStartsAsBefore(t *testing.T) {
	svc, instances, _, _, _, _, definitions, _ := newInstanceServiceHarness()
	tenantID, versionID := uuid.New(), uuid.New()
	definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))
	svc.Gate = service.NewTenantStartGate(&fakeTenantStates{status: map[uuid.UUID]string{tenantID: "active"}}, []string{"suspended"})

	_, err := svc.Start(context.Background(), port.StartInstanceInput{
		TenantID: tenantID, WorkflowVersionID: versionID, SubjectID: "SUBJ-1", BusinessKey: "TND-001",
		ContextJSON: json.RawMessage(`{}`), StartedByUserID: uuid.New(),
	})

	require.NoError(t, err)
	assert.Len(t, instances.byID, 1)
}

func TestTenantLifecycleReconciler_RecordsTheTenantStatusItAppliesForTheStartGate(t *testing.T) {
	svc, _, _, _, _, _, _ := newTenantLifecycleHarness()
	states := &fakeTenantStates{}
	svc.States = states
	tenantID := uuid.New()
	changedAt := time.Date(2026, 10, 18, 9, 0, 0, 0, time.UTC)

	require.NoError(t, svc.Apply(context.Background(), port.TenantLifecycleInput{
		TenantID: tenantID, Status: "suspended", PreviousStatus: "active", ChangedAt: changedAt,
	}))

	assert.Equal(t, []recordedState{{tenantID, "suspended", changedAt}}, states.recorded)
}

func TestTenantLifecycleReconciler_AFailureToRecordTheStatusIsReturned(t *testing.T) {
	svc, _, _, _, _, _, _ := newTenantLifecycleHarness()
	svc.States = &fakeTenantStates{recErr: errors.New("db down")}
	require.Error(t, svc.Apply(context.Background(), port.TenantLifecycleInput{TenantID: uuid.New(), Status: "suspended", PreviousStatus: "active"}))
}

func TestTenantLifecycleReconciler_TheStatusIsRecordedEvenWhenASignalFails(t *testing.T) {
	svc, instances, _, _, _, _, temporal := newTenantLifecycleHarness()
	states := &fakeTenantStates{}
	svc.States = states
	tenantID := uuid.New()
	inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning, TemporalWorkflowID: "tenant:a", RecordVersion: 1}
	instances.byID[inst.ID] = inst
	temporal.signalFunc = func(context.Context, string, uuid.UUID, string, any) error { return errors.New("temporal unavailable") }

	err := svc.Apply(context.Background(), port.TenantLifecycleInput{TenantID: tenantID, Status: "suspended", PreviousStatus: "active"})

	require.Error(t, err, "the failed signal still fails the event so it is redelivered")
	require.Len(t, states.recorded, 1, "a suspended tenant must not keep starting instances while its pause is retried")
	assert.Equal(t, "suspended", states.recorded[0].status)
}
