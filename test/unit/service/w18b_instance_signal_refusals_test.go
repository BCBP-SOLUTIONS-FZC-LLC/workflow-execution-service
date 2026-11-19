package service_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

type w18bLookup struct{ tenantID, instanceID uuid.UUID }

type w18bSignalInstances struct {
	port.InstanceRepository
	rows    []*domain.Instance
	lookups []w18bLookup
}

func (r *w18bSignalInstances) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	r.lookups = append(r.lookups, w18bLookup{tenantID, id})
	for _, inst := range r.rows {
		if inst.TenantID == tenantID && inst.ID == id {
			return inst, nil
		}
	}
	return nil, domain.ErrNotFound
}

type w18bSentSignal struct {
	workflowID string
	instanceID uuid.UUID
	name       string
	payload    any
}

type w18bSignalTemporal struct {
	port.TemporalClient
	signals []w18bSentSignal
}

func (c *w18bSignalTemporal) SignalWorkflow(_ context.Context, workflowID string, instanceID uuid.UUID, name string, payload any) error {
	c.signals = append(c.signals, w18bSentSignal{workflowID, instanceID, name, payload})
	return nil
}

func TestInstanceService_Pause_AnInstanceTheTenantDoesNotHaveIsNotFoundAndNothingIsSignalled(t *testing.T) {
	t.Parallel()

	tenantID, otherTenantID := uuid.New(), uuid.New()
	foreign := &domain.Instance{ID: uuid.New(), TenantID: otherTenantID, Status: domain.InstanceStatusRunning, RecordVersion: 4, TemporalWorkflowID: "wf-foreign"}
	cases := []struct {
		name       string
		instanceID uuid.UUID
	}{
		{"an id no tenant has", uuid.New()},
		{"an instance of another tenant", foreign.ID},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			instances := &w18bSignalInstances{rows: []*domain.Instance{foreign}}
			temporal := &w18bSignalTemporal{}
			svc := &service.InstanceService{Instances: instances, Temporal: temporal}

			err := svc.Pause(context.Background(), tenantID, tc.instanceID, uuid.New(), "audit", 4)

			require.ErrorIs(t, err, port.ErrInstanceNotFound)
			assert.Equal(t, []w18bLookup{{tenantID, tc.instanceID}}, instances.lookups, "one lookup, scoped to the caller's tenant")
			assert.Empty(t, temporal.signals)
		})
	}
}

func TestInstanceService_Pause_AVersionOtherThanTheCurrentOneIsAConflictWhetherLowerOrHigher(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	const current = int64(7)
	cases := []struct {
		name    string
		status  domain.InstanceStatus
		version int64
	}{
		{"a lower version", domain.InstanceStatusRunning, current - 1},
		{"a higher version", domain.InstanceStatusRunning, current + 1},
		{"a higher version on an instance whose state the pause would refuse", domain.InstanceStatusPaused, current + 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: tc.status, RecordVersion: current, TemporalWorkflowID: "wf-" + uuid.NewString()}
			instances := &w18bSignalInstances{rows: []*domain.Instance{inst}}
			temporal := &w18bSignalTemporal{}
			svc := &service.InstanceService{Instances: instances, Temporal: temporal}

			err := svc.Pause(context.Background(), tenantID, inst.ID, uuid.New(), "audit", tc.version)

			require.ErrorIs(t, err, port.ErrRecordVersionConflict)
			assert.NotErrorIs(t, err, port.ErrInvalidInstanceState, "the version is checked before the state")
			assert.Equal(t, []w18bLookup{{tenantID, inst.ID}}, instances.lookups)
			assert.Empty(t, temporal.signals, "a conflict sends no signal")
		})
	}

	t.Run("control: the current version sends the one signal", func(t *testing.T) {
		t.Parallel()

		inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning, RecordVersion: current, TemporalWorkflowID: "wf-" + uuid.NewString()}
		instances := &w18bSignalInstances{rows: []*domain.Instance{inst}}
		temporal := &w18bSignalTemporal{}
		svc := &service.InstanceService{Instances: instances, Temporal: temporal}

		require.NoError(t, svc.Pause(context.Background(), tenantID, inst.ID, uuid.New(), "audit", current))

		require.Len(t, temporal.signals, 1)
		assert.Equal(t, inst.TemporalWorkflowID, temporal.signals[0].workflowID)
		assert.Equal(t, inst.ID, temporal.signals[0].instanceID)
		assert.Equal(t, port.SignalInstancePause, temporal.signals[0].name)
	})
}
