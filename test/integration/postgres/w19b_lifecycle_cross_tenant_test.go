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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

type w19bTemporalCall struct{ op, workflowID string }

type w19bTemporalRecorder struct {
	port.TemporalClient
	calls []w19bTemporalCall
}

func (r *w19bTemporalRecorder) SignalWorkflow(_ context.Context, workflowID string, _ uuid.UUID, _ string, _ any) error {
	r.calls = append(r.calls, w19bTemporalCall{"signal", workflowID})
	return nil
}

func (r *w19bTemporalRecorder) TerminateWorkflow(_ context.Context, workflowID, _, _ string) error {
	r.calls = append(r.calls, w19bTemporalCall{"terminate", workflowID})
	return nil
}

type w19bPlanReader struct{ reads []uuid.UUID }

func (d *w19bPlanReader) GetCompiledWorkflow(_ context.Context, tenantID, _ uuid.UUID) (*port.CompiledWorkflow, error) {
	d.reads = append(d.reads, tenantID)
	return nil, port.ErrDefinitionUnavailable
}

func TestInstanceService_AdminLifecycleCalls_OfAnotherTenantsInstance_AreNotFoundAndChangeNothing(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instances := postgres.NewInstanceRepo(appPool)
	ctx := context.Background()

	routes := []struct {
		name string
		call func(svc *service.InstanceService, tenantID, instanceID uuid.UUID, version int64) error
	}{
		{"terminate", func(svc *service.InstanceService, tenantID, instanceID uuid.UUID, _ int64) error {
			return svc.Terminate(ctx, tenantID, instanceID, uuid.New(), "cleanup")
		}},
		{"force-forward", func(svc *service.InstanceService, tenantID, instanceID uuid.UUID, version int64) error {
			return svc.ForceForward(ctx, tenantID, instanceID, uuid.New(), "finance/Receive_Bid", version)
		}},
		{"force-back", func(svc *service.InstanceService, tenantID, instanceID uuid.UUID, version int64) error {
			return svc.ForceBack(ctx, tenantID, instanceID, uuid.New(), "", version)
		}},
	}
	for _, tc := range routes {
		t.Run(tc.name, func(t *testing.T) {
			owner, other := uuid.New(), uuid.New()
			inst := newInstance(owner, time.Now().UTC())
			require.NoError(t, instances.Create(ctx, inst))
			stored, err := instances.GetByID(ctx, owner, inst.ID)
			require.NoError(t, err, "control: the owning tenant reads its own instance")
			temporal, definitions := &w19bTemporalRecorder{}, &w19bPlanReader{}
			svc := &service.InstanceService{
				Instances: instances, Tasks: postgres.NewTaskRepo(appPool), Assignments: postgres.NewTaskAssignmentRepo(appPool),
				Transactor: postgres.NewTransactor(appPool), Temporal: temporal, Definitions: definitions,
			}

			err = tc.call(svc, other, inst.ID, stored.RecordVersion)

			require.ErrorIs(t, err, port.ErrInstanceNotFound)
			assert.Empty(t, temporal.calls, "the other tenant's workflow is neither signalled nor closed")
			assert.Empty(t, definitions.reads, "the other tenant's plan is not read")
			after, err := instances.GetByID(ctx, owner, inst.ID)
			require.NoError(t, err)
			assert.Equal(t, stored.Status, after.Status)
			assert.Equal(t, stored.RecordVersion, after.RecordVersion, "the row was not written")
		})
	}
}
