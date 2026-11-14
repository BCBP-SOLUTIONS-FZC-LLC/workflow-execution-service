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
	temporaladapter "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

// wflSignalRecorder is a Temporal client that only records the signals it is
// asked to send.
type wflSignalRecorder struct {
	port.TemporalClient
	signals []wflSignal
}

type wflSignal struct {
	workflowID string
	instanceID uuid.UUID
	name       string
}

func (r *wflSignalRecorder) SignalWorkflow(_ context.Context, workflowID string, instanceID uuid.UUID, name string, _ any) error {
	r.signals = append(r.signals, wflSignal{workflowID: workflowID, instanceID: instanceID, name: name})
	return nil
}

// A force-back that repeats the record version of the first one, after the
// workflow has applied that first force-back (its write of the instance's new
// node keys moves record_version), is refused with a version conflict and
// signals nothing; the same request with the new version is accepted.
func TestInstanceService_RepeatedForceBackWithTheOldRecordVersionConflictsAfterTheWorkflowApplied(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instances := postgres.NewInstanceRepo(appPool)
	recorder := &wflSignalRecorder{}
	svc := &service.InstanceService{Instances: instances, Tasks: postgres.NewTaskRepo(appPool), Temporal: recorder}
	workflowSide := &temporaladapter.Deps{Instances: instances}
	ctx := context.Background()

	tenantID, actorID, id := uuid.New(), uuid.New(), uuid.New()
	started := time.Now().UTC()
	inst := &domain.Instance{
		ID: id, TenantID: tenantID, WorkflowID: uuid.New(), WorkflowVersionID: uuid.New(),
		SubjectID: "subj-" + id.String(), BusinessKey: "bk-" + id.String(), TemporalWorkflowID: "wf-" + id.String(),
		Status: domain.InstanceStatusDegraded, CurrentNodeKeys: []string{"billing/prep"}, TaskQueue: "wf-queue-default",
		StartedByUserID: uuid.New(), StartedAt: &started,
	}
	require.NoError(t, instances.Create(ctx, inst))
	firstVersion := inst.RecordVersion

	require.NoError(t, svc.ForceBack(ctx, tenantID, id, actorID, "billing/prep", firstVersion))
	require.Equal(t, []wflSignal{{workflowID: "wf-" + id.String(), instanceID: id, name: port.SignalInstanceForceBack}}, recorder.signals)

	require.NoError(t, workflowSide.UpdateInstanceNodes(ctx, port.UpdateInstanceNodesInput{
		InstanceID: id.String(), TenantID: tenantID.String(), NodeKeys: []domain.NodeKey{"billing/prep"},
	}))

	err := svc.ForceBack(ctx, tenantID, id, actorID, "billing/prep", firstVersion)
	require.ErrorIs(t, err, port.ErrRecordVersionConflict)
	assert.Len(t, recorder.signals, 1, "the conflicting repeat sent no second signal")

	stored, err := instances.GetByID(ctx, tenantID, id)
	require.NoError(t, err)
	assert.Equal(t, firstVersion+1, stored.RecordVersion)
	require.NoError(t, svc.ForceBack(ctx, tenantID, id, actorID, "billing/prep", stored.RecordVersion))
	assert.Len(t, recorder.signals, 2, "with the current version the force-back is accepted")
}
