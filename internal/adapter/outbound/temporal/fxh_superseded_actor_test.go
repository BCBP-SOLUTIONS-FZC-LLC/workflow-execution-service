package temporal_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestUpdateTaskStatus_ASystemSupersedeLeavesTheActorOutOfTheEvent(t *testing.T) {
	t.Parallel()
	rig := w22aNewRig()
	tenant := uuid.New()
	inst := rig.addInstance(tenant, domain.InstanceStatusRunning)
	task := rig.addTask(tenant, inst, "sales/review", domain.TaskStatusReady)
	in := port.UpdateTaskStatusInput{TenantID: tenant.String(), TaskID: task.ID.String(), Status: domain.TaskStatusSuperseded, RecordVersion: task.RecordVersion}

	require.NoError(t, w22aNewDeps(t, rig).UpdateTaskStatus(context.Background(), in))

	superseded := rig.eventsOfType(domain.EventWorkflowTaskSuperseded)
	require.Len(t, superseded, 1)
	assert.NotContains(t, w22aRawFields(t, superseded[0]), "actor_user_id", "nobody acted, so no actor is named")
}

func TestRecordForceRoute_AnAdminSupersedeNamesTheAdminInTheEvent(t *testing.T) {
	t.Parallel()
	rig := w22aNewRig()
	tenant, admin := uuid.New(), uuid.New()
	inst := rig.addInstance(tenant, domain.InstanceStatusRunning)
	rig.addTask(tenant, inst, w22aReviewNode, domain.TaskStatusReady)

	require.NoError(t, w22aNewDeps(t, rig).RecordForceRoute(context.Background(), w22aForceRouteInput(tenant, inst, admin, w22aReviewNode)))

	superseded := rig.eventsOfType(domain.EventWorkflowTaskSuperseded)
	require.Len(t, superseded, 1)
	fields := w22aRawFields(t, superseded[0])
	assert.JSONEq(t, `"`+admin.String()+`"`, string(fields["actor_user_id"]))
}
