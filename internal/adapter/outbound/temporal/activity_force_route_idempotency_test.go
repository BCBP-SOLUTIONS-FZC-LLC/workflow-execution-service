package temporal_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
)

func (w *wkactWorld) appendUnrelatedEvent(tenant uuid.UUID, inst *domain.Instance) {
	payload, _ := json.Marshal(map[string]string{"workflow_instance_id": inst.ID.String()})
	w.outbox = append(w.outbox, events.NewEnvelope[json.RawMessage](domain.EventWorkflowTaskSLAWarning, domain.EventSource, payload,
		events.WithTenantID(tenant.String()), events.WithSubject("instances/"+inst.ID.String())))
}

// A retry of a route whose first attempt committed announces nothing more even
// when another event of the instance landed between the commit and the retry.
func TestRecordForceRoute_RetryAfterCommitWithAnEventInBetween_AnnouncesTheRouteOnce(t *testing.T) {
	t.Parallel()
	world := wkactNewWorld()
	deps, tx := wkactNewDeps(t, world)
	tenant := uuid.New()
	inst := world.addInstance(tenant, domain.InstanceStatusRunning)
	world.addTask(tenant, inst, "sales/review", domain.TaskStatusReady)
	in := forceRouteInput(inst, tenant, uuid.NewString(), "sales/review", "sales/approve")
	in.RecordVersion = 7

	require.NoError(t, deps.RecordForceRoute(context.Background(), in))
	world.appendUnrelatedEvent(tenant, inst)
	require.NoError(t, deps.RecordForceRoute(context.Background(), in))

	assert.Equal(t, 2, tx.committed)
	assert.Len(t, world.eventsOfType(domain.EventWorkflowInstanceForceRouted), 1, "the retry found its own earlier event")
	world.requireAllCallsForTenant(t, tenant)
}

// The same route by the same admin, made again from a later record version, is
// a genuine repeat and is announced again.
func TestRecordForceRoute_ARepeatOfTheSameRouteAtALaterVersion_IsAnnouncedAgain(t *testing.T) {
	t.Parallel()
	world := wkactNewWorld()
	deps, _ := wkactNewDeps(t, world)
	tenant := uuid.New()
	inst := world.addInstance(tenant, domain.InstanceStatusRunning)
	admin := uuid.NewString()
	first := forceRouteInput(inst, tenant, admin, "sales/review", "sales/approve")
	first.RecordVersion = 3
	again := first
	again.RecordVersion = 5

	require.NoError(t, deps.RecordForceRoute(context.Background(), first))
	require.NoError(t, deps.RecordForceRoute(context.Background(), again))

	assert.Len(t, world.eventsOfType(domain.EventWorkflowInstanceForceRouted), 2)
	world.requireAllCallsForTenant(t, tenant)
}
