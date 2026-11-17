package temporal_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
)

// ExistsByID answers from the world's stored events and records the lookup, so a
// test sees which id the activity asked for.
func (o wkactOutbox) ExistsByID(_ context.Context, tenant, id uuid.UUID) (bool, error) {
	o.w.record("Outbox.ExistsByID", tenant, id)
	for _, env := range o.w.outbox {
		if env.ID == id.String() && env.TenantID == tenant.String() {
			return true, nil
		}
	}
	return false, nil
}

func TestRecordForceRoute_LooksItsOwnEventUpByIdWithoutListingTheInstance(t *testing.T) {
	t.Parallel()
	world := wkactNewWorld()
	deps, _ := wkactNewDeps(t, world)
	tenant := uuid.New()
	inst := world.addInstance(tenant, domain.InstanceStatusRunning)
	in := forceRouteInput(inst, tenant, uuid.NewString(), "sales/review", "sales/approve")
	in.RecordVersion = 7

	require.NoError(t, deps.RecordForceRoute(context.Background(), in))
	announced := world.eventsOfType(domain.EventWorkflowInstanceForceRouted)
	require.Len(t, announced, 1)
	require.NoError(t, deps.RecordForceRoute(context.Background(), in))

	lookups := world.callsTo("Outbox.ExistsByID")
	require.Len(t, lookups, 2, "one lookup per attempt")
	for _, l := range lookups {
		assert.Equal(t, announced[0].ID, l.ID.String(), "the lookup names the deterministic event id")
		assert.Equal(t, tenant, l.Tenant)
	}
	assert.Empty(t, world.callsTo("Outbox.ListByInstance"), "the instance's events are not paged through")
	assert.Len(t, world.eventsOfType(domain.EventWorkflowInstanceForceRouted), 1, "the retry announces nothing more")
}
