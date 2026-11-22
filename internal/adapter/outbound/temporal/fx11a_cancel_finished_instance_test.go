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

func TestCancelInstance_FinishedInstance_IsLeftAsItIsAndEmitsNoCancelledEvent(t *testing.T) {
	t.Parallel()

	for _, status := range []domain.InstanceStatus{domain.InstanceStatusCompleted, domain.InstanceStatusFailed} {
		t.Run(string(status), func(t *testing.T) {
			t.Parallel()
			world := wkactNewWorld()
			deps, _ := wkactNewDeps(t, world)
			tenant := uuid.New()
			inst := world.addInstance(tenant, status)

			err := deps.CancelInstance(context.Background(), port.CancelInstanceInput{
				InstanceID: inst.ID.String(), TenantID: tenant.String(), AdminUserID: uuid.NewString(),
			})

			require.NoError(t, err)
			assert.Equal(t, status, world.instances[inst.ID].Status)
			assert.Empty(t, world.statusWrites)
			assert.Empty(t, world.eventsOfType(domain.EventWorkflowInstanceCancelled))
		})
	}
}
