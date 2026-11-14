package temporal_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	outboundtemporal "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestPauseAndResumeInstance_StaleInputVersion_IsIgnoredAndTheVersionReadInTheTransactionIsWritten(t *testing.T) {
	t.Parallel()

	const currentVersion, staleVersion = int64(5), int64(2)
	cases := []struct {
		name      string
		from, to  domain.InstanceStatus
		eventType string
		run       func(d *outboundtemporal.Deps, tenant, instance string) error
	}{
		{"pause", domain.InstanceStatusRunning, domain.InstanceStatusPaused, domain.EventWorkflowInstancePaused,
			func(d *outboundtemporal.Deps, tenant, instance string) error {
				return d.PauseInstance(context.Background(), port.PauseInstanceInput{
					InstanceID: instance, TenantID: tenant, AdminUserID: uuid.NewString(), Initiator: domain.InitiatorAdmin, RecordVersion: staleVersion,
				})
			}},
		{"resume", domain.InstanceStatusPaused, domain.InstanceStatusRunning, domain.EventWorkflowInstanceResumed,
			func(d *outboundtemporal.Deps, tenant, instance string) error {
				return d.ResumeInstance(context.Background(), port.ResumeInstanceInput{
					InstanceID: instance, TenantID: tenant, AdminUserID: uuid.NewString(), Initiator: domain.InitiatorAdmin, RecordVersion: staleVersion,
				})
			}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			world := wkactNewWorld()
			deps, tx := wkactNewDeps(t, world)
			tenant := uuid.New()
			inst := world.addInstance(tenant, tc.from)
			inst.RecordVersion = currentVersion

			require.NoError(t, tc.run(deps, tenant.String(), inst.ID.String()))

			assert.Equal(t, 1, tx.committed)
			assert.Equal(t, []wkactStatusWrite{{Tenant: tenant, ID: inst.ID, Status: tc.to, Version: currentVersion}}, world.statusWrites,
				"the write carries the version read in the transaction, not the stale input")
			assert.Equal(t, tc.to, world.instances[inst.ID].Status)
			assert.Equal(t, currentVersion+1, world.instances[inst.ID].RecordVersion)
			assert.Len(t, world.eventsOfType(tc.eventType), 1)
			world.requireAllCallsForTenant(t, tenant)
		})
	}
}
