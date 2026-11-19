package temporal_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestUpdateInstanceNodes_NewKeys_AreWrittenAtTheVersionJustReadInOneTenantTransaction(t *testing.T) {
	t.Parallel()
	rig := w25bNewRig()
	deps := w25bNewDeps(t, rig)
	tenant := uuid.New()
	inst := rig.addInstance(tenant, domain.InstanceStatusRunning)
	inst.CurrentNodeKeys = []string{"sales/old"}
	in := port.UpdateInstanceNodesInput{
		InstanceID: inst.ID.String(), TenantID: tenant.String(),
		NodeKeys: []domain.NodeKey{"sales/review", "ops/approve", "sales/archive"},
	}

	require.NoError(t, deps.UpdateInstanceNodes(context.Background(), in))

	want := []string{"sales/review", "ops/approve", "sales/archive"}
	require.Len(t, rig.nodeWrites, 1)
	assert.Equal(t, w25bNodeWrite{Tenant: tenant, ID: inst.ID, Keys: want, Version: 3}, rig.nodeWrites[0])
	assert.Equal(t, want, rig.instances[inst.ID].CurrentNodeKeys)
	assert.Equal(t, int64(4), rig.instances[inst.ID].RecordVersion)
	assert.Equal(t, []string{"Instances.GetByID", "Instances.UpdateCurrentNodeKeys"}, w25bOps(rig))
	assert.Len(t, rig.txOpened, 1, "the read and the write share one transaction")
	assert.Equal(t, 1, rig.committed)
	assert.Empty(t, rig.outbox, "moving the current nodes announces nothing")
	rig.requireEveryCallScopedTo(t, tenant)

	before := rig.snapshot()
	require.NoError(t, deps.UpdateInstanceNodes(context.Background(), in))
	w25bRequireUntouched(t, rig, before)
	assert.Len(t, rig.nodeWrites, 1, "the retry writes nothing more")
}

func w25bOps(rig *w25bRig) []string {
	out := make([]string, 0, len(rig.calls))
	for _, c := range rig.calls {
		out = append(out, c.Op)
	}
	return out
}

func TestUpdateInstanceNodes_IdsThatAreNotUUIDs_AreRefusedBeforeAnyRepositoryCall(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		tenantID    string
		instanceID  string
		messagePart string
	}{
		{name: "tenant id", tenantID: "bad", instanceID: uuid.NewString(), messagePart: "parse tenant_id"},
		{name: "instance id", tenantID: uuid.NewString(), instanceID: "bad", messagePart: "parse instance_id"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rig := w25bNewRig()
			deps := w25bNewDeps(t, rig)

			err := deps.UpdateInstanceNodes(context.Background(), port.UpdateInstanceNodesInput{
				TenantID: tc.tenantID, InstanceID: tc.instanceID, NodeKeys: []domain.NodeKey{"sales/review"},
			})

			_ = w25bRequireNonRetryable(t, err, "ValidationError", tc.messagePart)
			assert.Empty(t, rig.txOpened)
			assert.Empty(t, rig.calls)
			assert.Empty(t, rig.nodeWrites)
		})
	}
}

func TestUpdateInstanceNodes_InstanceThatIsNotThere_EndsAsANonRetryableNotFound(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		setup       func(rig *w25bRig, tenant uuid.UUID) uuid.UUID
		messagePart string
	}{
		{
			name:        "no such instance",
			setup:       func(_ *w25bRig, _ uuid.UUID) uuid.UUID { return uuid.New() },
			messagePart: "get instance",
		},
		{
			name: "instance of another tenant",
			setup: func(rig *w25bRig, _ uuid.UUID) uuid.UUID {
				return rig.addInstance(uuid.New(), domain.InstanceStatusRunning).ID
			},
			messagePart: "get instance",
		},
		{
			name: "instance gone between the read and the write",
			setup: func(rig *w25bRig, tenant uuid.UUID) uuid.UUID {
				inst := rig.addInstance(tenant, domain.InstanceStatusRunning)
				rig.failOn("Instances.UpdateCurrentNodeKeys", fmt.Errorf("row vanished: %w", domain.ErrNotFound), 0)
				return inst.ID
			},
			messagePart: "update current_node_keys",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rig := w25bNewRig()
			deps := w25bNewDeps(t, rig)
			tenant := uuid.New()
			instanceID := tc.setup(rig, tenant)
			before := rig.snapshot()

			err := deps.UpdateInstanceNodes(context.Background(), port.UpdateInstanceNodesInput{
				TenantID: tenant.String(), InstanceID: instanceID.String(), NodeKeys: []domain.NodeKey{"sales/review"},
			})

			appErr := w25bRequireNonRetryable(t, err, "NotFoundError", tc.messagePart)
			require.ErrorIs(t, err, domain.ErrNotFound)
			assert.NotNil(t, appErr.Unwrap(), "the cause stays attached")
			assert.Equal(t, 1, rig.rolledBack)
			assert.Empty(t, rig.outbox)
			for _, c := range rig.callsTo("Instances.GetByID") {
				assert.Equal(t, tenant, c.Tenant, "the lookup runs for the caller's tenant only")
			}
			w25bRequireUntouched(t, rig, before)
		})
	}
}
