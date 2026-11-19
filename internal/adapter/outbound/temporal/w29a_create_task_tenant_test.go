package temporal_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

func TestCreateTask_TheTransactionOpensUnderTheInputTenant_AndAnotherTenantsInstanceIsNotFound(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name          string
		callerIsOwner bool
	}{
		{name: "the tenant that owns the instance", callerIsOwner: true},
		{name: "another tenant naming the instance", callerIsOwner: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rig := w25bNewRig()
			deps := w25bNewDeps(t, rig)
			owner, caller := uuid.New(), uuid.New()
			if tc.callerIsOwner {
				caller = owner
			}
			inst := rig.addInstance(owner, domain.InstanceStatusRunning)
			in := w25bCreateInput(t, inst, caller, dsl.StageDef{Type: "review", DefaultAssignees: []string{uuid.NewString()}})

			out, err := deps.CreateTask(context.Background(), in)

			require.NotEmpty(t, rig.txOpened)
			for _, tx := range rig.txOpened {
				assert.True(t, tx.GUCSet, "the tenant is on the context before the transaction opens")
				assert.Equal(t, caller.String(), tx.Tenant, "the input tenant, not the instance's owner")
			}
			for _, c := range rig.calls {
				assert.True(t, c.InTx, "%s ran outside the transaction", c.Op)
				assert.Equal(t, caller.String(), c.GUC, "%s ran under another tenant", c.Op)
				assert.Equal(t, caller, c.Tenant, "%s was given another tenant", c.Op)
			}
			if tc.callerIsOwner {
				require.NoError(t, err)
				assert.Len(t, rig.tasks, 1)
				assert.NotEmpty(t, out.TaskID)
				assert.Len(t, rig.eventsOfType(domain.EventWorkflowTaskCreated), 1)
				return
			}
			_ = w25bRequireNonRetryable(t, err, "NotFoundError", "get instance")
			require.ErrorIs(t, err, domain.ErrNotFound)
			assert.Empty(t, out.TaskID)
			assert.Empty(t, rig.callsTo("Tasks.Create"), "nothing is written for another tenant's instance")
			assert.Empty(t, rig.callsTo("Assignments.Create"))
			assert.Empty(t, rig.tasks)
			assert.Empty(t, rig.assignments)
			assert.Empty(t, rig.outbox)
			assert.Equal(t, 1, rig.rolledBack)
			assert.Equal(t, domain.InstanceStatusRunning, rig.instances[inst.ID].Status)
		})
	}
}
