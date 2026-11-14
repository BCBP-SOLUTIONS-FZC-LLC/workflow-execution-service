package workflow_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// A top-level force-back records the route (old node, the completed node it
// returns to, its direction, the admin and the version the admin saw) and
// writes the returned-to node as the instance's current node, before the
// department runs again.
func TestExecute_TopLevelForceBackWritesTheReturnedToNodeAsTheCurrentNode(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := wflOneDept("ops",
		dsl.StageDef{Type: "prep", NodeID: "First"},
		dsl.StageDef{Type: "prep", NodeID: "Second"},
	)
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, collab)
	wflSignalAt(env, 1*time.Second, "stage-transition", wflDone("ops", "First", 1))
	wflSignalAt(env, 5*time.Second, "instance-force-back", adminSignalWire{AdminUserID: "admin-6", Reason: "redo", RecordVersion: 11})
	var writesBeforeRevisit [][]domain.NodeKey
	env.RegisterDelayedCallback(func() {
		for _, in := range wflInputs[port.UpdateInstanceNodesInput](fakes, port.ActivityUpdateInstanceNodes) {
			writesBeforeRevisit = append(writesBeforeRevisit, in.NodeKeys)
		}
	}, 10*time.Second)
	wflSignalAt(env, 20*time.Second, "stage-transition", wflDone("ops", "First", 2))
	wflSignalAt(env, 30*time.Second, "stage-transition", wflDone("ops", "Second", 2))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	routes := wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute)
	require.Len(t, routes, 1)
	assert.Equal(t, port.RecordForceRouteInput{
		InstanceID: wflInstance, TenantID: wflTenant, OldNodeKeys: []domain.NodeKey{"ops/Second"},
		TargetNodeID: "ops/First", Direction: domain.ForceRouteDirectionBack, AdminUserID: "admin-6", RecordVersion: 11,
	}, routes[0])
	assert.Equal(t, [][]domain.NodeKey{{"ops/First"}, {"ops/Second"}, {"ops/First"}, {"ops/First"}}, writesBeforeRevisit,
		"after the two tasks' own writes come the force-back's write of the returned-to node, then the rerun First's")
	for _, in := range wflInputs[port.UpdateInstanceNodesInput](fakes, port.ActivityUpdateInstanceNodes) {
		assert.Equal(t, wflTenant, in.TenantID)
		assert.Equal(t, wflInstance, in.InstanceID)
	}
}
