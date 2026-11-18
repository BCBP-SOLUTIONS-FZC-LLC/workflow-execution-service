package workflow_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
)

// A force-forward of a parked branch whose route record is refused changes
// nothing: the branch stays in the failed list and the park stays open, so the
// same force-forward sent again resolves it.
func TestExecute_ForceForwardOfAParkedBranchWhoseAuditWriteIsRefusedKeepsTheParkOpenForALaterForward(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	auditAttempts := 0
	fakes.failFor = func(name string, in any) error {
		if v, ok := in.(port.CreateTaskInput); ok && v.NodeKey == "billing/Charge" {
			return wflRefuse("create billing/Charge")
		}
		if name == port.ActivityRecordForceRoute {
			auditAttempts++
			if auditAttempts == 1 {
				return wflRefuse(name)
			}
		}
		return nil
	}
	wflRegisterAll(env, fakes, wflForkOnly())
	forward := adminSignalWire{AdminUserID: "admin-7", TargetNodeKey: "billing/Charge", RecordVersion: 9}
	var afterRefusal wfengine.WorkflowStatusQuery
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("warehouse", "Pack", 1))
	wflSignalAt(env, 5*time.Second, "instance-force-forward", forward)
	wflQueryStatusAt(t, env, 7*time.Second, &afterRefusal)
	wflSignalAt(env, 9*time.Second, "instance-force-forward", forward)

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.Equal(t, domain.InstanceStatusDegraded, afterRefusal.Status, "the refused forward left the park open")
	audits := wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute)
	require.Len(t, audits, 2, "one refused attempt, one that landed")
	for _, a := range audits {
		assert.Equal(t, wflTenant, a.TenantID)
		assert.Equal(t, wflInstance, a.InstanceID)
		assert.Equal(t, domain.ForceRouteDirectionForward, a.Direction)
		assert.Equal(t, "billing/Charge", a.TargetNodeID)
	}
	running := 0
	for _, s := range wflStatuses(fakes) {
		if s == domain.InstanceStatusRunning {
			running++
		}
	}
	assert.Equal(t, 1, running, "RUNNING is written once, when the second forward ends the park")
}
