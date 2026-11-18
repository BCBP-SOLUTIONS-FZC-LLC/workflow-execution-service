package workflow_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// When the rerun of the step before the fork, started by a defer inside a
// park, ends in an error other than a path end, the run fails with that error:
// the instance is written FAILED straight after DEGRADED, never RUNNING, and
// nothing runs after the failed rerun.
func TestExecute_ParkRerunThatFailsFailsTheRunWithoutWritingRunning(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	billingRefused := false
	intakeCreates := 0
	fakes.failFor = func(name string, in any) error {
		v, ok := in.(port.CreateTaskInput)
		if !ok {
			return nil
		}
		switch v.NodeKey {
		case "billing/B1":
			if !billingRefused {
				billingRefused = true
				return wflRefuse(name)
			}
		case "intake/A1":
			intakeCreates++
			if intakeCreates == 2 {
				return wflRefuse(name)
			}
		}
		return nil
	}
	wflRegisterAll(env, fakes, wflForkAfterIntake())
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("intake", "A1", 1))
	wflSignalAt(env, 4*time.Second, "stage-transition", wflDone("warehouse", "Pack", 1))
	wflSignalAt(env, 6*time.Second, "instance-force-back", adminSignalWire{AdminUserID: "admin-3", TargetNodeKey: "billing/B1", RecordVersion: 5})
	wflDeferTask(env, "defer-1", 8*time.Second, "billing/B1", &updateAnswer{})

	_, err := wflStart(t, env, fakes, "")

	require.Error(t, err)
	assert.Contains(t, err.Error(), port.ActivityCreateTask, "the rerun's own failure is the run's failure")
	assert.Equal(t, 2, intakeCreates, "the rerun tried to start the step once")
	creates := wflInputs[port.CreateTaskInput](fakes, port.ActivityCreateTask)
	require.NotEmpty(t, creates)
	last := creates[len(creates)-1]
	assert.Equal(t, domain.NodeKey("intake/A1"), last.NodeKey, "the refused rerun task is the last task asked for")
	assert.Equal(t, wflTenant, last.TenantID)
	assert.Equal(t, wflInstance, last.InstanceID)
	assert.Equal(t, []domain.InstanceStatus{domain.InstanceStatusDegraded, domain.InstanceStatusFailed}, wflStatuses(fakes))
	for _, in := range wflInputs[port.UpdateInstanceStatusInput](fakes, port.ActivityUpdateInstanceStatus) {
		assert.Equal(t, wflTenant, in.TenantID)
		assert.Equal(t, wflInstance, in.InstanceID)
	}
}
