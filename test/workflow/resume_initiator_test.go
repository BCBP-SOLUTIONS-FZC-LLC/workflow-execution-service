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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// A resume that carries no initiator does not lift a pause that someone else
// holds: the instance stays PAUSED, the result held during the pause is not
// released and no resume write is made.
func TestExecute_ResumeWithNoInitiatorDoesNotLiftAnOwnedPause(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, wflOneDept("sales", dsl.StageDef{Type: "prep"}))
	wflSignalAt(env, 2*time.Second, "instance-pause", wflAdminPause("tenant_state"))
	wflSignalAt(env, 4*time.Second, "stage-transition", wflDoneStage("sales", "prep", 1))
	wflSignalAt(env, 6*time.Second, "instance-resume", wflAdminPause(""))
	var queried wfengine.WorkflowStatusQuery
	wflQueryStatusAt(t, env, 8*time.Second, &queried)
	wflCancelAt(env, 10*time.Second)

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusTerminated, status)
	assert.Equal(t, domain.InstanceStatusPaused, queried.Status)
	assert.Zero(t, fakes.attemptsOf(port.ActivityResumeInstance), "no resume was written")
	assert.Zero(t, fakes.attemptsOf(port.ActivityCompleteAssignment), "the held result was not released")
	pauses := wflInputs[port.PauseInstanceInput](fakes, port.ActivityPauseInstance)
	require.Len(t, pauses, 1)
	assert.Equal(t, "tenant_state", pauses[0].Initiator)
	assert.Equal(t, wflTenant, pauses[0].TenantID)
	assert.Equal(t, wflInstance, pauses[0].InstanceID)
}
