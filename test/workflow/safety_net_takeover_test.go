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

// A safety-net pause that arrives while an out-of-office pause holds the
// instance takes the pause over: a later out-of-office resume no longer lifts
// it, and only an administrator's resume does.
func TestExecute_SafetyNetPauseTakesOverAnOutOfOfficePause(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, wflOneDept("sales", dsl.StageDef{Type: "prep"}))
	wflSignalAt(env, 2*time.Second, "instance-pause", wflAdminPause(domain.InitiatorOOO))
	wflSignalAt(env, 4*time.Second, "instance-pause", wflAdminPause(domain.InitiatorSafetyNet))
	wflSignalAt(env, 6*time.Second, "instance-resume", wflAdminPause(domain.InitiatorOOO))
	var afterOOOResume wfengine.WorkflowStatusQuery
	wflQueryStatusAt(t, env, 8*time.Second, &afterOOOResume)
	wflSignalAt(env, 10*time.Second, "instance-resume", wflAdminPause(domain.InitiatorAdmin))
	wflSignalAt(env, 12*time.Second, "stage-transition", wflDoneStage("sales", "prep", 1))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.Equal(t, domain.InstanceStatusPaused, afterOOOResume.Status, "the out-of-office resume did not lift the safety-net pause")
	resumes := wflInputs[port.ResumeInstanceInput](fakes, port.ActivityResumeInstance)
	require.Len(t, resumes, 1)
	assert.Equal(t, domain.InitiatorAdmin, resumes[0].Initiator)
	assert.Equal(t, wflTenant, resumes[0].TenantID)
	assert.Equal(t, wflInstance, resumes[0].InstanceID)
}
