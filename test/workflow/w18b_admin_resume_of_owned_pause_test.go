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

func TestExecute_AnAdminResumeLiftsAPauseHeldByANonAdminOwner(t *testing.T) {
	cases := []struct {
		name           string
		pauseInitiator string
	}{
		{"a tenant-state pause", domain.InitiatorTenantState},
		{"a safety-net pause", domain.InitiatorSafetyNet},
		{"an out-of-office pause", domain.InitiatorOOO},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			fakes := wflNewFakes()
			wflRegisterAll(env, fakes, wflOneDept("sales", dsl.StageDef{Type: "prep"}))
			wflSignalAt(env, 2*time.Second, "instance-pause", wflAdminPause(tc.pauseInitiator))
			wflSignalAt(env, 4*time.Second, "stage-transition", wflDoneStage("sales", "prep", 1))
			var pausedBeforeResume wfengine.WorkflowStatusQuery
			wflQueryStatusAt(t, env, 5*time.Second, &pausedBeforeResume)
			wflSignalAt(env, 6*time.Second, "instance-resume", adminSignalWire{
				AdminUserID: "admin-9", Initiator: domain.InitiatorAdmin, RecordVersion: 3,
			})

			status, err := wflStart(t, env, fakes, "")

			require.NoError(t, err)
			assert.Equal(t, domain.InstanceStatusPaused, pausedBeforeResume.Status, "the result held during the pause kept the instance paused")
			assert.Equal(t, domain.InstanceStatusCompleted, status, "the held result was released once the pause was lifted")
			resumes := wflInputs[port.ResumeInstanceInput](fakes, port.ActivityResumeInstance)
			require.Len(t, resumes, 1)
			assert.Equal(t, wflTenant, resumes[0].TenantID)
			assert.Equal(t, wflInstance, resumes[0].InstanceID)
			assert.Equal(t, "admin-9", resumes[0].AdminUserID)
			assert.Equal(t, domain.InitiatorAdmin, resumes[0].Initiator, "resume activity input names the admin, not the pause's owner")
			pauses := wflInputs[port.PauseInstanceInput](fakes, port.ActivityPauseInstance)
			require.Len(t, pauses, 1)
			assert.Equal(t, tc.pauseInitiator, pauses[0].Initiator)
			assert.Equal(t, 1, fakes.attemptsOf(port.ActivityCompleteAssignment))
		})
	}
}
