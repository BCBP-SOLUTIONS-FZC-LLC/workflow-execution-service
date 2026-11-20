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

func TestExecute_PauseReceivedWithAStageResultHoldsTheNextStageUntilResume(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	resumeAt := 10 * time.Second
	var secondCreatedAt time.Time
	fakes.failFor = func(name string, in any) error {
		if create, ok := in.(port.CreateTaskInput); ok && create.NodeKey == "sales/S2" {
			secondCreatedAt = env.Now()
		}
		return nil
	}
	collab := wflOneDept("sales",
		dsl.StageDef{Type: "prep", NodeID: "S1"}, dsl.StageDef{Type: "prep", NodeID: "S2"})
	wflRegisterAll(env, fakes, collab)
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	wflSignalAt(env, 5*time.Second, "stage-transition", wflDone("sales", "S1", 1))
	wflSignalAt(env, 5*time.Second, "instance-pause", wflAdminPause(domain.InitiatorAdmin))
	wflSignalAt(env, resumeAt, "instance-resume", wflAdminPause(domain.InitiatorAdmin))
	wflSignalAt(env, resumeAt+2*time.Second, "stage-transition", wflDone("sales", "S2", 1))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	require.False(t, secondCreatedAt.IsZero(), "the second stage ran")
	assert.False(t, secondCreatedAt.Before(start.Add(resumeAt)), "second stage waited for resume")
}
