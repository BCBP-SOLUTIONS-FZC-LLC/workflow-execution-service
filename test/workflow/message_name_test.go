package workflow_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

func TestExecute_MessageStageWithAnInvalidNameFailsThePath(t *testing.T) {
	cases := []struct {
		name  string
		stage dsl.StageDef
	}{
		{"send without a name", dsl.StageDef{Type: "send_task", NodeID: "Send"}},
		{"receive without a name", dsl.StageDef{Type: "receive_task", NodeID: "Recv"}},
		{"send with an oversized name", dsl.StageDef{
			Type: "send_task", NodeID: "Send",
			Extras: map[string]string{"message": strings.Repeat("m", 300)},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			fakes := wflNewFakes()
			wflRegisterAll(env, fakes, wflOneDept("A", tc.stage))

			_, err := wflStart(t, env, fakes, "")

			require.Error(t, err)
			assert.Contains(t, err.Error(), "message name")
			assert.Equal(t, []domain.InstanceStatus{domain.InstanceStatusFailed}, wflStatuses(fakes))
		})
	}
}
