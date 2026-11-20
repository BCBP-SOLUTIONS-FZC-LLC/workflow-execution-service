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

func TestExecute_BoundaryMessageWithAnInvalidNameFailsThePath(t *testing.T) {
	cases := []struct{ name, message string }{
		{"blank name", "   "},
		{"oversized name", strings.Repeat("m", 300)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			fakes := wflNewFakes()
			wflRegisterAll(env, fakes, wflOneDept("A", dsl.StageDef{
				Type: "prep", NodeID: "Prepare",
				BoundaryMessage: &dsl.MessagePath{MessageName: tc.message, Interrupting: true},
			}))

			_, err := wflStart(t, env, fakes, "")

			require.Error(t, err)
			assert.Contains(t, err.Error(), "message name")
			assert.Equal(t, []domain.InstanceStatus{domain.InstanceStatusFailed}, wflStatuses(fakes))
		})
	}
}
