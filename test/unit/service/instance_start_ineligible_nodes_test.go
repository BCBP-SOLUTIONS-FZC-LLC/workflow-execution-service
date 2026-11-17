package service_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

func TestInstanceStart_ANodeWithSeveralIneligibleAssigneesIsNamedOnce(t *testing.T) {
	svc, _, _, _, _, _, definitions, eligibility := newInstanceServiceHarness()
	definitions.resp = publishedCompiledWorkflow(uuid.New(), uuid.New(), compiledPlanJSON(t,
		dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer", DefaultAssignees: []string{uuid.New().String(), uuid.New().String()}},
		dsl.StageDef{Type: "userTask", NodeID: "approve", Role: "approver", DefaultAssignees: []string{uuid.New().String()}}))
	eligibility.batchCheck = func(_ context.Context, _ uuid.UUID, reqs []port.EligibilityCheckRequest) ([]port.EligibilityResult, error) {
		results := make([]port.EligibilityResult, len(reqs))
		return results, nil
	}

	_, err := svc.Start(context.Background(), port.StartInstanceInput{
		TenantID: uuid.New(), WorkflowVersionID: uuid.New(), SubjectID: "SUBJ-1", BusinessKey: "TND-200", StartedByUserID: uuid.New(),
	})

	var ineligible *port.AssigneeIneligibleError
	require.ErrorAs(t, err, &ineligible)
	assert.Equal(t, []string{"finance/review", "finance/approve"}, ineligible.Nodes)
}
