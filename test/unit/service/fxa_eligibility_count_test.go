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

func fxaStartWithEligibilityAnswer(t *testing.T, answer func(reqs []port.EligibilityCheckRequest) []port.EligibilityResult) error {
	t.Helper()
	svc, instances, _, _, outbox, temporal, definitions, eligibility := newInstanceServiceHarness()
	tenantID, versionID := uuid.New(), uuid.New()
	definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t,
		dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer", DefaultAssignees: []string{uuid.New().String()}},
		dsl.StageDef{Type: "userTask", NodeID: "approve", Role: "approver", DefaultAssignees: []string{uuid.New().String()}}))
	var seenTenant uuid.UUID
	var seenRequests int
	eligibility.batchCheck = func(_ context.Context, tenant uuid.UUID, reqs []port.EligibilityCheckRequest) ([]port.EligibilityResult, error) {
		seenTenant, seenRequests = tenant, len(reqs)
		return answer(reqs), nil
	}
	starts := 0
	temporal.startFunc = func(context.Context, port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
		starts++
		return port.StartWorkflowOutput{}, nil
	}

	_, err := svc.Start(context.Background(), port.StartInstanceInput{
		TenantID: tenantID, WorkflowVersionID: versionID, SubjectID: "SUBJ-1", BusinessKey: "TND-300", StartedByUserID: uuid.New(),
	})

	assert.Equal(t, tenantID, seenTenant)
	assert.Equal(t, 2, seenRequests)
	assert.Empty(t, instances.byID, "no instance is created")
	assert.Zero(t, starts, "no workflow is launched")
	assert.Empty(t, outbox.enqueued, "nothing is published")
	return err
}

func TestInstanceStart_AShortEligibilityAnswerIsAnUpstreamFailure(t *testing.T) {
	err := fxaStartWithEligibilityAnswer(t, func(reqs []port.EligibilityCheckRequest) []port.EligibilityResult {
		return []port.EligibilityResult{{Eligible: true}}
	})

	require.ErrorIs(t, err, port.ErrEligibilityAnswerCount)
}

func TestInstanceStart_ALongEligibilityAnswerIsAnUpstreamFailure(t *testing.T) {
	var err error
	require.NotPanics(t, func() {
		err = fxaStartWithEligibilityAnswer(t, func(reqs []port.EligibilityCheckRequest) []port.EligibilityResult {
			return []port.EligibilityResult{{Eligible: true}, {Eligible: true}, {Eligible: false}}
		})
	})

	require.ErrorIs(t, err, port.ErrEligibilityAnswerCount)
}
