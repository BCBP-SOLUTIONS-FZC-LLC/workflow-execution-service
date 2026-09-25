package service_test

import (
	"context"
	"testing"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl/dsltest"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// Node keys and IAM departments in workflow-models' dsltest.LibraryCalls, once
// its calls are expanded.
const (
	goldenEngIAM    = "2200a5ae-2466-54f7-a1d8-243106e05705"
	goldenOpsIAM    = "92fd92cf-4b2c-58dc-bf98-f3d0af729876"
	goldenReviewer  = "550e8400-e29b-41d4-a716-446655440000"
	goldenOpsReview = "018f2d3c-0000-7000-8000-0000000000b2"
	goldenCheckEng  = "CA_Eng::Review_Check::Process_Check@v1/Check_Task"
	goldenCheckOps  = "CA_Ops::Review_Check::Process_Check@v1/Check_Task"
)

func goldenStart(overrides map[string]uuid.UUID) port.StartInstanceInput {
	return port.StartInstanceInput{
		TenantID: uuid.New(), WorkflowVersionID: uuid.New(), SubjectID: "SUBJ-1", BusinessKey: "GOLDEN",
		StartedByUserID: uuid.New(), OverrideMap: overrides,
	}
}

func TestInstanceService_Start_LibraryCalls(t *testing.T) {
	t.Run("a module task naming nobody refuses the start, named by its call-scoped node key", func(t *testing.T) {
		svc, _, _, _, _, _, definitions, _ := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), uuid.New(), string(dsltest.LibraryCalls()))

		_, err := svc.Start(context.Background(), goldenStart(nil))
		var ineligible *port.AssigneeIneligibleError
		require.ErrorAs(t, err, &ineligible)
		assert.ElementsMatch(t, []string{goldenCheckEng, goldenCheckOps}, ineligible.Nodes)
	})

	t.Run("override_map names module tasks, and each call's tasks are checked in the caller's department", func(t *testing.T) {
		svc, _, _, _, _, _, definitions, eligibility := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), uuid.New(), string(dsltest.LibraryCalls()))
		engChecker, opsChecker := uuid.New(), uuid.New()
		var got []port.EligibilityCheckRequest
		eligibility.batchCheck = func(_ context.Context, reqs []port.EligibilityCheckRequest, _ uuid.UUID) ([]port.EligibilityResult, error) {
			got = reqs
			results := make([]port.EligibilityResult, len(reqs))
			for i := range results {
				results[i] = port.EligibilityResult{Eligible: true}
			}
			return results, nil
		}

		_, err := svc.Start(context.Background(), goldenStart(map[string]uuid.UUID{goldenCheckEng: engChecker, goldenCheckOps: opsChecker}))
		require.NoError(t, err)
		assert.ElementsMatch(t, []port.EligibilityCheckRequest{
			{NewUserID: uuid.MustParse(goldenReviewer), DepartmentID: uuid.MustParse(goldenEngIAM), RequiredLevel: "reviewer"},
			{NewUserID: engChecker, DepartmentID: uuid.MustParse(goldenEngIAM), RequiredLevel: "checker"},
			{NewUserID: uuid.MustParse(goldenOpsReview), DepartmentID: uuid.MustParse(goldenOpsIAM), RequiredLevel: "reviewer"},
			{NewUserID: opsChecker, DepartmentID: uuid.MustParse(goldenOpsIAM), RequiredLevel: "checker"},
		}, got)
	})

	t.Run("a plan that cannot be expanded is refused as an invalid version", func(t *testing.T) {
		svc, _, _, _, _, _, definitions, _ := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), uuid.New(),
			`{"main_plan":"main","schema_version":1,"plans":[{"name":"main","departments":[],"execution":{"steps":[{"call_plan":{"node_id":"CA_1","plan":"Nowhere"}}]}}]}`)

		_, err := svc.Start(context.Background(), goldenStart(nil))
		require.ErrorIs(t, err, port.ErrVersionInvalid)
	})
}
