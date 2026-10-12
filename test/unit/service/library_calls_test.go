package service_test

import (
	"context"
	"encoding/json"
	"maps"
	"slices"
	"testing"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl/dsltest"
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
	goldenReviewer  = "018f2d3c-0000-7000-8000-0000000000a1"
	goldenOpsReview = "018f2d3c-0000-7000-8000-0000000000b2"
	goldenEngCheck  = "018f2d3c-0000-7000-8000-0000000000c1"
	goldenOpsCheck  = "018f2d3c-0000-7000-8000-0000000000c2"
	goldenCheckEng  = "CA_Eng::Review_Check::Process_Check/Check_Task"
	goldenCheckOps  = "CA_Ops::Review_Check::Process_Check/Check_Task"
)

func goldenStart(overrides map[string]uuid.UUID) port.StartInstanceInput {
	return port.StartInstanceInput{
		TenantID: uuid.New(), WorkflowVersionID: uuid.New(), SubjectID: "SUBJ-1", BusinessKey: "GOLDEN",
		StartedByUserID: uuid.New(), OverrideMap: overrides,
	}
}

// goldenHumanNodeKeys is dsl.NodeKey over every human task of the golden's
// expanded main plan.
func goldenHumanNodeKeys(t *testing.T) []string {
	t.Helper()
	var collab dsl.CompiledCollaboration
	require.NoError(t, json.Unmarshal(dsltest.LibraryCalls(), &collab))
	expanded, err := dsl.ExpandCalls(&collab, domain.MaxPlanStages)
	require.NoError(t, err)
	var keys []string
	for _, d := range expanded.Plans[0].Departments {
		for i := range d.Stages {
			if d.Stages[i].CreatesHumanTask() {
				keys = append(keys, dsl.NodeKey(d.ID, &d.Stages[i]))
			}
		}
	}
	return keys
}

func TestInstanceService_Start_LibraryCalls(t *testing.T) {
	t.Run("each call's defaults, a nested module's by path included, are checked in the caller's department", func(t *testing.T) {
		svc, _, _, _, _, _, definitions, eligibility := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), uuid.New(), string(dsltest.LibraryCalls()))
		var got []port.EligibilityCheckRequest
		eligibility.batchCheck = func(_ context.Context, reqs []port.EligibilityCheckRequest, _ uuid.UUID) ([]port.EligibilityResult, error) {
			got = reqs
			results := make([]port.EligibilityResult, len(reqs))
			for i := range results {
				results[i] = port.EligibilityResult{Eligible: reqs[i].NewUserID != uuid.MustParse(goldenOpsCheck)}
			}
			return results, nil
		}

		_, err := svc.Start(context.Background(), goldenStart(nil))
		var ineligible *port.AssigneeIneligibleError
		require.ErrorAs(t, err, &ineligible)
		assert.Equal(t, []string{goldenCheckOps}, ineligible.Nodes, "the refusal names the call-scoped node key")
		assert.ElementsMatch(t, []port.EligibilityCheckRequest{
			{NewUserID: uuid.MustParse(goldenReviewer), DepartmentID: uuid.MustParse(goldenEngIAM), RequiredLevel: "reviewer"},
			{NewUserID: uuid.MustParse(goldenEngCheck), DepartmentID: uuid.MustParse(goldenEngIAM), RequiredLevel: "checker"},
			{NewUserID: uuid.MustParse(goldenOpsReview), DepartmentID: uuid.MustParse(goldenOpsIAM), RequiredLevel: "reviewer"},
			{NewUserID: uuid.MustParse(goldenOpsCheck), DepartmentID: uuid.MustParse(goldenOpsIAM), RequiredLevel: "checker"},
		}, got)
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

	// Definition's node read keys each human task with dsl.NodeKey over this
	// same expanded plan (definition LLD §3.3.12.2), so every key it returns
	// must be one override_map accepts and the eligibility check covers.
	t.Run("override_map accepts every key dsl.NodeKey gives the expanded plan's human tasks", func(t *testing.T) {
		overrides := map[string]uuid.UUID{}
		for _, key := range goldenHumanNodeKeys(t) {
			overrides[key] = uuid.New()
		}
		require.Len(t, overrides, 4)

		svc, _, _, _, _, _, definitions, eligibility := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), uuid.New(), string(dsltest.LibraryCalls()))
		var checked []uuid.UUID
		eligibility.batchCheck = func(_ context.Context, reqs []port.EligibilityCheckRequest, _ uuid.UUID) ([]port.EligibilityResult, error) {
			results := make([]port.EligibilityResult, len(reqs))
			for i := range reqs {
				checked = append(checked, reqs[i].NewUserID)
				results[i] = port.EligibilityResult{Eligible: true}
			}
			return results, nil
		}

		_, err := svc.Start(context.Background(), goldenStart(overrides))
		require.NoError(t, err)
		assert.ElementsMatch(t, slices.Collect(maps.Values(overrides)), checked, "every override is the user checked for its node")
	})

	t.Run("a plan that cannot be expanded is refused as an invalid version", func(t *testing.T) {
		svc, _, _, _, _, _, definitions, _ := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), uuid.New(),
			`{"main_plan":"main","schema_version":1,"plans":[{"name":"main","departments":[],"execution":{"steps":[{"call_plan":{"node_id":"CA_1","plan":"Nowhere"}}]}}]}`)

		_, err := svc.Start(context.Background(), goldenStart(nil))
		require.ErrorIs(t, err, port.ErrVersionInvalid)
	})

	// An empty target is read as the end of a path only because every plan
	// that expands marks such a boundary Terminates; one that does not is
	// refused here, before any instance runs it.
	t.Run("a boundary that neither targets a department nor terminates is refused as an invalid version", func(t *testing.T) {
		svc, _, _, _, _, _, definitions, _ := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), uuid.New(),
			`{"main_plan":"main","schema_version":1,"plans":[{"name":"main","departments":[{"id":"A","label":"A","stages":[{"type":"review","activity":"review","node_id":"T1","role":"r","default_assignees":["`+goldenReviewer+`"],"boundary_timer":{"duration":"1h","interrupting":true}}]}],"execution":{"steps":[{"sequential":["A"]}]}}]}`)

		_, err := svc.Start(context.Background(), goldenStart(nil))
		require.ErrorIs(t, err, port.ErrVersionInvalid)
	})
}
