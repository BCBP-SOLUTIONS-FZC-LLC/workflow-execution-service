package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var errW18aTemporalDown = errors.New("w18a temporal frontend unreachable")

type w18aEligibilityCall struct {
	tenantID uuid.UUID
	requests []port.EligibilityCheckRequest
}

// w18aRecordEligibility makes every assignee eligible and records each batch with the tenant it carried.
func w18aRecordEligibility(eligibility *fakeEligibilityChecker) *[]w18aEligibilityCall {
	var calls []w18aEligibilityCall
	eligibility.batchCheck = func(_ context.Context, tenantID uuid.UUID, requests []port.EligibilityCheckRequest) ([]port.EligibilityResult, error) {
		calls = append(calls, w18aEligibilityCall{tenantID, requests})
		results := make([]port.EligibilityResult, len(requests))
		for i := range results {
			results[i] = port.EligibilityResult{Eligible: true}
		}
		return results, nil
	}
	return &calls
}

// w18aRecordStarts records every workflow start the service sends and lets each succeed with one run id.
func w18aRecordStarts(temporal *fakeTemporalClient) *[]port.StartWorkflowInput {
	var starts []port.StartWorkflowInput
	temporal.startFunc = func(_ context.Context, in port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
		starts = append(starts, in)
		return port.StartWorkflowOutput{TemporalWorkflowID: in.TemporalWorkflowID, TemporalRunID: "run-1"}, nil
	}
	return &starts
}

func w18aStartInput(tenantID, versionID, caller uuid.UUID) port.StartInstanceInput {
	return port.StartInstanceInput{
		TenantID: tenantID, WorkflowVersionID: versionID, SubjectID: "SUBJ-18", BusinessKey: "TND-18", StartedByUserID: caller,
	}
}

func TestInstanceService_Start_PicksThePlanNamedByTheMainPlanNotTheFirstOne(t *testing.T) {
	libDepartment, mainDepartment := uuid.New(), uuid.New()
	libAssignee, mainAssignee := uuid.New(), uuid.New()
	raw, err := json.Marshal(dsl.CompiledCollaboration{
		MainPlan: "main", SchemaVersion: dsl.CurrentSchemaVersion,
		Plans: []*dsl.CompiledPlan{
			{
				Name: "lib", TaskQueue: "lib-queue",
				Departments: []dsl.DepartmentDef{{ID: "legal", Label: "Legal", IAMDepartmentID: libDepartment.String(), Stages: []dsl.StageDef{
					{Type: "userTask", NodeID: "check", Role: "reviewer", DefaultAssignees: []string{libAssignee.String()}},
				}}},
			},
			{
				Name: "main", TaskQueue: "main-queue",
				Departments: []dsl.DepartmentDef{{ID: "finance", Label: "Finance", IAMDepartmentID: mainDepartment.String(), Stages: []dsl.StageDef{
					{Type: "userTask", NodeID: "review", Role: "approver", DefaultAssignees: []string{mainAssignee.String()}},
				}}},
			},
		},
	})
	require.NoError(t, err)
	planJSON := string(raw)
	tenantID, versionID, caller := uuid.New(), uuid.New(), uuid.New()

	t.Run("the departments checked and the queue used are the main plan's", func(t *testing.T) {
		svc, instances, _, _, _, temporal, definitions, eligibility := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, planJSON)
		checks := w18aRecordEligibility(eligibility)
		starts := w18aRecordStarts(temporal)

		got, err := svc.Start(context.Background(), w18aStartInput(tenantID, versionID, caller))

		require.NoError(t, err)
		require.Len(t, *checks, 1)
		assert.Equal(t, tenantID, (*checks)[0].tenantID)
		assert.Equal(t, []port.EligibilityCheckRequest{{NewUserID: mainAssignee, DepartmentID: mainDepartment, RequiredLevel: "approver"}}, (*checks)[0].requests,
			"only the main plan's assignee is checked, in the main plan's department")
		require.Len(t, *starts, 1)
		assert.Equal(t, "main-queue", (*starts)[0].TaskQueue)
		assert.Equal(t, tenantID, (*starts)[0].TenantID)
		assert.Equal(t, "main-queue", instances.byID[got.ID].TaskQueue)
	})

	t.Run("a node key of another plan is not a valid override", func(t *testing.T) {
		svc, instances, _, _, _, temporal, definitions, eligibility := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, planJSON)
		checks := w18aRecordEligibility(eligibility)
		starts := w18aRecordStarts(temporal)
		in := w18aStartInput(tenantID, versionID, caller)
		in.OverrideMap = map[string]uuid.UUID{"legal/check": uuid.New()}

		_, err := svc.Start(context.Background(), in)

		require.ErrorIs(t, err, port.ErrOverrideMapInvalid)
		assert.Empty(t, *checks, "nobody is checked for a refused override")
		assert.Empty(t, instances.byID)
		assert.Empty(t, *starts)
	})
}

func TestInstanceService_Start_ChecksEveryDefaultAssigneeAndSkipsOnlyTheUnparseableOnes(t *testing.T) {
	department := uuid.New()
	first, second, third := uuid.New(), uuid.New(), uuid.New()
	raw, err := json.Marshal(dsl.CompiledCollaboration{
		MainPlan: "main", SchemaVersion: dsl.CurrentSchemaVersion,
		Plans: []*dsl.CompiledPlan{{
			Name: "main", TaskQueue: "wf-queue-default",
			Departments: []dsl.DepartmentDef{{ID: "finance", Label: "Finance", IAMDepartmentID: department.String(), Stages: []dsl.StageDef{
				{Type: "userTask", NodeID: "review", Role: "approver", DefaultAssignees: []string{first.String(), "not-a-uuid", second.String()}},
				{Type: "userTask", NodeID: "sign", Role: "signer", DefaultAssignees: []string{third.String()}},
			}}},
		}},
	})
	require.NoError(t, err)
	svc, instances, _, _, _, _, definitions, eligibility := newInstanceServiceHarness()
	tenantID, versionID := uuid.New(), uuid.New()
	definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, string(raw))
	checks := w18aRecordEligibility(eligibility)

	got, err := svc.Start(context.Background(), w18aStartInput(tenantID, versionID, uuid.New()))

	require.NoError(t, err)
	require.Len(t, *checks, 1, "all assignees are checked in one batch")
	assert.Equal(t, tenantID, (*checks)[0].tenantID)
	assert.Equal(t, []port.EligibilityCheckRequest{
		{NewUserID: first, DepartmentID: department, RequiredLevel: "approver"},
		{NewUserID: second, DepartmentID: department, RequiredLevel: "approver"},
		{NewUserID: third, DepartmentID: department, RequiredLevel: "signer"},
	}, (*checks)[0].requests, "one check per parseable assignee; the unparseable id is skipped, its neighbours are not")
	require.Len(t, instances.byID, 1)
	assert.Equal(t, tenantID, instances.byID[got.ID].TenantID)
}

func TestInstanceService_Start_StartWorkflowFailsOnceThenSucceeds_StartsWithoutAnotherRow(t *testing.T) {
	svc, instances, _, _, outbox, temporal, definitions, _ := newInstanceServiceHarness()
	tenantID, versionID, caller := uuid.New(), uuid.New(), uuid.New()
	definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))
	var attempts []port.StartWorkflowInput
	temporal.startFunc = func(_ context.Context, in port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
		attempts = append(attempts, in)
		if len(attempts) == 1 {
			return port.StartWorkflowOutput{}, errW18aTemporalDown
		}
		return port.StartWorkflowOutput{TemporalWorkflowID: in.TemporalWorkflowID, TemporalRunID: "run-2"}, nil
	}

	got, err := svc.Start(context.Background(), w18aStartInput(tenantID, versionID, caller))

	require.NoError(t, err)
	require.Len(t, attempts, 2, "the failed attempt is retried once, inside the one call")
	assert.Equal(t, attempts[0], attempts[1], "the retry carries the same workflow id, instance id and tenant")
	assert.Equal(t, tenantID, attempts[1].TenantID)
	assert.Equal(t, got.ID, attempts[1].InstanceID)
	assert.Equal(t, "run-2", got.TemporalRunID)
	require.Len(t, instances.byID, 1)
	assert.Equal(t, "run-2", instances.byID[got.ID].TemporalRunID, "the run id of the attempt that worked is recorded")
	assert.Len(t, outbox.enqueued, 1, "one started event")
}
