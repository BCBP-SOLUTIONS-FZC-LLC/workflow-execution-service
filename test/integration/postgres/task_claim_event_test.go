//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

type tceEligible struct{ port.EligibilityChecker }

func (tceEligible) CheckEligibility(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string) (bool, error) {
	return true, nil
}

type tceDefinitions struct{ planJSON string }

func (d tceDefinitions) GetCompiledWorkflow(_ context.Context, _, versionID uuid.UUID) (*port.CompiledWorkflow, error) {
	return &port.CompiledWorkflow{VersionID: versionID, VersionNumber: 1, Status: "PUBLISHED", IsValid: true, CompiledPlanJSON: d.planJSON}, nil
}

func tceNewDefinitions(t *testing.T, deptID uuid.UUID) tceDefinitions {
	t.Helper()
	plan := &dsl.CompiledPlan{Name: "main", Departments: []dsl.DepartmentDef{{
		ID: "finance", IAMDepartmentID: deptID.String(),
		Stages: []dsl.StageDef{{Type: "userTask", NodeID: "review", Role: "approver"}},
	}}}
	raw, err := json.Marshal(dsl.CompiledCollaboration{SchemaVersion: dsl.CurrentSchemaVersion, MainPlan: plan.Name, Plans: []*dsl.CompiledPlan{plan}})
	require.NoError(t, err)
	return tceDefinitions{planJSON: string(raw)}
}

func TestTaskService_Claim_StoresTheLeadAndTheClaimedEventTogether(t *testing.T) {
	r := w25bNewRace(t)
	svc := &service.TaskService{
		Instances: r.deps.Instances, Tasks: r.tasks, Assignments: r.assignments,
		Transactor: r.deps.Transactor, Outbox: r.outbox, Validator: r.deps.Validator,
		Eligibility: tceEligible{}, Definitions: tceNewDefinitions(t, r.task.DepartmentID),
	}

	claimed, err := svc.Claim(context.Background(), r.tenant, r.task.ID, r.users[0], r.task.RecordVersion)

	require.NoError(t, err)
	assert.Equal(t, r.task.ID, claimed.ID)
	assert.Equal(t, []uuid.UUID{r.users[0]}, r.eventUsers(t, domain.EventWorkflowTaskClaimed, "claimed_by_user_id"))
	lead, err := r.assignments.GetByID(context.Background(), r.tenant, r.held[0].ID)
	require.NoError(t, err)
	assert.True(t, lead.IsLead)
}
