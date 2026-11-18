//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

type w17aPlanRead struct{ tenantID, versionID uuid.UUID }

// w17aPlans serves one compiled plan for any version and records the tenant of each read.
type w17aPlans struct {
	resp  *port.CompiledWorkflow
	reads []w17aPlanRead
}

func (p *w17aPlans) GetCompiledWorkflow(_ context.Context, tenantID, versionID uuid.UUID) (*port.CompiledWorkflow, error) {
	p.reads = append(p.reads, w17aPlanRead{tenantID: tenantID, versionID: versionID})
	return p.resp, nil
}

// w17aEligible answers "eligible" to every check and records the tenant of each batch.
type w17aEligible struct{ tenants []uuid.UUID }

func (e *w17aEligible) CheckEligibility(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string) (bool, error) {
	return true, nil
}

func (e *w17aEligible) CheckEligibilityBatch(_ context.Context, tenantID uuid.UUID, requests []port.EligibilityCheckRequest) ([]port.EligibilityResult, error) {
	e.tenants = append(e.tenants, tenantID)
	out := make([]port.EligibilityResult, len(requests))
	for i := range out {
		out[i].Eligible = true
	}
	return out, nil
}

func TestDelegationReroute_DelegatorHoldsRowsInTwoTenants_OnlyTheEventTenantsRowsMove(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instances := postgres.NewInstanceRepo(appPool)
	tasks := postgres.NewTaskRepo(appPool)
	assignments := postgres.NewTaskAssignmentRepo(appPool)
	outbox := postgres.NewOutboxRepo(appPool)
	validator, err := eventbus.NewSchemaValidator()
	require.NoError(t, err)
	ctx := context.Background()

	deptID := uuid.New()
	planJSON, err := json.Marshal(dsl.CompiledCollaboration{
		SchemaVersion: dsl.CurrentSchemaVersion,
		MainPlan:      "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main", TaskQueue: "wf-queue-default",
			Departments: []dsl.DepartmentDef{{
				ID: "finance", Label: "Finance", IAMDepartmentID: deptID.String(),
				Stages: []dsl.StageDef{{Type: "userTask", NodeID: "review", Role: "reviewer"}},
			}},
		}},
	})
	require.NoError(t, err)

	tenantA, tenantB := uuid.New(), uuid.New()
	delegator, delegate, delegationID := uuid.New(), uuid.New(), uuid.New()
	type seeded struct {
		task       *domain.Task
		assignment *domain.TaskAssignment
	}
	seed := func(tenantID uuid.UUID) seeded {
		inst := newInstance(tenantID, time.Now().UTC())
		require.NoError(t, instances.Create(ctx, inst))
		task := newTask(tenantID, inst.ID)
		task.DepartmentID = deptID
		require.NoError(t, tasks.Create(ctx, task))
		a := newTaskAssignment(tenantID, task.ID, delegator)
		require.NoError(t, assignments.Create(ctx, a))
		return seeded{task: task, assignment: a}
	}
	inA, inB := seed(tenantA), seed(tenantB)

	plans := &w17aPlans{resp: &port.CompiledWorkflow{
		WorkflowID: uuid.New(), VersionID: uuid.New(), Status: "PUBLISHED", IsValid: true, CompiledPlanJSON: string(planJSON),
	}}
	eligibility := &w17aEligible{}
	reconciler := &service.DelegationReconciler{
		Instances: instances, Tasks: tasks, Assignments: assignments,
		Outbox: outbox, Signals: outbox, Transactor: postgres.NewTransactor(appPool),
		Definitions: plans, Eligibility: eligibility, Validator: validator,
		Availability: postgres.NewUserAvailabilityRepo(appPool),
		Locks:        postgres.NewRecencyGuardRepo(appPool, appPool),
		Ended:        postgres.NewEndedDelegationRepo(appPool),
	}

	require.NoError(t, reconciler.Reroute(ctx, port.DelegationRerouteInput{
		TenantID: tenantA, DelegationID: delegationID, DelegatorID: delegator, DelegateID: delegate, Scope: "all",
	}))

	assert.Equal(t, []uuid.UUID{tenantA}, eligibility.tenants, "the eligibility check is for the event's tenant only")
	require.NotEmpty(t, plans.reads)
	for _, read := range plans.reads {
		assert.Equal(t, tenantA, read.tenantID, "plans are read only for the event's tenant")
	}
	movedA, err := assignments.GetByID(ctx, tenantA, inA.assignment.ID)
	require.NoError(t, err)
	assert.False(t, movedA.IsActive, "the delegator's row in the event's tenant is vacated")
	activeA, err := assignments.ListActiveByTask(ctx, tenantA, inA.task.ID)
	require.NoError(t, err)
	require.Len(t, activeA, 1)
	assert.Equal(t, delegate, activeA[0].UserID, "and handed to the delegate")

	keptB, err := assignments.GetByID(ctx, tenantB, inB.assignment.ID)
	require.NoError(t, err)
	assert.True(t, keptB.IsActive, "the delegator's row in the other tenant stays active")
	activeB, err := assignments.ListActiveByTask(ctx, tenantB, inB.task.ID)
	require.NoError(t, err)
	require.Len(t, activeB, 1)
	assert.Equal(t, inB.assignment.ID, activeB[0].ID)
	assert.Equal(t, delegator, activeB[0].UserID, "it is still held by the delegator")
	delegateInB, err := assignments.ListActiveByUser(ctx, tenantB, delegate)
	require.NoError(t, err)
	assert.Empty(t, delegateInB, "the delegate gained nothing in the other tenant")
}
