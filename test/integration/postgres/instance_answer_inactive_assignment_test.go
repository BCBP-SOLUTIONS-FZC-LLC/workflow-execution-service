//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

const csfAnswerKey = "finance/Receive_Bid"

type csfAnswerDefinitions struct{ resp *port.CompiledWorkflow }

func (d csfAnswerDefinitions) GetCompiledWorkflow(context.Context, uuid.UUID, uuid.UUID) (*port.CompiledWorkflow, error) {
	return d.resp, nil
}

type csfAnswerTemporal struct {
	port.TemporalClient
	waiting []string
	queries []string
	signals []string
}

func (c *csfAnswerTemporal) QueryWorkflow(_ context.Context, workflowID, _ string, out any) error {
	c.queries = append(c.queries, workflowID)
	raw, err := json.Marshal(c.waiting)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, out)
}

func (c *csfAnswerTemporal) SignalWorkflow(_ context.Context, workflowID string, _ uuid.UUID, signalName string, _ any) error {
	c.signals = append(c.signals, workflowID+" "+signalName)
	return nil
}

func TestInstanceAnswer_AssigneeVisibilityReadsOnlyActiveAssignments(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	taskRepo := postgres.NewTaskRepo(appPool)
	assignmentRepo := postgres.NewTaskAssignmentRepo(appPool)
	ctx := context.Background()

	plan, err := json.Marshal(dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main", TaskQueue: "wf-queue-default",
			Departments: []dsl.DepartmentDef{{ID: "finance", Label: "Finance", Stages: []dsl.StageDef{
				{Type: "receive_task", NodeID: "Receive_Bid", Extras: map[string]string{"message": "bid-received"}},
			}}},
		}},
	})
	require.NoError(t, err)

	type seeded struct {
		svc      *service.InstanceService
		temporal *csfAnswerTemporal
		tenantID uuid.UUID
		instance uuid.UUID
		taskID   uuid.UUID
		caller   uuid.UUID
		assigned uuid.UUID
	}
	seed := func(t *testing.T) seeded {
		t.Helper()
		s := seeded{tenantID: uuid.New(), caller: uuid.New(), temporal: &csfAnswerTemporal{waiting: []string{csfAnswerKey}}}
		inst := newInstance(s.tenantID, time.Now().UTC())
		require.NoError(t, instanceRepo.Create(ctx, inst))
		task := newTask(s.tenantID, inst.ID)
		require.NoError(t, taskRepo.Create(ctx, task))
		assignment := newTaskAssignment(s.tenantID, task.ID, s.caller)
		require.NoError(t, assignmentRepo.Create(ctx, assignment))
		s.instance, s.taskID, s.assigned = inst.ID, task.ID, assignment.ID
		s.svc = &service.InstanceService{
			Instances:   instanceRepo,
			Tasks:       taskRepo,
			Assignments: assignmentRepo,
			Temporal:    s.temporal,
			Definitions: csfAnswerDefinitions{resp: &port.CompiledWorkflow{
				WorkflowID: inst.WorkflowID, VersionID: inst.WorkflowVersionID, Status: "PUBLISHED", IsValid: true, CompiledPlanJSON: string(plan),
			}},
		}
		return s
	}
	answer := func(s seeded) error {
		return s.svc.AnswerOutsideWait(ctx, s.tenantID, s.instance, port.ReadScope{CallerUserID: s.caller}, csfAnswerKey, nil)
	}

	t.Run("an active assignment lets the caller answer", func(t *testing.T) {
		s := seed(t)

		require.NoError(t, answer(s))

		assert.Len(t, s.temporal.signals, 1)
	})

	t.Run("a vacated assignment gives no right to answer", func(t *testing.T) {
		s := seed(t)
		_, err := assignmentRepo.Vacate(ctx, s.tenantID, s.assigned)
		require.NoError(t, err)

		err = answer(s)

		assert.ErrorIs(t, err, port.ErrNotAuthorizedForRead)
		assert.Empty(t, s.temporal.queries)
		assert.Empty(t, s.temporal.signals)
	})

	t.Run("a completed assignment gives no right to answer", func(t *testing.T) {
		s := seed(t)
		task, err := taskRepo.GetByID(ctx, s.tenantID, s.taskID)
		require.NoError(t, err)
		_, err = assignmentRepo.Complete(ctx, s.tenantID, s.assigned, json.RawMessage(`{}`), task.RecordVersion)
		require.NoError(t, err)

		err = answer(s)

		assert.ErrorIs(t, err, port.ErrNotAuthorizedForRead)
		assert.Empty(t, s.temporal.queries)
		assert.Empty(t, s.temporal.signals)
	})
}
