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

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestCreateTask_ReplayAfterACommittedCreate_CommitsWithoutSecondRowOrEvent(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	taskRepo := postgres.NewTaskRepo(appPool)
	assignmentRepo := postgres.NewTaskAssignmentRepo(appPool)
	outboxRepo := postgres.NewOutboxRepo(appPool)
	validator, err := eventbus.NewSchemaValidator()
	require.NoError(t, err)
	deps := &temporal.Deps{
		Instances:   instanceRepo,
		Tasks:       taskRepo,
		Assignments: assignmentRepo,
		Outbox:      outboxRepo,
		Transactor:  postgres.NewTransactor(appPool),
		Validator:   validator,
	}
	ctx := context.Background()

	tenantID := uuid.New()
	inst := newInstance(tenantID, time.Now().UTC())
	require.NoError(t, instanceRepo.Create(ctx, inst))
	assignee := uuid.New()
	input := port.CreateTaskInput{
		InstanceID:      inst.ID.String(),
		TenantID:        tenantID.String(),
		NodeKey:         "finance/review",
		CompiledNode:    json.RawMessage(`{"type":"userTask","node_id":"review","default_assignees":["` + assignee.String() + `","` + assignee.String() + `"]}`),
		VisitCount:      1,
		IAMDepartmentID: uuid.New().String(),
	}

	first, err := deps.CreateTask(ctx, input)
	require.NoError(t, err)
	replayed, err := deps.CreateTask(ctx, input)

	require.NoError(t, err, "a retry after a committed create must succeed, not fail on commit")
	assert.Equal(t, first.TaskID, replayed.TaskID)

	tasks, _, err := taskRepo.ListByInstance(ctx, tenantID, inst.ID, port.PageRequest{Limit: 100})
	require.NoError(t, err)
	assert.Len(t, tasks, 1)
	active, err := assignmentRepo.ListActiveByTask(ctx, tenantID, uuid.MustParse(first.TaskID))
	require.NoError(t, err)
	assert.Len(t, active, 1, "the repeated assignee holds one assignment")

	records, _, err := outboxRepo.ListByInstance(ctx, tenantID, inst.ID, port.PageRequest{Limit: 100})
	require.NoError(t, err)
	created := 0
	for _, rec := range records {
		if rec.EventType == domain.EventWorkflowTaskCreated {
			created++
		}
	}
	assert.Equal(t, 1, created, "one task.created event for the one task")
}
