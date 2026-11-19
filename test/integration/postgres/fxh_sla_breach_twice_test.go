//go:build integration

package postgres_test

import (
	"context"
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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

// This guards two breaches recorded one after the other: the second finds the event through the
// existing-record check, so one event is stored and the counter moves once. It does not exercise
// the insert race; the unit test of the lost unique insert covers that.
func TestRecordSLABreach_TwoSequentialBreachesForTheSameTaskWriteOneEventInPostgres(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	taskRepo := postgres.NewTaskRepo(appPool)
	outboxRepo := postgres.NewOutboxRepo(appPool)
	validator, err := eventbus.NewSchemaValidator()
	require.NoError(t, err)
	deps := &temporal.Deps{
		Instances: instanceRepo, Tasks: taskRepo, Assignments: postgres.NewTaskAssignmentRepo(appPool),
		Outbox: outboxRepo, Transactor: postgres.NewTransactor(appPool), Validator: validator,
	}
	ctx := context.Background()
	tenantID := uuid.New()
	inst := newInstance(tenantID, time.Now().UTC())
	require.NoError(t, instanceRepo.Create(ctx, inst))
	task := newTask(tenantID, inst.ID)
	dueAt := time.Now().UTC().Add(-time.Minute)
	task.DueAt = &dueAt
	require.NoError(t, taskRepo.Create(ctx, task))
	in := port.RecordSLABreachInput{InstanceID: inst.ID.String(), TenantID: tenantID.String(), TaskID: task.ID.String(), NodeKey: domain.NodeKey(task.NodeKey)}
	obstest.Init(t)
	before := obstest.Sum(t, "workflow_execution_service_sla_breaches_total", nil)

	require.NoError(t, deps.RecordSLABreach(ctx, in))
	require.NoError(t, deps.RecordSLABreach(ctx, in), "the second breach for the same task is already recorded")

	records, _, err := outboxRepo.ListByInstance(ctx, tenantID, inst.ID, port.PageRequest{Limit: 100})
	require.NoError(t, err)
	breaches := 0
	for _, rec := range records {
		if rec.EventType == domain.EventWorkflowTaskSLABreached {
			breaches++
		}
	}
	assert.Equal(t, 1, breaches, "one breach event for the one task")
	assert.Equal(t, before+1, obstest.Sum(t, "workflow_execution_service_sla_breaches_total", nil), "the counter moved once")
}
