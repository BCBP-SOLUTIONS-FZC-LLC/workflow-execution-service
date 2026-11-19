//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	outboundtemporal "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

var errW25bOutboxDown = errors.New("w25b outbox down")

// w25bFailingOutbox refuses the enqueue while down is set; every other call
// goes to the real outbox.
type w25bFailingOutbox struct {
	port.OutboxRepository
	down bool
}

func (o *w25bFailingOutbox) Enqueue(ctx context.Context, env events.Envelope[json.RawMessage]) error {
	if o.down {
		return errW25bOutboxDown
	}
	return o.OutboxRepository.Enqueue(ctx, env)
}

func TestCreateTask_OutboxEnqueueFailsOnRealPostgres_LeavesNoTaskAssignmentOrEventAndTheRetryCreatesThem(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	validator, err := eventbus.NewSchemaValidator()
	require.NoError(t, err)
	instances := postgres.NewInstanceRepo(appPool)
	tasks := postgres.NewTaskRepo(appPool)
	assignments := postgres.NewTaskAssignmentRepo(appPool)
	outbox := &w25bFailingOutbox{OutboxRepository: postgres.NewOutboxRepo(appPool), down: true}
	deps := &outboundtemporal.Deps{
		Instances: instances, Tasks: tasks, Assignments: assignments, Outbox: outbox,
		Transactor: postgres.NewTransactor(appPool), Validator: validator,
	}
	ctx := context.Background()
	tenant := uuid.New()
	inst := newInstance(tenant, time.Now().UTC())
	require.NoError(t, instances.Create(ctx, inst))
	user := uuid.New()
	compiled, err := json.Marshal(dsl.StageDef{Type: "review", DefaultAssignees: []string{user.String()}})
	require.NoError(t, err)
	in := port.CreateTaskInput{
		InstanceID: inst.ID.String(), TenantID: tenant.String(), NodeKey: "sales/review", CompiledNode: compiled,
		VisitCount: 1, IAMDepartmentID: uuid.NewString(),
	}

	_, err = deps.CreateTask(ctx, in)

	require.ErrorIs(t, err, errW25bOutboxDown)
	var appErr *temporal.ApplicationError
	require.False(t, errors.As(err, &appErr), "a plain error is retried by Temporal")
	rows, _, err := tasks.ListByInstance(ctx, tenant, inst.ID, port.PageRequest{Limit: 10})
	require.NoError(t, err)
	assert.Empty(t, rows, "the task row went with the failed transaction")
	records, _, err := postgres.NewOutboxRepo(appPool).ListByInstance(ctx, tenant, inst.ID, port.PageRequest{Limit: 10})
	require.NoError(t, err)
	assert.Empty(t, records)
	active, err := assignments.ListActiveByUser(ctx, tenant, user)
	require.NoError(t, err)
	assert.Empty(t, active, "no assignment of the user outlived the failed transaction")

	outbox.down = false
	out, err := deps.CreateTask(ctx, in)
	require.NoError(t, err)

	taskID := uuid.MustParse(out.TaskID)
	stored, err := tasks.GetByID(ctx, tenant, taskID)
	require.NoError(t, err)
	assert.Equal(t, domain.TaskStatusReady, stored.Status)
	held, err := assignments.ListActiveByTask(ctx, tenant, taskID)
	require.NoError(t, err)
	require.Len(t, held, 1)
	assert.Equal(t, user, held[0].UserID)
	records, _, err = postgres.NewOutboxRepo(appPool).ListByInstance(ctx, tenant, inst.ID, port.PageRequest{Limit: 10})
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, domain.EventWorkflowTaskCreated, records[0].EventType)

	again, err := deps.CreateTask(ctx, in)
	require.NoError(t, err)
	assert.Equal(t, out.TaskID, again.TaskID)
	records, _, err = postgres.NewOutboxRepo(appPool).ListByInstance(ctx, tenant, inst.ID, port.PageRequest{Limit: 10})
	require.NoError(t, err)
	assert.Len(t, records, 1, "a repeat of the committed call announces nothing more")

	otherTenant := uuid.New()
	_, err = tasks.GetByID(ctx, otherTenant, taskID)
	require.ErrorIs(t, err, domain.ErrNotFound, "another tenant cannot see the task")
}
