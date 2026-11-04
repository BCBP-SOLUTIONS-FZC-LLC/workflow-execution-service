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
	sdktemporal "go.temporal.io/sdk/temporal"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestCreateTask_RefusesAnInstanceThatIsNoLongerRunning(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	taskRepo := postgres.NewTaskRepo(appPool)
	validator, err := eventbus.NewSchemaValidator()
	require.NoError(t, err)
	deps := &temporal.Deps{
		Instances:   instanceRepo,
		Tasks:       taskRepo,
		Assignments: postgres.NewTaskAssignmentRepo(appPool),
		Outbox:      postgres.NewOutboxRepo(appPool),
		Transactor:  postgres.NewTransactor(appPool),
		Validator:   validator,
	}
	ctx := context.Background()

	tenantID := uuid.New()
	inst := newInstance(tenantID, time.Now().UTC())
	require.NoError(t, instanceRepo.Create(ctx, inst))
	input := port.CreateTaskInput{
		InstanceID:      inst.ID.String(),
		TenantID:        tenantID.String(),
		NodeKey:         "finance/review",
		CompiledNode:    json.RawMessage(`{"type":"userTask","node_id":"review","default_assignees":["` + uuid.New().String() + `"]}`),
		VisitCount:      1,
		IAMDepartmentID: uuid.New().String(),
	}

	t.Run("a running instance gets its task", func(t *testing.T) {
		_, err := deps.CreateTask(ctx, input)
		require.NoError(t, err)
	})

	_, err = instanceRepo.UpdateStatus(ctx, tenantID, inst.ID, domain.InstanceStatusTerminated, 1)
	require.NoError(t, err)

	t.Run("a terminated instance gets no task and the refusal is not retried", func(t *testing.T) {
		input.NodeKey = "finance/second"
		_, err := deps.CreateTask(ctx, input)
		require.Error(t, err)
		var appErr *sdktemporal.ApplicationError
		require.ErrorAs(t, err, &appErr)
		assert.Equal(t, "ValidationError", appErr.Type())

		var cursor *port.Cursor
		count := 0
		for {
			page, next, err := taskRepo.ListByInstance(ctx, tenantID, inst.ID, port.PageRequest{After: cursor, Limit: 100})
			require.NoError(t, err)
			count += len(page)
			if next == nil {
				break
			}
			cursor = next
		}
		assert.Equal(t, 1, count, "only the task created while the instance was running exists")
	})
}
