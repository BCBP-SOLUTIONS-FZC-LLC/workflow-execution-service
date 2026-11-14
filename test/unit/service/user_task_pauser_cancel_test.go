package service_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

type csbPauseCall struct {
	tenantID uuid.UUID
	id       uuid.UUID
}

// csbPauseAssignments answers the user's active assignments and records who was asked.
type csbPauseAssignments struct {
	port.TaskAssignmentRepository
	rows       []*domain.TaskAssignment
	gotTenant  uuid.UUID
	gotUser    uuid.UUID
	listedOnce bool
}

func (r *csbPauseAssignments) ListActiveByUser(_ context.Context, tenantID, userID uuid.UUID) ([]*domain.TaskAssignment, error) {
	r.gotTenant, r.gotUser, r.listedOnce = tenantID, userID, true
	return r.rows, nil
}

// csbPauseTasks reads a task by id. Like a database call, it fails with the
// context's error once the context is cancelled, when failOnCancel is set.
type csbPauseTasks struct {
	port.TaskRepository
	byID         map[uuid.UUID]*domain.Task
	failOnCancel bool
	calls        []csbPauseCall
}

func (r *csbPauseTasks) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Task, error) {
	r.calls = append(r.calls, csbPauseCall{tenantID, id})
	if r.failOnCancel && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return r.byID[id], nil
}

type csbPauseInstances struct {
	port.InstanceRepository
	byID  map[uuid.UUID]*domain.Instance
	calls []csbPauseCall
}

func (r *csbPauseInstances) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	r.calls = append(r.calls, csbPauseCall{tenantID, id})
	return r.byID[id], nil
}

// csbPauseTemporal cancels the caller's context while it handles the first
// signal, then, when failOnCancel is set, fails every later signal with the
// context's error as a real client would.
type csbPauseTemporal struct {
	port.TemporalClient
	cancel       context.CancelFunc
	failOnCancel bool
	signalled    []string
	instances    []uuid.UUID
}

func (c *csbPauseTemporal) SignalWorkflow(ctx context.Context, temporalWorkflowID string, instanceID uuid.UUID, _ string, _ any) error {
	c.signalled = append(c.signalled, temporalWorkflowID)
	c.instances = append(c.instances, instanceID)
	if len(c.signalled) == 1 {
		c.cancel()
		return nil
	}
	if c.failOnCancel {
		return ctx.Err()
	}
	return nil
}

func TestUserTaskPauser_PauseUserTasks_ACancelledContextDoesNotStopTheLoop(t *testing.T) {
	cases := []struct {
		name           string
		readsFail      bool
		stage          string
		wantTaskReads  int
		wantInstReads  int
		wantSignalsTry int
	}{
		{name: "the remaining task reads fail", readsFail: true, stage: "read_task", wantTaskReads: 3, wantInstReads: 1, wantSignalsTry: 1},
		{name: "the remaining signals fail", readsFail: false, stage: "signal", wantTaskReads: 3, wantInstReads: 3, wantSignalsTry: 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obstest.Init(t)
			counter := func() float64 {
				return obstest.Sum(t, "workflow_execution_service_user_pause_failures_total", map[string]string{"stage": tc.stage})
			}
			before := counter()

			tenantID, userID := uuid.New(), uuid.New()
			assignments := &csbPauseAssignments{}
			tasks := &csbPauseTasks{byID: map[uuid.UUID]*domain.Task{}, failOnCancel: tc.readsFail}
			instances := &csbPauseInstances{byID: map[uuid.UUID]*domain.Instance{}}
			var instanceIDs []uuid.UUID
			var workflowIDs []string
			for i := range 3 {
				inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning, TemporalWorkflowID: "tenant:subject:key-" + string(rune('a'+i)), RecordVersion: 1}
				instances.byID[inst.ID] = inst
				instanceIDs = append(instanceIDs, inst.ID)
				workflowIDs = append(workflowIDs, inst.TemporalWorkflowID)
				task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: inst.ID}
				tasks.byID[task.ID] = task
				assignments.rows = append(assignments.rows, &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true})
			}

			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			temporal := &csbPauseTemporal{cancel: cancel, failOnCancel: !tc.readsFail}
			pauser := &service.UserTaskPauser{Instances: instances, Assignments: assignments, Tasks: tasks, Temporal: temporal}

			err := pauser.PauseUserTasks(ctx, tenantID, userID, nil)

			require.ErrorIs(t, err, port.ErrUserPauseIncomplete, "a pause cut short must not be acknowledged")
			require.ErrorIs(t, err, context.Canceled, "the cause of the two failed instances is the cancelled context")
			assert.Equal(t, before+2, counter(), "the two instances after the cancellation are each counted")

			assert.True(t, assignments.listedOnce)
			assert.Equal(t, tenantID, assignments.gotTenant)
			assert.Equal(t, userID, assignments.gotUser)
			assert.Len(t, tasks.calls, tc.wantTaskReads, "every assignment is still tried after the cancellation")
			for _, c := range tasks.calls {
				assert.Equal(t, tenantID, c.tenantID)
			}
			require.Len(t, instances.calls, tc.wantInstReads)
			for i, c := range instances.calls {
				assert.Equal(t, tenantID, c.tenantID)
				assert.Equal(t, instanceIDs[i], c.id)
			}
			assert.Equal(t, workflowIDs[:tc.wantSignalsTry], temporal.signalled)
			assert.Equal(t, instanceIDs[:tc.wantSignalsTry], temporal.instances, "each signal names its own instance")
		})
	}
}
