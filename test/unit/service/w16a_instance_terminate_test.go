package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var (
	errW16aAssignmentRead = errors.New("w16a: assignment read failed")
	errW16aVacate         = errors.New("w16a: vacate failed")
)

// w16aPagedTasks lists an instance's tasks page by page in a fixed order and records each page request.
type w16aPagedTasks struct {
	*cscTaskRepo
	order      []uuid.UUID
	updateErr  error
	pageLimits []int
	pageAfter  []uuid.UUID
}

func (r *w16aPagedTasks) ListByInstance(_ context.Context, tenantID, instanceID uuid.UUID, page port.PageRequest) ([]*domain.Task, *port.Cursor, error) {
	r.lists = append(r.lists, cscTaskListCall{TenantID: tenantID, InstanceID: instanceID})
	r.pageLimits = append(r.pageLimits, page.Limit)
	start := 0
	if page.After != nil {
		r.pageAfter = append(r.pageAfter, page.After.ID)
		for i, id := range r.order {
			if id == page.After.ID {
				start = i + 1
			}
		}
	} else {
		r.pageAfter = append(r.pageAfter, uuid.Nil)
	}
	end := start + page.Limit
	if end > len(r.order) {
		end = len(r.order)
	}
	var out []*domain.Task
	for _, id := range r.order[start:end] {
		copied := *r.byID[id]
		out = append(out, &copied)
	}
	if end >= len(r.order) {
		return out, nil, nil
	}
	return out, &port.Cursor{ID: r.order[end-1]}, nil
}

func (r *w16aPagedTasks) UpdateStatus(ctx context.Context, tenantID, id uuid.UUID, status domain.TaskStatus, recordVersion int64) (*domain.Task, error) {
	if r.updateErr != nil {
		r.updates = append(r.updates, cscTaskUpdate{TenantID: tenantID, TaskID: id, Status: status, RecordVersion: recordVersion, InTx: r.tx.active})
		return nil, r.updateErr
	}
	return r.cscTaskRepo.UpdateStatus(ctx, tenantID, id, status, recordVersion)
}

func (r *w16aPagedTasks) add(t *domain.Task) {
	r.byID[t.ID] = t
	r.order = append(r.order, t.ID)
}

// w16aFaultyAssignments fails the active-assignment read or the vacate, and records the ids each call got.
type w16aFaultyAssignments struct {
	*cscAssignmentRepo
	listErr   error
	vacateErr error
	listed    []cscVacateCall
}

func (r *w16aFaultyAssignments) ListActiveByTask(ctx context.Context, tenantID, taskID uuid.UUID) ([]*domain.TaskAssignment, error) {
	r.listed = append(r.listed, cscVacateCall{TenantID: tenantID, AssignmentID: taskID})
	if r.listErr != nil {
		return nil, r.listErr
	}
	return r.cscAssignmentRepo.ListActiveByTask(ctx, tenantID, taskID)
}

func (r *w16aFaultyAssignments) Vacate(ctx context.Context, tenantID, id uuid.UUID) (*domain.TaskAssignment, error) {
	if r.vacateErr != nil {
		r.vacated = append(r.vacated, cscVacateCall{TenantID: tenantID, AssignmentID: id})
		return nil, r.vacateErr
	}
	return r.cscAssignmentRepo.Vacate(ctx, tenantID, id)
}

func w16aTerminateRig(inst *domain.Instance) (*cscHarness, *w16aPagedTasks, *w16aFaultyAssignments) {
	h := newCscHarness(inst)
	tasks := &w16aPagedTasks{cscTaskRepo: h.tasks}
	assignments := &w16aFaultyAssignments{cscAssignmentRepo: h.assignments}
	h.svc.Tasks = tasks
	h.svc.Assignments = assignments
	return h, tasks, assignments
}

func TestInstanceService_Terminate_FailsOnlyReadyAndInProgressTasksAcrossEveryPage(t *testing.T) {
	tenantID, actorID := uuid.New(), uuid.New()
	inst := newCscInstance(tenantID, domain.InstanceStatusRunning, 4)
	h, tasks, _ := w16aTerminateRig(inst)

	// 230 tasks: three pages of at most 100. Statuses cycle so every kind lands on every page.
	statuses := []domain.TaskStatus{
		domain.TaskStatusReady, domain.TaskStatusCompleted, domain.TaskStatusInProgress,
		domain.TaskStatusDeferred, domain.TaskStatusFailed, domain.TaskStatusSuperseded,
	}
	wantFailed := map[uuid.UUID]int64{}
	untouched := map[uuid.UUID]domain.TaskStatus{}
	for i := 0; i < 230; i++ {
		status := statuses[i%len(statuses)]
		task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: inst.ID, Status: status, RecordVersion: int64(i + 1)}
		tasks.add(task)
		if status == domain.TaskStatusReady || status == domain.TaskStatusInProgress {
			wantFailed[task.ID] = task.RecordVersion
		} else {
			untouched[task.ID] = status
		}
	}

	require.NoError(t, h.svc.Terminate(context.Background(), tenantID, inst.ID, actorID, "cleanup"))

	assert.Equal(t, []int{100, 100, 100}, tasks.pageLimits, "each page asks for 100 rows and the walk ends on the short page")
	assert.Equal(t, uuid.Nil, tasks.pageAfter[0])
	assert.Equal(t, tasks.order[99], tasks.pageAfter[1], "the second page starts after the last row of the first")
	assert.Equal(t, tasks.order[199], tasks.pageAfter[2], "the third page starts after the last row of the second")
	for _, l := range tasks.lists {
		assert.Equal(t, cscTaskListCall{TenantID: tenantID, InstanceID: inst.ID}, l)
	}

	require.Len(t, tasks.updates, len(wantFailed))
	gotFailed := map[uuid.UUID]int64{}
	for _, u := range tasks.updates {
		assert.Equal(t, domain.TaskStatusFailed, u.Status)
		assert.Equal(t, tenantID, u.TenantID)
		assert.True(t, u.InTx)
		gotFailed[u.TaskID] = u.RecordVersion
	}
	assert.Equal(t, wantFailed, gotFailed, "every READY and IN_PROGRESS task is failed with the version it was read at")
	for id, status := range untouched {
		assert.Equal(t, status, tasks.byID[id].Status, "a task that is not READY or IN_PROGRESS keeps its status")
	}

	require.Len(t, h.outbox.attempts, len(wantFailed)+1, "one task-failed event per failed task, then the instance event")
	for _, a := range h.outbox.attempts[:len(wantFailed)] {
		assert.Equal(t, domain.EventWorkflowTaskFailed, a.Envelope.Type)
	}
	assert.Equal(t, domain.EventWorkflowInstanceTerminated, h.outbox.attempts[len(wantFailed)].Envelope.Type)
}

func TestInstanceService_Terminate_ACascadeFailureEndsTheTransactionCallbackWithTheUnchangedErrorKind(t *testing.T) {
	cases := []struct {
		name        string
		setup       func(tasks *w16aPagedTasks, assignments *w16aFaultyAssignments)
		wantErr     error
		sameError   bool
		notErr      error
		wantVacates bool
		readsAssign bool
	}{
		{
			name:    "a task that vanished is mapped to task not found",
			setup:   func(tasks *w16aPagedTasks, _ *w16aFaultyAssignments) { tasks.updateErr = domain.ErrNotFound },
			wantErr: port.ErrTaskNotFound,
			notErr:  domain.ErrNotFound,
		},
		{
			name: "an assignment read failure comes back as it was",
			setup: func(_ *w16aPagedTasks, assignments *w16aFaultyAssignments) {
				assignments.listErr = errW16aAssignmentRead
			},
			wantErr:     errW16aAssignmentRead,
			sameError:   true,
			readsAssign: true,
		},
		{
			name:        "an assignment vacate failure comes back as it was",
			setup:       func(_ *w16aPagedTasks, assignments *w16aFaultyAssignments) { assignments.vacateErr = errW16aVacate },
			wantErr:     errW16aVacate,
			sameError:   true,
			wantVacates: true,
			readsAssign: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tenantID, actorID := uuid.New(), uuid.New()
			inst := newCscInstance(tenantID, domain.InstanceStatusRunning, 4)
			h, tasks, assignments := w16aTerminateRig(inst)
			open := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: inst.ID, Status: domain.TaskStatusInProgress, RecordVersion: 3}
			tasks.add(open)
			assignment := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: open.ID, UserID: uuid.New(), IsActive: true}
			h.assignments.byID[assignment.ID] = assignment
			tc.setup(tasks, assignments)

			err := h.svc.Terminate(context.Background(), tenantID, inst.ID, actorID, "reason")

			require.ErrorIs(t, err, tc.wantErr)
			if tc.notErr != nil {
				assert.NotErrorIs(t, err, tc.notErr)
			}
			if tc.sameError {
				assert.Equal(t, tc.wantErr, err, "the repository error is not wrapped")
			}

			require.Equal(t, 1, h.tx.runs)
			require.Equal(t, err, h.tx.lastErr, "the callback ends in the failure; a real transaction rolls back on a callback error (shown against Postgres in the integration test)")
			assert.Equal(t, []string{tenantID.String()}, h.tx.guTenants)
			require.Len(t, h.instances.updates, 1)
			assert.Equal(t, cscUpdateCall{TenantID: tenantID, InstanceID: inst.ID, Status: domain.InstanceStatusTerminated, RecordVersion: 4, InTx: true}, h.instances.updates[0],
				"the status change ran inside the transaction that is rolled back")
			assert.Empty(t, h.outbox.attempts, "no event is written once the cascade failed")
			assert.Equal(t, []cscTerminate{{WorkflowID: inst.TemporalWorkflowID, RunID: inst.TemporalRunID, Reason: "reason"}}, h.temporal.terminates,
				"the run was terminated once before the transaction, and not again after the failure")
			assert.Equal(t, []cscTaskUpdate{{TenantID: tenantID, TaskID: open.ID, Status: domain.TaskStatusFailed, RecordVersion: 3, InTx: true}}, tasks.updates)
			if tc.wantVacates {
				assert.Equal(t, []cscVacateCall{{TenantID: tenantID, AssignmentID: assignment.ID}}, assignments.vacated)
			}
			if tc.readsAssign {
				assert.Equal(t, []cscVacateCall{{TenantID: tenantID, AssignmentID: open.ID}}, assignments.listed)
			} else {
				assert.Empty(t, assignments.listed, "the assignments are not read once the task update failed")
			}
		})
	}
}
