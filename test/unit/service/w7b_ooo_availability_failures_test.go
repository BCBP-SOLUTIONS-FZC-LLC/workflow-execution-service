package service_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type w7bLogEntry struct {
	msg    string
	fields map[string]any
}

// w7bInfoLogger records the info lines; the shared fake drops them.
type w7bInfoLogger struct {
	infos []w7bLogEntry
}

func (l *w7bInfoLogger) Debug(string, map[string]any) {}
func (l *w7bInfoLogger) Info(msg string, fields map[string]any) {
	l.infos = append(l.infos, w7bLogEntry{msg: msg, fields: fields})
}
func (l *w7bInfoLogger) Warn(string, map[string]any)  {}
func (l *w7bInfoLogger) Error(string, map[string]any) {}

// w7bFailingTasks fails the read of chosen tasks and records the tenant of every read.
type w7bFailingTasks struct {
	*fakeTaskRepo
	failing map[uuid.UUID]error
	tenants []uuid.UUID
}

func (r *w7bFailingTasks) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Task, error) {
	r.tenants = append(r.tenants, tenantID)
	if err := r.failing[id]; err != nil {
		return nil, err
	}
	return r.fakeTaskRepo.GetByID(ctx, tenantID, id)
}

// w7bFailingInstances fails the read of chosen instances and records the tenant of every read.
type w7bFailingInstances struct {
	*fakeInstanceRepo
	failing map[uuid.UUID]error
	tenants []uuid.UUID
}

func (r *w7bFailingInstances) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	r.tenants = append(r.tenants, tenantID)
	if err := r.failing[id]; err != nil {
		return nil, err
	}
	return r.fakeInstanceRepo.GetByID(ctx, tenantID, id)
}

type w7bOOOInstance struct {
	instance *domain.Instance
	task     *domain.Task
}

// w7bSeedOOOInstance gives the user an active assignment on a task of a new instance.
func w7bSeedOOOInstance(instances *fakeInstanceRepo, tasks *fakeTaskRepo, assignments *fakeAssignmentRepo, tenantID, userID uuid.UUID) w7bOOOInstance {
	inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning, TemporalWorkflowID: tenantID.String() + ":" + uuid.NewString(), RecordVersion: 3}
	instances.byID[inst.ID] = inst
	task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: inst.ID}
	tasks.byID[task.ID] = task
	assignments.add(&domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true})
	return w7bOOOInstance{instance: inst, task: task}
}

func w7bSignalledInstances(temporal *fakeTemporalClient, name string) []uuid.UUID {
	var ids []uuid.UUID
	for _, s := range temporal.signalsNamed(name) {
		ids = append(ids, s.InstanceID)
	}
	return ids
}

var w7bOOOStatuses = []struct {
	status string
	signal string
}{
	{status: port.AvailabilityOOO, signal: port.SignalInstancePause},
	{status: port.AvailabilityAvailable, signal: port.SignalInstanceResume},
}

func TestOOOAvailabilityReconciler_Apply_WorkflowNoLongerExists_SkipsItAndSignalsTheOthers(t *testing.T) {
	for _, tc := range w7bOOOStatuses {
		t.Run(tc.status, func(t *testing.T) {
			svc, instances, tasks, assignments, temporal := newOOOAvailabilityHarness()
			log := &w7bInfoLogger{}
			svc.Log = log
			tenantID, userID := uuid.New(), uuid.New()
			first := w7bSeedOOOInstance(instances, tasks, assignments, tenantID, userID)
			gone := w7bSeedOOOInstance(instances, tasks, assignments, tenantID, userID)
			last := w7bSeedOOOInstance(instances, tasks, assignments, tenantID, userID)
			temporal.signalFunc = func(_ context.Context, workflowID string, _ uuid.UUID, _ string, _ any) error {
				if workflowID == gone.instance.TemporalWorkflowID {
					return fmt.Errorf("signal: %w", port.ErrWorkflowNotFound)
				}
				return nil
			}

			err := svc.Apply(context.Background(), port.UserAvailabilityInput{TenantID: tenantID, UserID: userID, Status: tc.status})

			require.NoError(t, err, "a workflow that no longer exists does not fail the event")
			assert.ElementsMatch(t, []uuid.UUID{first.instance.ID, gone.instance.ID, last.instance.ID}, w7bSignalledInstances(temporal, tc.signal),
				"every instance is attempted, the missing one included")
			assert.Len(t, temporal.signals, 3, "and nothing else is sent")
			var skipped []any
			for _, e := range log.infos {
				skipped = append(skipped, e.fields["instance_id"])
				assert.Equal(t, tc.signal, e.fields["signal"])
			}
			assert.Equal(t, []any{gone.instance.ID}, skipped, "only the missing instance is logged as skipped")
		})
	}
}

func TestOOOAvailabilityReconciler_Apply_TaskOrInstanceReadFails_SignalsTheOthersThenFailsTheEvent(t *testing.T) {
	errTaskRead := errors.New("task store down")
	errInstanceRead := errors.New("instance store down")
	for _, tc := range w7bOOOStatuses {
		t.Run(tc.status, func(t *testing.T) {
			svc, instances, tasks, assignments, temporal := newOOOAvailabilityHarness()
			tenantID, userID := uuid.New(), uuid.New()
			badTask := w7bSeedOOOInstance(instances, tasks, assignments, tenantID, userID)
			badInstance := w7bSeedOOOInstance(instances, tasks, assignments, tenantID, userID)
			ok := w7bSeedOOOInstance(instances, tasks, assignments, tenantID, userID)
			taskReads := &w7bFailingTasks{fakeTaskRepo: tasks, failing: map[uuid.UUID]error{badTask.task.ID: errTaskRead}}
			instanceReads := &w7bFailingInstances{fakeInstanceRepo: instances, failing: map[uuid.UUID]error{badInstance.instance.ID: errInstanceRead}}
			svc.Tasks, svc.Instances = taskReads, instanceReads

			err := svc.Apply(context.Background(), port.UserAvailabilityInput{TenantID: tenantID, UserID: userID, Status: tc.status})

			require.Error(t, err)
			assert.ErrorIs(t, err, errTaskRead, "the task read failure is returned")
			assert.ErrorIs(t, err, errInstanceRead, "joined with the instance read failure")
			assert.NotErrorIs(t, err, domain.ErrNotFound)
			assert.Equal(t, []uuid.UUID{ok.instance.ID}, w7bSignalledInstances(temporal, tc.signal),
				"the instance after the failures is still signalled")
			assert.Len(t, temporal.signals, 1)
			assert.Equal(t, []uuid.UUID{tenantID, tenantID, tenantID}, taskReads.tenants, "every task read names the event's tenant")
			assert.Equal(t, []uuid.UUID{tenantID, tenantID}, instanceReads.tenants, "so does every instance read")
		})
	}
}
