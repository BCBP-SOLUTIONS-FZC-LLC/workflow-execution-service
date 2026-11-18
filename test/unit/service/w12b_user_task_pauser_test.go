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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

var (
	errW12bPauseList     = errors.New("assignment list unreadable")
	errW12bPauseTask     = errors.New("task unreadable")
	errW12bPauseInstance = errors.New("instance unreadable")
	errW12bPauseSignal   = errors.New("signal refused")
)

type w12bPauseIDCall struct{ tenantID, id uuid.UUID }

type w12bPauseAssignments struct {
	port.TaskAssignmentRepository
	active []*domain.TaskAssignment
	err    error
	listed []w12bPauseIDCall
}

func (r *w12bPauseAssignments) ListActiveByUser(_ context.Context, tenantID, userID uuid.UUID) ([]*domain.TaskAssignment, error) {
	r.listed = append(r.listed, w12bPauseIDCall{tenantID, userID})
	if r.err != nil {
		return nil, r.err
	}
	return r.active, nil
}

type w12bPauseTasks struct {
	port.TaskRepository
	byID  map[uuid.UUID]*domain.Task
	errs  map[uuid.UUID]error
	reads []w12bPauseIDCall
}

func (r *w12bPauseTasks) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Task, error) {
	r.reads = append(r.reads, w12bPauseIDCall{tenantID, id})
	if err := r.errs[id]; err != nil {
		return nil, err
	}
	return r.byID[id], nil
}

type w12bPauseInstances struct {
	port.InstanceRepository
	byID  map[uuid.UUID]*domain.Instance
	errs  map[uuid.UUID]error
	reads []w12bPauseIDCall
}

func (r *w12bPauseInstances) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	r.reads = append(r.reads, w12bPauseIDCall{tenantID, id})
	if err := r.errs[id]; err != nil {
		return nil, err
	}
	return r.byID[id], nil
}

type w12bPauseSignalCall struct {
	temporalWorkflowID string
	instanceID         uuid.UUID
	name               string
}

type w12bPauseTemporal struct {
	port.TemporalClient
	errs      map[uuid.UUID]error
	attempted []w12bPauseSignalCall
}

func (c *w12bPauseTemporal) SignalWorkflow(_ context.Context, temporalWorkflowID string, instanceID uuid.UUID, name string, _ any) error {
	c.attempted = append(c.attempted, w12bPauseSignalCall{temporalWorkflowID, instanceID, name})
	return c.errs[instanceID]
}

type w12bPauseLog struct{ warns []map[string]any }

func (l *w12bPauseLog) Debug(string, map[string]any) {}
func (l *w12bPauseLog) Info(string, map[string]any)  {}
func (l *w12bPauseLog) Warn(_ string, f map[string]any) {
	l.warns = append(l.warns, f)
}
func (l *w12bPauseLog) Error(string, map[string]any) {}

type w12bPauseHarness struct {
	pauser      *service.UserTaskPauser
	assignments *w12bPauseAssignments
	tasks       *w12bPauseTasks
	instances   *w12bPauseInstances
	temporal    *w12bPauseTemporal
	log         *w12bPauseLog
	tenantID    uuid.UUID
	userID      uuid.UUID
}

func w12bNewPauseHarness() *w12bPauseHarness {
	h := &w12bPauseHarness{
		assignments: &w12bPauseAssignments{},
		tasks:       &w12bPauseTasks{byID: map[uuid.UUID]*domain.Task{}, errs: map[uuid.UUID]error{}},
		instances:   &w12bPauseInstances{byID: map[uuid.UUID]*domain.Instance{}, errs: map[uuid.UUID]error{}},
		temporal:    &w12bPauseTemporal{errs: map[uuid.UUID]error{}},
		log:         &w12bPauseLog{},
		tenantID:    uuid.New(),
		userID:      uuid.New(),
	}
	h.pauser = &service.UserTaskPauser{
		Instances: h.instances, Assignments: h.assignments, Tasks: h.tasks, Temporal: h.temporal, Log: h.log,
	}
	return h
}

type w12bPauseRow struct {
	assignmentID uuid.UUID
	taskID       uuid.UUID
	instanceID   uuid.UUID
}

// hold gives the user an active assignment on a new task of the given instance; a zero instance id makes a new running instance.
func (h *w12bPauseHarness) hold(instanceID uuid.UUID) w12bPauseRow {
	if instanceID == uuid.Nil {
		instanceID = uuid.New()
	}
	if _, known := h.instances.byID[instanceID]; !known {
		h.instances.byID[instanceID] = &domain.Instance{
			ID: instanceID, TenantID: h.tenantID, Status: domain.InstanceStatusRunning,
			TemporalWorkflowID: "tenant:" + instanceID.String(), RecordVersion: 2,
		}
	}
	task := &domain.Task{ID: uuid.New(), TenantID: h.tenantID, WorkflowInstanceID: instanceID}
	h.tasks.byID[task.ID] = task
	a := &domain.TaskAssignment{ID: uuid.New(), TenantID: h.tenantID, TaskID: task.ID, UserID: h.userID, IsActive: true}
	h.assignments.active = append(h.assignments.active, a)
	return w12bPauseRow{assignmentID: a.ID, taskID: task.ID, instanceID: instanceID}
}

func (h *w12bPauseHarness) pause() error {
	return h.pauser.PauseUserTasks(context.Background(), h.tenantID, h.userID, nil)
}

func w12bPauseFailures(t *testing.T, stage string) float64 {
	t.Helper()
	return obstest.Sum(t, "workflow_execution_service_user_pause_failures_total", map[string]string{"stage": stage})
}

func TestUserTaskPauser_PauseUserTasks_ThreeKindsOfFailureInOneCall(t *testing.T) {
	obstest.Init(t)
	stages := []string{"read_task", "read_instance", "signal"}
	before := map[string]float64{}
	for _, stage := range stages {
		before[stage] = w12bPauseFailures(t, stage)
	}
	h := w12bNewPauseHarness()
	unreadableTask := h.hold(uuid.Nil)
	h.tasks.errs[unreadableTask.taskID] = errW12bPauseTask
	unreadableInstance := h.hold(uuid.Nil)
	h.instances.errs[unreadableInstance.instanceID] = errW12bPauseInstance
	refusedSignal := h.hold(uuid.Nil)
	h.temporal.errs[refusedSignal.instanceID] = errW12bPauseSignal
	healthy := h.hold(uuid.Nil)

	err := h.pause()

	require.ErrorIs(t, err, port.ErrUserPauseIncomplete)
	require.ErrorIs(t, err, errW12bPauseTask)
	require.ErrorIs(t, err, errW12bPauseInstance)
	require.ErrorIs(t, err, errW12bPauseSignal)
	assert.Equal(t, []w12bPauseIDCall{{h.tenantID, h.userID}}, h.assignments.listed)
	assert.Equal(t, []w12bPauseIDCall{
		{h.tenantID, unreadableTask.taskID}, {h.tenantID, unreadableInstance.taskID},
		{h.tenantID, refusedSignal.taskID}, {h.tenantID, healthy.taskID},
	}, h.tasks.reads, "every assignment's task is read, in the caller's tenant")
	assert.Equal(t, []w12bPauseIDCall{
		{h.tenantID, unreadableInstance.instanceID}, {h.tenantID, refusedSignal.instanceID}, {h.tenantID, healthy.instanceID},
	}, h.instances.reads, "the instance of the unreadable task is never read, every other one is")
	assert.Equal(t, []w12bPauseSignalCall{
		{"tenant:" + refusedSignal.instanceID.String(), refusedSignal.instanceID, port.SignalInstancePause},
		{"tenant:" + healthy.instanceID.String(), healthy.instanceID, port.SignalInstancePause},
	}, h.temporal.attempted, "the failures before and after do not stop the healthy instance from being signalled")
	for _, stage := range stages {
		assert.Equal(t, before[stage]+1, w12bPauseFailures(t, stage), "stage %s is counted once", stage)
	}
	assert.Len(t, h.log.warns, 3, "one warning per failure")
}

func TestUserTaskPauser_PauseUserTasks_AnAssignmentListFailureIsReturnedAsItIsAndNothingElseRuns(t *testing.T) {
	obstest.Init(t)
	stages := []string{"read_task", "read_instance", "signal"}
	before := map[string]float64{}
	for _, stage := range stages {
		before[stage] = w12bPauseFailures(t, stage)
	}
	h := w12bNewPauseHarness()
	h.hold(uuid.Nil)
	h.assignments.err = errW12bPauseList

	err := h.pause()

	require.Equal(t, errW12bPauseList, err, "the repository error comes back unwrapped")
	assert.NotErrorIs(t, err, port.ErrUserPauseIncomplete, "it is not the partial-failure error the caller retries as incomplete")
	assert.Equal(t, []w12bPauseIDCall{{h.tenantID, h.userID}}, h.assignments.listed)
	assert.Empty(t, h.tasks.reads)
	assert.Empty(t, h.instances.reads)
	assert.Empty(t, h.temporal.attempted)
	assert.Empty(t, h.log.warns)
	for _, stage := range stages {
		assert.Equal(t, before[stage], w12bPauseFailures(t, stage), "stage %s is not counted", stage)
	}
}

func TestUserTaskPauser_PauseUserTasks_AnUnreadableInstanceIsNotRetriedForALaterAssignmentOfTheSameInstance(t *testing.T) {
	cases := []struct {
		name          string
		sameInstance  bool
		wantReads     int
		wantFailCount float64
	}{
		{"two assignments on one instance read it once", true, 1, 1},
		{"two assignments on different instances read each (control)", false, 2, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obstest.Init(t)
			before := w12bPauseFailures(t, "read_instance")
			h := w12bNewPauseHarness()
			first := h.hold(uuid.Nil)
			secondInstance := uuid.Nil
			if tc.sameInstance {
				secondInstance = first.instanceID
			}
			second := h.hold(secondInstance)
			h.instances.errs[first.instanceID] = errW12bPauseInstance
			h.instances.errs[second.instanceID] = errW12bPauseInstance

			err := h.pause()

			require.ErrorIs(t, err, port.ErrUserPauseIncomplete)
			require.ErrorIs(t, err, errW12bPauseInstance)
			assert.Len(t, h.tasks.reads, 2, "both assignments' tasks are read")
			require.Len(t, h.instances.reads, tc.wantReads)
			assert.Equal(t, w12bPauseIDCall{h.tenantID, first.instanceID}, h.instances.reads[0])
			assert.Empty(t, h.temporal.attempted)
			assert.Equal(t, before+tc.wantFailCount, w12bPauseFailures(t, "read_instance"))
			assert.Len(t, h.log.warns, int(tc.wantFailCount))
		})
	}
}
