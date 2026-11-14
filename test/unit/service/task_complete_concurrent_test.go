package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// csbOverlap holds each caller until two have arrived, so both are inside the
// service at the same time before either goes on.
type csbOverlap struct {
	mu      sync.Mutex
	arrived int
	both    chan struct{}
}

func (o *csbOverlap) wait(ctx context.Context) error {
	o.mu.Lock()
	o.arrived++
	if o.arrived == 2 {
		close(o.both)
	}
	o.mu.Unlock()
	select {
	case <-o.both:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type csbTenantRead struct {
	tenantID uuid.UUID
	id       uuid.UUID
}

// csbOverlapTasks reads tasks for the tenant it is asked for, records who asked,
// and makes the first read of each caller wait for the other caller's first read.
type csbOverlapTasks struct {
	port.TaskRepository
	inner   port.TaskRepository
	overlap *csbOverlap
	mu      sync.Mutex
	reads   []csbTenantRead
}

func (r *csbOverlapTasks) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Task, error) {
	r.mu.Lock()
	r.reads = append(r.reads, csbTenantRead{tenantID, id})
	r.mu.Unlock()
	if err := r.overlap.wait(ctx); err != nil {
		return nil, err
	}
	return r.inner.GetByID(ctx, tenantID, id)
}

type csbOverlapInstances struct {
	port.InstanceRepository
	inner port.InstanceRepository
	mu    sync.Mutex
	reads []csbTenantRead
}

func (r *csbOverlapInstances) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	r.mu.Lock()
	r.reads = append(r.reads, csbTenantRead{tenantID, id})
	r.mu.Unlock()
	return r.inner.GetByID(ctx, tenantID, id)
}

type csbSignalLog struct {
	port.TemporalClient
	mu      sync.Mutex
	signals []signalCall
}

func (c *csbSignalLog) SignalWorkflow(_ context.Context, temporalWorkflowID string, instanceID uuid.UUID, signalName string, payload any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.signals = append(c.signals, signalCall{TemporalWorkflowID: temporalWorkflowID, InstanceID: instanceID, SignalName: signalName, Payload: payload})
	return nil
}

func TestTaskService_Complete_TwoConcurrentCallsForOneTaskVersionSucceedOrOneConflicts(t *testing.T) {
	svc, instances, tasks, assignments, _, _ := newTaskServiceHarness()
	tenantID, instanceID, userID := uuid.New(), uuid.New(), uuid.New()
	instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, TemporalWorkflowID: "tenant:subject:key", Status: domain.InstanceStatusRunning}
	task := newReadyTask(tenantID, instanceID)
	tasks.byID[task.ID] = task
	assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true}

	overlap := &csbOverlap{both: make(chan struct{})}
	taskReads := &csbOverlapTasks{inner: tasks, overlap: overlap}
	instanceReads := &csbOverlapInstances{inner: instances}
	temporal := &csbSignalLog{}
	svc.Tasks, svc.Instances, svc.Temporal = taskReads, instanceReads, temporal

	// A guard so that a change that keeps the second call out of the service
	// fails here with the context error instead of hanging.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = svc.Complete(ctx, tenantID, task.ID, userID, json.RawMessage(`{"decision":"approve"}`), task.RecordVersion)
		}()
	}
	wg.Wait()

	succeeded := 0
	for i, err := range errs {
		if err == nil {
			succeeded++
			continue
		}
		assert.True(t, errors.Is(err, port.ErrRecordVersionConflict), "call %d may only fail with a version conflict, got %v", i, err)
	}
	require.GreaterOrEqual(t, succeeded, 1, "at least one of the two calls completes the task")

	require.NotEmpty(t, temporal.signals)
	for _, s := range temporal.signals {
		assert.Equal(t, "stage-transition", s.SignalName)
		assert.Equal(t, "tenant:subject:key", s.TemporalWorkflowID)
		assert.Equal(t, instanceID, s.InstanceID)
		b, err := json.Marshal(s.Payload)
		require.NoError(t, err)
		var wire struct {
			DeptID, NodeID, UserID string
			RecordVersion          int64
		}
		require.NoError(t, json.Unmarshal(b, &wire))
		assert.Equal(t, task.RecordVersion, wire.RecordVersion)
		assert.Equal(t, "finance", wire.DeptID)
		assert.Equal(t, "review", wire.NodeID)
		assert.Equal(t, userID.String(), wire.UserID)
	}

	require.Len(t, taskReads.reads, 2)
	for _, r := range taskReads.reads {
		assert.Equal(t, csbTenantRead{tenantID, task.ID}, r, "the task is read for the caller's tenant")
	}
	require.NotEmpty(t, instanceReads.reads)
	for _, r := range instanceReads.reads {
		assert.Equal(t, csbTenantRead{tenantID, instanceID}, r, "the instance is read for the caller's tenant")
	}
}
