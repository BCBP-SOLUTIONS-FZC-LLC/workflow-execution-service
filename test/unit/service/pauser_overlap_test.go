package service_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

// csaOverlapBarrier holds each caller until every caller has arrived, so the
// calls are known to overlap.
type csaOverlapBarrier struct {
	need    int
	mu      sync.Mutex
	arrived int
	release chan struct{}
}

func csaNewOverlapBarrier(need int) *csaOverlapBarrier {
	return &csaOverlapBarrier{need: need, release: make(chan struct{})}
}

func (b *csaOverlapBarrier) wait(ctx context.Context) error {
	b.mu.Lock()
	b.arrived++
	if b.arrived == b.need {
		close(b.release)
	}
	b.mu.Unlock()
	select {
	case <-b.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// csaOverlapInstances answers each instance read with the stored instance, but
// only once every caller has read, so none sees another's effect.
type csaOverlapInstances struct {
	*csaInstances
	barrier *csaOverlapBarrier
}

func (r *csaOverlapInstances) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	inst, err := r.csaInstances.GetByID(ctx, tenantID, id)
	if err != nil {
		return nil, err
	}
	snapshot := *inst
	if err := r.barrier.wait(ctx); err != nil {
		return nil, err
	}
	return &snapshot, nil
}

// csaSafeTemporal records signals under a lock, for calls made from several goroutines.
type csaSafeTemporal struct {
	*fakeTemporalClient
	mu   sync.Mutex
	sent []signalCall
}

func (c *csaSafeTemporal) SignalWorkflow(_ context.Context, temporalWorkflowID string, instanceID uuid.UUID, signalName string, payload any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sent = append(c.sent, signalCall{TemporalWorkflowID: temporalWorkflowID, InstanceID: instanceID, SignalName: signalName, Payload: payload})
	return nil
}

func TestUserTaskPauser_PauseUserTasks_TwoOverlappingCallsBothSucceedAndSignalTheSameInstance(t *testing.T) {
	instances := &csaInstances{fakeInstanceRepo: newFakeInstanceRepo()}
	tasks := &csaTasks{fakeTaskRepo: newFakeTaskRepo()}
	assignments := csaWrapAssignments(newFakeAssignmentRepo())
	temporal := &csaSafeTemporal{fakeTemporalClient: &fakeTemporalClient{}}
	tenantID, userID := uuid.New(), uuid.New()
	inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning, TemporalWorkflowID: "tenant:subject:key", RecordVersion: 7}
	instances.byID[inst.ID] = inst
	task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: inst.ID}
	tasks.byID[task.ID] = task
	assignments.add(&domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true})

	const callers = 2
	overlap := &csaOverlapInstances{csaInstances: instances, barrier: csaNewOverlapBarrier(callers)}
	pauser := &service.UserTaskPauser{Instances: overlap, Assignments: assignments, Tasks: tasks, Temporal: temporal}

	// The deadline only stops a hung test; nothing asserts on elapsed time.
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	errs := make([]error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = pauser.PauseUserTasks(ctx, tenantID, userID, nil)
		}()
	}
	wg.Wait()

	for i, err := range errs {
		require.NoError(t, err, "call %d", i)
	}
	assert.Equal(t, callers, overlap.barrier.arrived, "both calls read the instance before either signalled")
	assert.Equal(t, []csaIDCall{{tenantID, inst.ID}, {tenantID, inst.ID}}, instances.reads)
	assert.Equal(t, []csaUserCall{{tenantID, userID}, {tenantID, userID}}, assignments.listed)
	require.NotEmpty(t, temporal.sent, "the instance is signalled")
	for _, sent := range temporal.sent {
		assert.Equal(t, port.SignalInstancePause, sent.SignalName)
		assert.Equal(t, inst.ID, sent.InstanceID)
		assert.Equal(t, inst.TemporalWorkflowID, sent.TemporalWorkflowID)
		raw := csaPayloadJSON(t, sent.Payload)
		assert.Contains(t, raw, `"Initiator":"`+domain.InitiatorSafetyNet+`"`)
		assert.Contains(t, raw, `"RecordVersion":7`)
	}
}
