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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

// tskfixBarrierTasks holds every reader of a task until `parties` readers have arrived, so
// concurrent completions are both past their checks before either one signals.
type tskfixBarrierTasks struct {
	*fakeTaskRepo
	mu      sync.Mutex
	arrived int
	parties int
	release chan struct{}
}

func newTskfixBarrierTasks(base *fakeTaskRepo, parties int) *tskfixBarrierTasks {
	return &tskfixBarrierTasks{fakeTaskRepo: base, parties: parties, release: make(chan struct{})}
}

func (b *tskfixBarrierTasks) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Task, error) {
	task, err := b.fakeTaskRepo.GetByID(ctx, tenantID, id)
	b.mu.Lock()
	b.arrived++
	if b.arrived == b.parties {
		close(b.release)
	}
	b.mu.Unlock()
	<-b.release
	return task, err
}

type tskfixSafeTemporal struct {
	mu      sync.Mutex
	signals []signalCall
	failure error
}

func (c *tskfixSafeTemporal) SignalWorkflow(_ context.Context, wfID string, instanceID uuid.UUID, name string, payload any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.signals = append(c.signals, signalCall{TemporalWorkflowID: wfID, InstanceID: instanceID, SignalName: name, Payload: payload})
	return c.failure
}

func (c *tskfixSafeTemporal) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.signals)
}

func (c *tskfixSafeTemporal) StartWorkflow(context.Context, port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
	return port.StartWorkflowOutput{}, nil
}
func (c *tskfixSafeTemporal) UpdateWorkflow(context.Context, string, uuid.UUID, string, any) error {
	return nil
}
func (c *tskfixSafeTemporal) TerminateWorkflow(context.Context, string, string, string) error {
	return nil
}
func (c *tskfixSafeTemporal) QueryWorkflow(context.Context, string, string, any) error { return nil }

// tskfixSafeCache is a mutex-guarded cache whose SetNX can be made to fail.
type tskfixSafeCache struct {
	mu        sync.Mutex
	values    map[string]string
	setNXErr  error
	setNXKeys []string
}

func newTskfixSafeCache() *tskfixSafeCache { return &tskfixSafeCache{values: map[string]string{}} }

func (c *tskfixSafeCache) Get(_ context.Context, key string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.values[key], nil
}

func (c *tskfixSafeCache) Set(_ context.Context, key, value string, _ time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.values[key] = value
	return nil
}

func (c *tskfixSafeCache) Del(_ context.Context, keys ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, k := range keys {
		delete(c.values, k)
	}
	return nil
}

func (c *tskfixSafeCache) SetNX(_ context.Context, key, value string, _ time.Duration) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.setNXKeys = append(c.setNXKeys, key)
	if c.setNXErr != nil {
		return false, c.setNXErr
	}
	if _, ok := c.values[key]; ok {
		return false, nil
	}
	c.values[key] = value
	return true, nil
}

func (c *tskfixSafeCache) Ping(context.Context) error { return nil }

type tskfixCompleteWorld struct {
	svc      *service.TaskService
	temporal *tskfixSafeTemporal
	cache    *tskfixSafeCache
	tenantID uuid.UUID
	task     *domain.Task
	userID   uuid.UUID
}

func newTskfixCompleteWorld(parties int) *tskfixCompleteWorld {
	svc, instances, tasks, assignments, _, _ := newTaskServiceHarness()
	tenantID, instanceID := uuid.New(), uuid.New()
	instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, TemporalWorkflowID: "tenant:biz", Status: domain.InstanceStatusRunning}
	task := newReadyTask(tenantID, instanceID)
	tasks.byID[task.ID] = task
	userID := uuid.New()
	assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true}

	w := &tskfixCompleteWorld{temporal: &tskfixSafeTemporal{}, cache: newTskfixSafeCache(), tenantID: tenantID, task: task, userID: userID}
	svc.Tasks = newTskfixBarrierTasks(tasks, parties)
	svc.Temporal = w.temporal
	svc.Cache = w.cache
	w.svc = svc
	return w
}

func (w *tskfixCompleteWorld) complete() error {
	_, err := w.svc.Complete(context.Background(), w.tenantID, w.task.ID, w.userID, json.RawMessage(`{}`), w.task.RecordVersion)
	return err
}

func TestTaskService_Complete_ConcurrentCompletionsSignalOnce(t *testing.T) {
	w := newTskfixCompleteWorld(2)

	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = w.complete()
		}()
	}
	wg.Wait()

	assert.Equal(t, 1, w.temporal.count(), "two concurrent completions of one task must send one signal")
	succeeded, conflicted := 0, 0
	for _, err := range errs {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, port.ErrRecordVersionConflict):
			conflicted++
		}
	}
	assert.Equal(t, 1, succeeded)
	assert.Equal(t, 1, conflicted, "the loser is refused with a version conflict")
}

func TestTaskService_Complete_FailedSignalReleasesTheDedupeKey(t *testing.T) {
	w := newTskfixCompleteWorld(1)
	w.temporal.failure = assert.AnError

	require.Error(t, w.complete())
	assert.Empty(t, w.cache.values, "a completion that never reached the workflow must stay retryable")

	w.temporal.failure = nil
	require.NoError(t, w.complete())
	assert.Equal(t, 2, w.temporal.count())
}

func TestTaskService_Complete_CacheFailureFailsOpen(t *testing.T) {
	w := newTskfixCompleteWorld(1)
	w.cache.setNXErr = assert.AnError

	require.NoError(t, w.complete())
	assert.Equal(t, 1, w.temporal.count())
	assert.NotEmpty(t, w.cache.setNXKeys, "the guard was consulted")
}
