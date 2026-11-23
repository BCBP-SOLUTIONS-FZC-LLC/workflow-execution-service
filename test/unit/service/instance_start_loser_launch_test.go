package service_test

import (
	"context"
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

const cstBarrierTimeout = 30 * time.Second

// cstBarrier releases its waiters once n of them have arrived. A waiter that is
// never joined gives up after cstBarrierTimeout and reports it, so a broken
// overlap fails the test instead of hanging it.
type cstBarrier struct {
	mu      sync.Mutex
	need    int
	arrived int
	open    chan struct{}
}

func cstNewBarrier(n int) *cstBarrier {
	return &cstBarrier{need: n, open: make(chan struct{})}
}

func (b *cstBarrier) wait() bool {
	b.mu.Lock()
	b.arrived++
	if b.arrived == b.need {
		close(b.open)
	}
	b.mu.Unlock()
	select {
	case <-b.open:
		return true
	case <-time.After(cstBarrierTimeout):
		return false
	}
}

type cstCall struct {
	tenantID uuid.UUID
	id       uuid.UUID
}

// cstGuardedInstances makes the shared in-memory repo safe for two callers and
// lets the two starts overlap: both business-key lookups happen before either
// insert, so the second insert meets the first one's row on the unique key.
// It hands out copies, as a database does, so the callers never share a row.
type cstGuardedInstances struct {
	*fakeInstanceRepo
	mu          sync.Mutex
	lookupGate  *cstBarrier
	lookups     int
	lookupKeys  []csbLookup
	createErrs  []error
	runIDWrites []cstCall
	reads       []cstCall
}

func (r *cstGuardedInstances) GetLiveByBusinessKey(ctx context.Context, tenantID uuid.UUID, subjectID, businessKey string) (*domain.Instance, error) {
	r.mu.Lock()
	r.lookups++
	n := r.lookups
	r.lookupKeys = append(r.lookupKeys, csbLookup{tenantID, subjectID, businessKey})
	got, err := r.fakeInstanceRepo.GetLiveByBusinessKey(ctx, tenantID, subjectID, businessKey)
	var cp *domain.Instance
	if err == nil {
		c := *got
		cp = &c
	}
	r.mu.Unlock()
	if n <= 2 {
		r.lookupGate.wait()
	}
	return cp, err
}

func (r *cstGuardedInstances) Create(ctx context.Context, inst *domain.Instance) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	err := r.fakeInstanceRepo.Create(ctx, inst)
	r.createErrs = append(r.createErrs, err)
	return err
}

func (r *cstGuardedInstances) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reads = append(r.reads, cstCall{tenantID, id})
	got, err := r.fakeInstanceRepo.GetByID(ctx, tenantID, id)
	if err != nil {
		return nil, err
	}
	c := *got
	return &c, nil
}

func (r *cstGuardedInstances) RecordRunID(ctx context.Context, tenantID, id uuid.UUID, runID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.runIDWrites = append(r.runIDWrites, cstCall{tenantID, id})
	return r.fakeInstanceRepo.RecordRunID(ctx, tenantID, id, runID)
}

// cstOpenRuns plays Temporal for a repeated workflow id: the first start opens
// a run, a later start of the same id is given the run that is already open.
type cstOpenRuns struct {
	mu     sync.Mutex
	gate   *cstBarrier
	starts []port.StartWorkflowInput
	runs   map[string]string
}

func (o *cstOpenRuns) start(_ context.Context, in port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
	o.mu.Lock()
	o.starts = append(o.starts, in)
	o.mu.Unlock()
	if !o.gate.wait() {
		return port.StartWorkflowOutput{}, errors.New("the other start never reached StartWorkflow")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	run, open := o.runs[in.TemporalWorkflowID]
	if !open {
		run = "run-" + uuid.NewString()
		o.runs[in.TemporalWorkflowID] = run
	}
	return port.StartWorkflowOutput{TemporalWorkflowID: in.TemporalWorkflowID, TemporalRunID: run}, nil
}

func TestInstanceService_Start_LoserOfTheInsertRaceLaunchesTheWinnersWorkflowIDAgain(t *testing.T) {
	tenantID, versionID, caller := uuid.New(), uuid.New(), uuid.New()
	svc, instances, _, _, outbox, temporal, definitions, _ := newInstanceServiceHarness()
	definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))

	guarded := &cstGuardedInstances{fakeInstanceRepo: instances, lookupGate: cstNewBarrier(2)}
	svc.Instances = guarded
	runs := &cstOpenRuns{gate: cstNewBarrier(2), runs: map[string]string{}}
	temporal.startFunc = runs.start

	type result struct {
		inst *port.Instance
		err  error
	}
	results := make([]result, 2)
	var wg sync.WaitGroup
	for i := range results {
		wg.Add(1)
		go func() {
			defer wg.Done()
			in := startfixInput(tenantID, versionID, caller)
			results[i].inst, results[i].err = svc.Start(context.Background(), in)
		}()
	}
	wg.Wait()

	require.NoError(t, results[0].err)
	require.NoError(t, results[1].err)
	require.Len(t, guarded.byID, 1, "the loser inserts no second instance row")
	var stored *domain.Instance
	for _, inst := range guarded.byID {
		stored = inst
	}

	var wins, losses int
	for _, err := range guarded.createErrs {
		switch {
		case err == nil:
			wins++
		case errors.Is(err, domain.ErrDuplicateBusinessKey):
			losses++
		}
	}
	assert.Equal(t, 1, wins, "one insert wins")
	assert.Equal(t, 1, losses, "the other insert hits the unique key")
	assert.Equal(t, 3, guarded.lookups, "both look before inserting, and the loser looks again after losing")
	for _, k := range guarded.lookupKeys {
		assert.Equal(t, csbLookup{tenantID, "SUBJ-1", "TND-001"}, k)
	}

	assert.Equal(t, stored.ID, results[0].inst.ID)
	assert.Equal(t, stored.ID, results[1].inst.ID, "both callers get the same instance")

	require.Len(t, runs.starts, 2, "the loser launches the workflow itself")
	for _, in := range runs.starts {
		assert.Equal(t, stored.TemporalWorkflowID, in.TemporalWorkflowID, "both launches use the one stored workflow id")
		assert.Equal(t, stored.ID, in.InstanceID)
		assert.Equal(t, tenantID, in.TenantID)
		assert.Equal(t, versionID, in.WorkflowVersionID)
	}
	assert.Equal(t, "tenant:"+tenantID.String()+":instance:"+stored.ID.String(), stored.TemporalWorkflowID)
	require.Len(t, runs.runs, 1, "one workflow is open for the id")
	run := runs.runs[stored.TemporalWorkflowID]
	assert.Equal(t, run, results[0].inst.TemporalRunID)
	assert.Equal(t, run, results[1].inst.TemporalRunID)
	assert.Equal(t, run, stored.TemporalRunID, "the one run is recorded on the row")

	require.Len(t, guarded.runIDWrites, 2)
	for _, w := range guarded.runIDWrites {
		assert.Equal(t, cstCall{tenantID, stored.ID}, w)
	}
	assert.Len(t, outbox.enqueued, 1, "only the winner's insert wrote a started event")
	assert.Empty(t, temporal.signals)
}
