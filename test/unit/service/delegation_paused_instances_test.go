package service_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// csaDefinitions answers from a script and records the tenant and version of
// every read.
type csaDefinitions struct {
	mu     sync.Mutex
	reads  []csaIDCall
	answer func(call int) (*port.CompiledWorkflow, error)
}

func (d *csaDefinitions) GetCompiledWorkflow(_ context.Context, tenantID, versionID uuid.UUID) (*port.CompiledWorkflow, error) {
	d.mu.Lock()
	d.reads = append(d.reads, csaIDCall{tenantID, versionID})
	call := len(d.reads)
	d.mu.Unlock()
	return d.answer(call)
}

func csaAlwaysAnswer(resp *port.CompiledWorkflow) func(int) (*port.CompiledWorkflow, error) {
	return func(int) (*port.CompiledWorkflow, error) { return resp, nil }
}

// csaFailingTransactor fails the transaction before its function runs and counts
// the attempts.
type csaFailingTransactor struct {
	err   error
	mu    sync.Mutex
	calls int
}

func (f *csaFailingTransactor) RunInTx(context.Context, func(context.Context) error) error {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	return f.err
}

func (f *csaFailingTransactor) RunInTxWithRetry(ctx context.Context, fn func(context.Context) error) error {
	return f.RunInTx(ctx, fn)
}

func csaSignalsNamed(temporal *fakeTemporalClient, name string) []signalCall {
	var out []signalCall
	for _, s := range temporal.signals {
		if s.SignalName == name {
			out = append(out, s)
		}
	}
	return out
}

func TestDelegationReconciler_Reroute_PausedInstanceGetsTheReassignSignal(t *testing.T) {
	svc, instances, tasks, assignments, outbox, temporal, definitions, _ := newDelegationReconcilerHarness()
	tenantID, delegatorID, delegateID, delegationID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	inst, task, held := seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, delegatorID, "biz-paused", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)
	// The delegator went out of office with no delegate, so the instance was paused.
	inst.Status = domain.InstanceStatusPaused
	recording := csaWrapAssignments(assignments)
	svc.Assignments = recording
	scopedTasks, scopedInstances := &csaTasks{fakeTaskRepo: tasks}, &csaInstances{fakeInstanceRepo: instances}
	svc.Tasks, svc.Instances = scopedTasks, scopedInstances

	err := svc.Reroute(context.Background(), port.DelegationRerouteInput{
		TenantID: tenantID, DelegationID: delegationID, DelegatorID: delegatorID, DelegateID: delegateID, Scope: "all",
	})

	require.NoError(t, err)
	require.Len(t, temporal.signals, 1, "only the reassign signal is sent")
	sent := temporal.signals[0]
	assert.Equal(t, port.SignalInstanceReassign, sent.SignalName)
	assert.Equal(t, inst.ID, sent.InstanceID)
	assert.Equal(t, inst.TemporalWorkflowID, sent.TemporalWorkflowID)
	assert.Equal(t, csaReassignWire{TaskID: task.ID.String(), OldUserID: delegatorID.String(), NewUserID: delegateID.String()}, csaDecodeReassign(t, sent.Payload))
	assert.Equal(t, []csaIDCall{{tenantID, task.ID}}, scopedTasks.reads, "the task is read in the caller's tenant")
	assert.Equal(t, []csaIDCall{{tenantID, inst.ID}}, scopedInstances.reads, "the instance is read in the caller's tenant")

	assert.False(t, held.IsActive, "the task moved off the delegator")
	assert.Equal(t, []csaUserCall{{tenantID, delegatorID}}, recording.listed)
	assert.Equal(t, []csaIDCall{{tenantID, held.ID}}, recording.vacated)
	assert.Equal(t, []csaCreateCall{{tenantID, task.ID, delegateID}}, recording.created)
	assert.Equal(t, "delegation:"+delegationID.String(), recording.creation[0].Reason)
	require.Len(t, outbox.enqueued, 1)
	assert.Equal(t, domain.EventWorkflowTaskReassigned, outbox.enqueued[0].Type)
}

func TestDelegationReconciler_Reverse_AFailedRestorationChangesNothingAndTheRedeliveryCommits(t *testing.T) {
	svc, instances, tasks, assignments, outbox, temporal, definitions, _ := newDelegationReconcilerHarness()
	tenantID, delegatorID, delegateID, delegationID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	inst, task, held := seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, delegateID, "biz-restore", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)
	inst.Status = domain.InstanceStatusRunning
	held.Reason = "delegation:" + delegationID.String()
	store := newFakeUserAvailability()
	require.NoError(t, store.byRecord(tenantID, delegatorID, port.AvailabilityOOO))
	svc.Availability = store
	recording := csaWrapAssignments(assignments)
	svc.Assignments = recording
	scopedTasks, scopedInstances := &csaTasks{fakeTaskRepo: tasks}, &csaInstances{fakeInstanceRepo: instances}
	svc.Tasks, svc.Instances = scopedTasks, scopedInstances
	transactor := &csaFailingTransactor{err: assert.AnError}
	svc.Transactor = transactor
	in := port.DelegationReversalInput{
		TenantID: tenantID, DelegationID: delegationID, DelegatorID: delegatorID, DelegateID: delegateID, EndedReason: "expired",
	}

	err := svc.Reverse(context.Background(), in)

	require.ErrorIs(t, err, assert.AnError, "the event is redelivered")
	assert.Equal(t, 1, transactor.calls)
	assert.Equal(t, []csaIDCall{{tenantID, task.ID}}, scopedTasks.reads, "the task is read in the caller's tenant")
	assert.Equal(t, []csaIDCall{{tenantID, inst.ID}}, scopedInstances.reads, "the instance is read in the caller's tenant")
	assert.Empty(t, csaSignalsNamed(temporal, port.SignalInstanceReassign), "the restoration did not commit, so nothing is reassigned")
	assert.True(t, held.IsActive, "the task is still with the delegate")
	assert.Empty(t, recording.created, "no replacement was written")
	assert.Empty(t, outbox.enqueued)

	// The redelivery commits the restoration.
	svc.Transactor = fakeTransactor{}
	require.NoError(t, svc.Reverse(context.Background(), in))
	assert.False(t, held.IsActive)
	assert.Equal(t, []csaCreateCall{{tenantID, task.ID, delegatorID}}, recording.created, "the delegator holds the task after the redelivery")
}

func TestDelegationReconciler_Reroute_PlanReadsPerEvent(t *testing.T) {
	obstest.Init(t)
	const missesMetric = "workflow_execution_service_wf_cache_misses_total"
	const hitsMetric = "workflow_execution_service_wf_cache_hits_total"

	setup := func(t *testing.T) (*csaDefinitionsHarness, string) {
		svc, instances, tasks, assignments, _, _, _, _ := newDelegationReconcilerHarness()
		tenantID, delegatorID := uuid.New(), uuid.New()
		versionID := uuid.New()
		iamDeptID := uuid.New().String()
		definitions := &csaDefinitions{answer: csaAlwaysAnswer(publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSONWithDept(t,
			dsl.DepartmentDef{ID: "finance", IAMDepartmentID: iamDeptID, Stages: []dsl.StageDef{{Type: "userTask", NodeID: "review", Role: "reviewer"}}})))}
		svc.Definitions = definitions
		inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, WorkflowVersionID: versionID, BusinessKey: "biz-reads", TemporalWorkflowID: tenantID.String() + ":biz-reads"}
		instances.byID[inst.ID] = inst
		for i := 0; i < 2; i++ {
			task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: inst.ID, NodeKey: "finance/review", DepartmentID: uuid.MustParse(iamDeptID), RecordVersion: 1}
			tasks.byID[task.ID] = task
			assignments.add(&domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: delegatorID, IsActive: true})
		}
		return &csaDefinitionsHarness{svc: svc, definitions: definitions, tenantID: tenantID, delegatorID: delegatorID, versionID: versionID}, iamDeptID
	}
	run := func(t *testing.T, h *csaDefinitionsHarness, scope string, scopeID *string) (misses, hits float64) {
		missesBefore := obstest.Sum(t, missesMetric, nil)
		hitsBefore := obstest.Sum(t, hitsMetric, nil)
		require.NoError(t, h.svc.Reroute(context.Background(), port.DelegationRerouteInput{
			TenantID: h.tenantID, DelegationID: uuid.New(), DelegatorID: h.delegatorID, DelegateID: uuid.New(), Scope: scope, ScopeID: scopeID,
		}))
		return obstest.Sum(t, missesMetric, nil) - missesBefore, obstest.Sum(t, hitsMetric, nil) - hitsBefore
	}

	t.Run("scope all reads the plan once for the two tasks of one version", func(t *testing.T) {
		h, _ := setup(t)

		misses, hits := run(t, h, "all", nil)

		assert.Equal(t, []csaIDCall{{h.tenantID, h.versionID}}, h.definitions.reads)
		assert.Equal(t, 1.0, misses)
		assert.Equal(t, 1.0, hits)
	})

	t.Run("a department scope builds a second plan cache, so the version is read twice", func(t *testing.T) {
		h, iamDeptID := setup(t)

		misses, hits := run(t, h, "department", &iamDeptID)

		assert.Equal(t, []csaIDCall{{h.tenantID, h.versionID}, {h.tenantID, h.versionID}}, h.definitions.reads,
			"the scope check and the eligibility check each read the plan")
		assert.Equal(t, 2.0, misses)
		assert.Equal(t, 2.0, hits, "each cache answers its second task from memory")
	})
}

type csaDefinitionsHarness struct {
	svc         csaRerouter
	definitions *csaDefinitions
	tenantID    uuid.UUID
	delegatorID uuid.UUID
	versionID   uuid.UUID
}

type csaRerouter interface {
	Reroute(ctx context.Context, in port.DelegationRerouteInput) error
}

func csaPayloadJSON(t *testing.T, payload any) string {
	t.Helper()
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	return string(raw)
}
