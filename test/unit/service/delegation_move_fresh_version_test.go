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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// fvTasks hands out copies of the stored task and bumps the stored record version
// after the first read, as a claim or completion landing right after it would.
type fvTasks struct {
	*fakeTaskRepo
	reads int
}

func (r *fvTasks) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Task, error) {
	task, err := r.fakeTaskRepo.GetByID(ctx, tenantID, id)
	if err != nil {
		return nil, err
	}
	read := *task
	r.reads++
	if r.reads == 1 {
		task.RecordVersion++
	}
	return &read, nil
}

// fvAssignments keeps the real SetLead contract: a stale task version is a conflict.
type fvAssignments struct {
	*fakeAssignmentRepo
	tasks  *fakeTaskRepo
	claims int
}

func (r *fvAssignments) ClaimLead(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, int64) (*domain.TaskAssignment, error) {
	r.claims++
	return nil, domain.ErrNotFound
}

func (r *fvAssignments) SetLead(ctx context.Context, tenantID, taskID, id uuid.UUID, version int64) (*domain.TaskAssignment, error) {
	if r.tasks.byID[taskID].RecordVersion != version {
		return nil, domain.ErrRecordVersionConflict
	}
	return r.fakeAssignmentRepo.SetLead(ctx, tenantID, taskID, id, version)
}

type fvWorld struct {
	tasks       *fvTasks
	assignments *fvAssignments
	task        *domain.Task
	lead        *domain.TaskAssignment
	holder      *domain.TaskAssignment
	tenantID    uuid.UUID
	from, to    uuid.UUID
}

// newFVWorld has user "from" leading a task that user "to" already holds as a co-assignee.
func newFVWorld(instances *fakeInstanceRepo, tasks *fakeTaskRepo, assignments *fakeAssignmentRepo, tenantID uuid.UUID, reason string) *fvWorld {
	w := &fvWorld{tenantID: tenantID, from: uuid.New(), to: uuid.New()}
	inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, TemporalWorkflowID: tenantID.String() + ":biz", RecordVersion: 1}
	instances.byID[inst.ID] = inst
	w.task = &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: inst.ID, NodeKey: fx10ReviewNode, RecordVersion: 3}
	tasks.byID[w.task.ID] = w.task
	w.lead = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: w.task.ID, UserID: w.from, IsActive: true, IsLead: true, Reason: reason}
	w.holder = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: w.task.ID, UserID: w.to, IsActive: true}
	assignments.add(w.lead)
	assignments.add(w.holder)
	w.tasks = &fvTasks{fakeTaskRepo: tasks}
	w.assignments = &fvAssignments{fakeAssignmentRepo: assignments, tasks: tasks}
	return w
}

func (w *fvWorld) requireHandedOver(t *testing.T, err error) {
	t.Helper()
	require.NoError(t, err, "a version bump between the read and the transaction must not fail the move")
	assert.False(t, w.lead.IsActive)
	assert.True(t, w.holder.IsLead, "the user who already held the task inherits the lead")
	assert.Zero(t, w.assignments.claims, "a lead handed over by a delegation move did not claim the task")
}

func TestDelegationMoves_ReReadTheTaskInsideTheTransactionBeforeHandingTheLeadOver(t *testing.T) {
	t.Run("reassign to a delegate who already holds the task", func(t *testing.T) {
		instances, tasks, assignments, outbox := newFakeInstanceRepo(), newFakeTaskRepo(), newFakeAssignmentRepo(), &fakeOutbox{}
		w := newFVWorld(instances, tasks, assignments, uuid.New(), "delegation:"+uuid.NewString())
		svc := &service.WorkflowClient{Instances: instances, Tasks: w.tasks, Assignments: w.assignments, Transactor: fakeTransactor{}, Outbox: outbox, Signals: outbox, Validator: realValidator(), Ended: newFakeEndedDelegations(), Locks: &fakeLocker{}}
		fx10AllowClientEligibility(svc, uuid.Nil)

		count, err := svc.ReassignDelegate(context.Background(), port.ReassignDelegateInput{TenantID: w.tenantID, OldDelegateID: w.from, NewDelegateID: w.to})

		w.requireHandedOver(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("cancel that returns the work to a delegator who already holds the task", func(t *testing.T) {
		instances, tasks, assignments, outbox := newFakeInstanceRepo(), newFakeTaskRepo(), newFakeAssignmentRepo(), &fakeOutbox{}
		w := newFVWorld(instances, tasks, assignments, uuid.New(), "")
		w.lead.Reason = dcrHop(uuid.New(), w.to)
		svc := &service.WorkflowClient{Instances: instances, Tasks: w.tasks, Assignments: w.assignments, Transactor: fakeTransactor{}, Outbox: outbox, Signals: outbox, Validator: realValidator(), Ended: newFakeEndedDelegations(), Locks: &fakeLocker{}}
		fx10AllowClientEligibility(svc, uuid.Nil)

		count, err := svc.CancelByDelegate(context.Background(), port.CancelByDelegateInput{TenantID: w.tenantID, DelegateUserID: w.from})

		w.requireHandedOver(t, err)
		assert.Equal(t, 1, count)
	})

	t.Run("reroute to a delegate who already holds the task", func(t *testing.T) {
		svc, instances, tasks, assignments, _, _, definitions, _ := newDelegationReconcilerHarness()
		tenantID := uuid.New()
		w := newFVWorld(instances, tasks, assignments, tenantID, "")
		inst := instances.byID[w.task.WorkflowInstanceID]
		versionID := uuid.New()
		inst.WorkflowVersionID = versionID
		w.task.NodeKey = "finance/review"
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}))
		svc.Tasks, svc.Assignments = w.tasks, w.assignments

		err := svc.Reroute(context.Background(), port.DelegationRerouteInput{TenantID: tenantID, DelegationID: uuid.New(), DelegatorID: w.from, DelegateID: w.to, Scope: "all"})

		w.requireHandedOver(t, err)
	})
}
