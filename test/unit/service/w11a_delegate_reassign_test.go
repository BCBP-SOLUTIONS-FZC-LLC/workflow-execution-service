package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

var errW11aEligibilityDown = errors.New("w11a: membership service returned 503")

// w11aTx runs the callback and records the tenant on its context and whether a
// callback is running.
type w11aTx struct {
	tenants []string
	running bool
}

func (t *w11aTx) RunInTx(ctx context.Context, fn func(context.Context) error) error {
	tenant := ""
	if g, ok := pgcommon.GUCSetFromContext(ctx); ok {
		tenant = g.TenantID
	}
	t.tenants = append(t.tenants, tenant)
	t.running = true
	defer func() { t.running = false }()
	return fn(ctx)
}

func (t *w11aTx) RunInTxWithRetry(ctx context.Context, fn func(context.Context) error) error {
	return t.RunInTx(ctx, fn)
}

// w11aScopedTasks answers only for the tenant a row belongs to, as row-level
// security does.
type w11aScopedTasks struct {
	*fakeTaskRepo
	reads []w11aRead
}

func (r *w11aScopedTasks) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Task, error) {
	r.reads = append(r.reads, w11aRead{tenantID, id})
	task, err := r.fakeTaskRepo.GetByID(ctx, tenantID, id)
	if err != nil {
		return nil, err
	}
	if task.TenantID != tenantID {
		return nil, domain.ErrNotFound
	}
	return task, nil
}

type w11aListCall struct{ Tenant, User uuid.UUID }

type w11aListedAssignments struct {
	*fakeAssignmentRepo
	listed   []w11aListCall
	vacated  []w11aRead
	inserted []domain.TaskAssignment
}

func (r *w11aListedAssignments) VacateActive(ctx context.Context, tenantID, id uuid.UUID) (*domain.TaskAssignment, error) {
	r.vacated = append(r.vacated, w11aRead{tenantID, id})
	return r.fakeAssignmentRepo.VacateActive(ctx, tenantID, id)
}

func (r *w11aListedAssignments) Create(ctx context.Context, a *domain.TaskAssignment) error {
	r.inserted = append(r.inserted, *a)
	return r.fakeAssignmentRepo.Create(ctx, a)
}

func (r *w11aListedAssignments) ListActiveByUser(ctx context.Context, tenantID, userID uuid.UUID) ([]*domain.TaskAssignment, error) {
	r.listed = append(r.listed, w11aListCall{tenantID, userID})
	return r.fakeAssignmentRepo.ListActiveByUser(ctx, tenantID, userID)
}

type w11aEligibilityCall struct {
	Tenant, User, Department uuid.UUID
	Level                    string
}

type w11aEligibility struct {
	calls []w11aEligibilityCall
	err   error
}

func (e *w11aEligibility) CheckEligibility(_ context.Context, tenantID, userID, departmentID uuid.UUID, level string) (bool, error) {
	e.calls = append(e.calls, w11aEligibilityCall{tenantID, userID, departmentID, level})
	if e.err != nil {
		return false, e.err
	}
	return true, nil
}

func (e *w11aEligibility) CheckEligibilityBatch(context.Context, uuid.UUID, []port.EligibilityCheckRequest) ([]port.EligibilityResult, error) {
	return nil, errors.New("w11a: batch is not used by a delegate reassignment")
}

type w11aReassign struct {
	svc         *service.WorkflowClient
	instances   *fakeInstanceRepo
	tasks       *w11aScopedTasks
	assignments *w11aListedAssignments
	outbox      *fakeOutbox
	tx          *w11aTx
	definitions *fakeDefinitionClient
	eligibility *w11aEligibility
	tenant      uuid.UUID
	oldDelegate uuid.UUID
	newDelegate uuid.UUID
	department  uuid.UUID
	nodeKey     string
}

type w11aHeld struct {
	instance   *domain.Instance
	task       *domain.Task
	assignment *domain.TaskAssignment
}

func w11aNewReassign() *w11aReassign {
	h := &w11aReassign{
		instances:   newFakeInstanceRepo(),
		tasks:       &w11aScopedTasks{fakeTaskRepo: newFakeTaskRepo()},
		assignments: &w11aListedAssignments{fakeAssignmentRepo: newFakeAssignmentRepo()},
		outbox:      &fakeOutbox{},
		tx:          &w11aTx{},
		definitions: &fakeDefinitionClient{},
		eligibility: &w11aEligibility{},
		tenant:      uuid.New(), oldDelegate: uuid.New(), newDelegate: uuid.New(),
		department: uuid.New(), nodeKey: "finance/review",
	}
	h.svc = &service.WorkflowClient{
		Instances: h.instances, Tasks: h.tasks, Assignments: h.assignments, Transactor: h.tx,
		Outbox: h.outbox, Signals: h.outbox, Validator: realValidator(), Ended: newFakeEndedDelegations(),
		Locks: &fakeLocker{}, Log: &fakeLogger{},
	}
	return h
}

// withEligibility turns the membership check on, against a plan whose only stage
// needs the reviewer level in the department h.department.
func (h *w11aReassign) withEligibility(t *testing.T) uuid.UUID {
	t.Helper()
	versionID := uuid.New()
	stage := dsl.StageDef{NodeID: "review", Role: "reviewer"}
	h.nodeKey = dsl.NodeKey("finance", &stage)
	plan := compiledPlanJSONWithDept(t, dsl.DepartmentDef{ID: "finance", IAMDepartmentID: h.department.String(), Label: "Finance", Stages: []dsl.StageDef{stage}})
	h.definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, plan)
	h.svc.Definitions = h.definitions
	h.svc.Eligibility = h.eligibility
	return versionID
}

// hold gives the old delegate an active assignment with the given reason on a new
// task of a new instance.
func (h *w11aReassign) hold(versionID uuid.UUID, recordVersion int64, reason string) w11aHeld {
	inst := &domain.Instance{
		ID: uuid.New(), TenantID: h.tenant, WorkflowVersionID: versionID,
		TemporalWorkflowID: h.tenant.String() + ":" + uuid.NewString(), RecordVersion: 1,
	}
	h.instances.byID[inst.ID] = inst
	task := &domain.Task{
		ID: uuid.New(), TenantID: h.tenant, WorkflowInstanceID: inst.ID, NodeKey: h.nodeKey,
		DepartmentID: h.department, RecordVersion: recordVersion,
	}
	h.tasks.byID[task.ID] = task
	a := &domain.TaskAssignment{ID: uuid.New(), TenantID: h.tenant, TaskID: task.ID, UserID: h.oldDelegate, IsActive: true, Reason: reason}
	h.assignments.add(a)
	return w11aHeld{instance: inst, task: task, assignment: a}
}

func (h *w11aReassign) reassign(delegationID *uuid.UUID) (int, error) {
	return h.svc.ReassignDelegate(context.Background(), port.ReassignDelegateInput{
		TenantID: h.tenant, OldDelegateID: h.oldDelegate, NewDelegateID: h.newDelegate, DelegationID: delegationID,
	})
}

func (h *w11aReassign) holds(taskID, userID uuid.UUID) bool {
	for _, a := range h.assignments.byID {
		if a.TaskID == taskID && a.UserID == userID && a.IsActive {
			return true
		}
	}
	return false
}

func TestWorkflowClient_ReassignDelegate_DelegationMatchesNoAssignment_MovesNothingAndCountsZero(t *testing.T) {
	t.Parallel()

	h := w11aNewReassign()
	named, other := uuid.New(), uuid.New()
	tagged := h.hold(uuid.New(), 3, "delegation:"+other.String()+":"+uuid.NewString())
	untagged := h.hold(uuid.New(), 3, "manual-assign")

	count, err := h.reassign(&named)

	require.NoError(t, err)
	assert.Zero(t, count, "a delegation that holds none of the old delegate's work moves none of it")
	assert.Equal(t, []w11aListCall{{h.tenant, h.oldDelegate}}, h.assignments.listed, "the old delegate's work was read once, for the caller's tenant")
	assert.Empty(t, h.assignments.vacated)
	assert.Empty(t, h.assignments.inserted)
	assert.Empty(t, h.tx.tenants, "no transaction is opened for nothing")
	assert.Empty(t, h.tasks.reads, "no task of an unmatched row is read")
	assert.Empty(t, h.outbox.signals)
	assert.Empty(t, h.outbox.enqueued)
	assert.True(t, h.holds(tagged.task.ID, h.oldDelegate), "another delegation's row stays")
	assert.True(t, h.holds(untagged.task.ID, h.oldDelegate), "an untagged row stays")
	assert.False(t, h.holds(tagged.task.ID, h.newDelegate))
	assert.False(t, h.holds(untagged.task.ID, h.newDelegate))
}

func TestWorkflowClient_ReassignDelegate_EligibilityReadFails_MovesNothingAndRetrySucceeds(t *testing.T) {
	t.Parallel()

	h := w11aNewReassign()
	versionID := h.withEligibility(t)
	held := h.hold(versionID, 5, "delegation:"+uuid.NewString())
	h.eligibility.err = errW11aEligibilityDown

	count, err := h.reassign(nil)

	require.ErrorIs(t, err, port.ErrEligibilityRead, "the handler maps this family to 503 and counts it against the membership service")
	require.ErrorIs(t, err, errW11aEligibilityDown, "the cause is kept")
	assert.Zero(t, count, "the count is discarded so the caller retries the event")
	assert.Empty(t, h.tx.tenants, "every read is done before the transaction, so there is nothing to roll back")
	assert.True(t, h.holds(held.task.ID, h.oldDelegate))
	assert.False(t, h.holds(held.task.ID, h.newDelegate))
	assert.Empty(t, h.outbox.signals)
	assert.Empty(t, h.outbox.enqueued)
	require.Len(t, h.eligibility.calls, 1)
	assert.Equal(t, w11aEligibilityCall{Tenant: h.tenant, User: h.newDelegate, Department: held.task.DepartmentID, Level: "reviewer"}, h.eligibility.calls[0],
		"the new delegate is the one asked about, for the task's department")

	h.eligibility.err = nil
	count, err = h.reassign(nil)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "the redelivered event moves the row")
	assert.True(t, h.holds(held.task.ID, h.newDelegate))
}

type w11aReassignWire struct {
	TaskID        string
	OldUserID     string
	NewUserID     string
	RecordVersion int64
	RecordedAt    time.Time
}

func TestWorkflowClient_ReassignDelegate_RecordsOneSignalPerMovedTaskOnItsInstanceWorkflow(t *testing.T) {
	t.Parallel()

	h := w11aNewReassign()
	first := h.hold(uuid.New(), 7, "delegation:"+uuid.NewString())
	second := h.hold(uuid.New(), 9, "delegation:"+uuid.NewString())
	var runningAtRecord []bool
	h.outbox.onSignal = func() { runningAtRecord = append(runningAtRecord, h.tx.running) }
	before := time.Now().UTC()

	count, err := h.reassign(nil)

	require.NoError(t, err)
	require.Equal(t, 2, count)
	require.Len(t, h.outbox.signals, 2)
	assert.Equal(t, []bool{true, true}, runningAtRecord, "each signal is recorded inside the move's transaction")
	assert.Equal(t, []string{h.tenant.String()}, h.tx.tenants, "one transaction on the caller's tenant")
	assert.Equal(t, []uuid.UUID{h.tenant, h.tenant}, h.outbox.signalTenants)
	assert.Len(t, h.outbox.enqueued, 2, "the reassigned events are separate rows from the signals")

	for _, held := range []w11aHeld{first, second} {
		var found []signalCall
		for _, s := range h.outbox.signals {
			if w11aDecode[w11aReassignWire](t, s.Payload).TaskID == held.task.ID.String() {
				found = append(found, s)
			}
		}
		require.Len(t, found, 1, "one signal for each moved task")
		assert.Equal(t, port.SignalInstanceReassign, found[0].SignalName)
		assert.Equal(t, held.instance.TemporalWorkflowID, found[0].TemporalWorkflowID, "recorded on the task's own workflow")
		assert.Equal(t, held.instance.ID, found[0].InstanceID)
		wire := w11aDecode[w11aReassignWire](t, found[0].Payload)
		assert.Equal(t, h.oldDelegate.String(), wire.OldUserID)
		assert.Equal(t, h.newDelegate.String(), wire.NewUserID)
		assert.Equal(t, held.task.RecordVersion, wire.RecordVersion, "the version read before the move")
		assert.False(t, wire.RecordedAt.Before(before))
		assert.False(t, wire.RecordedAt.After(time.Now().UTC()))
	}
	assert.Equal(t, []w11aRead{{h.tenant, first.assignment.ID}, {h.tenant, second.assignment.ID}}, h.assignments.vacated,
		"each old row is vacated under the caller's tenant")
	require.Len(t, h.assignments.inserted, 2)
	for i, held := range []w11aHeld{first, second} {
		assert.Equal(t, h.tenant, h.assignments.inserted[i].TenantID)
		assert.Equal(t, held.task.ID, h.assignments.inserted[i].TaskID)
		assert.Equal(t, h.newDelegate, h.assignments.inserted[i].UserID)
	}
	assert.Equal(t, int64(7), first.task.RecordVersion)
	assert.Equal(t, int64(9), second.task.RecordVersion)
}
