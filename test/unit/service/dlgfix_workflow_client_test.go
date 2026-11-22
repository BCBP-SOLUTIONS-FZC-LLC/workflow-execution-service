package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// dlgfixTx is a transactor that behaves like a real one for assignment rows: a
// failing callback puts every row back as it was. It records the tenant on the
// context it was handed and whether a callback is still running, so a test can
// tell what happened before and after the commit.
type dlgfixTx struct {
	repo      *fakeAssignmentRepo
	outbox    *fakeOutbox
	tenants   []string
	running   bool
	commits   int
	rollbacks int
	// aborted is set by a unique violation: like Postgres without savepoints,
	// every later statement of the transaction fails and it cannot commit.
	aborted bool
	// onStart runs as the transaction opens, to change rows the caller read earlier.
	onStart func()
}

var errTxAborted = errors.New("current transaction is aborted, commands ignored until end of transaction block")

type dlgfixRow struct {
	row    domain.TaskAssignment
	target *domain.TaskAssignment
}

func (t *dlgfixTx) RunInTx(ctx context.Context, fn func(context.Context) error) error {
	tenant := ""
	if g, ok := pgcommon.GUCSetFromContext(ctx); ok {
		tenant = g.TenantID
	}
	t.tenants = append(t.tenants, tenant)
	t.aborted = false
	if t.onStart != nil {
		t.onStart()
	}
	saved := make(map[uuid.UUID]dlgfixRow, len(t.repo.byID))
	for id, a := range t.repo.byID {
		saved[id] = dlgfixRow{row: *a, target: a}
	}
	recorded, events := 0, 0
	if t.outbox != nil {
		recorded, events = len(t.outbox.signals), len(t.outbox.enqueued)
	}
	t.running = true
	err := fn(ctx)
	t.running = false
	if err == nil && t.aborted {
		err = errTxAborted
	}
	if err == nil {
		t.commits++
		return nil
	}
	t.rollbacks++
	if t.outbox != nil {
		t.outbox.signals = t.outbox.signals[:recorded]
		t.outbox.signalTenants = t.outbox.signalTenants[:recorded]
		t.outbox.enqueued = t.outbox.enqueued[:events]
	}
	for id := range t.repo.byID {
		if _, kept := saved[id]; !kept {
			delete(t.repo.byID, id)
		}
	}
	for id, s := range saved {
		*s.target = s.row
		t.repo.byID[id] = s.target
	}
	return err
}

func (t *dlgfixTx) RunInTxWithRetry(ctx context.Context, fn func(context.Context) error) error {
	return t.RunInTx(ctx, fn)
}

// dlgfixAssignments fails the vacate or create of chosen rows.
type dlgfixAssignments struct {
	*fakeAssignmentRepo
	tx          *dlgfixTx
	vacateFails map[uuid.UUID]error
	createFails map[uuid.UUID]error
	vacated     []uuid.UUID
	created     []*domain.TaskAssignment
	// afterList runs once the active rows of a task were read, so a test can end
	// one before the caller writes, as a concurrent complete would.
	afterList func(taskID uuid.UUID)
}

func (r *dlgfixAssignments) inAbortedTx() bool { return r.tx != nil && r.tx.aborted }

func (r *dlgfixAssignments) ListActiveByTask(ctx context.Context, tenantID, taskID uuid.UUID) ([]*domain.TaskAssignment, error) {
	if r.inAbortedTx() {
		return nil, errTxAborted
	}
	rows, err := r.fakeAssignmentRepo.ListActiveByTask(ctx, tenantID, taskID)
	if r.afterList != nil {
		r.afterList(taskID)
	}
	return rows, err
}

func (r *dlgfixAssignments) SetLead(ctx context.Context, tenantID, taskID, id uuid.UUID, version int64) (*domain.TaskAssignment, error) {
	if r.inAbortedTx() {
		return nil, errTxAborted
	}
	return r.fakeAssignmentRepo.SetLead(ctx, tenantID, taskID, id, version)
}

func (r *dlgfixAssignments) VacateActive(ctx context.Context, tenantID, id uuid.UUID) (*domain.TaskAssignment, error) {
	if r.inAbortedTx() {
		return nil, errTxAborted
	}
	r.vacated = append(r.vacated, id)
	if err := r.vacateFails[id]; err != nil {
		return nil, err
	}
	return r.fakeAssignmentRepo.VacateActive(ctx, tenantID, id)
}

func (r *dlgfixAssignments) Create(ctx context.Context, a *domain.TaskAssignment) error {
	if r.inAbortedTx() {
		return errTxAborted
	}
	if err := r.createFails[a.TaskID]; err != nil {
		return err
	}
	err := r.fakeAssignmentRepo.Create(ctx, a)
	if errors.Is(err, domain.ErrDuplicateActiveAssignment) && r.tx != nil {
		r.tx.aborted = true
	}
	if err == nil {
		r.created = append(r.created, a)
	}
	return err
}

// dlgfixDefinitions answers per version, and fails the versions it was told to.
type dlgfixDefinitions struct {
	byVersion map[uuid.UUID]*port.CompiledWorkflow
	failing   map[uuid.UUID]error
}

func (d *dlgfixDefinitions) GetCompiledWorkflow(_ context.Context, _, versionID uuid.UUID) (*port.CompiledWorkflow, error) {
	if err := d.failing[versionID]; err != nil {
		return nil, err
	}
	return d.byVersion[versionID], nil
}

type dlgfixClient struct {
	svc         *service.WorkflowClient
	instances   *fakeInstanceRepo
	tasks       *fakeTaskRepo
	assignments *dlgfixAssignments
	outbox      *fakeOutbox
	tx          *dlgfixTx
	definitions *dlgfixDefinitions
	tenantID    uuid.UUID
	oldDelegate uuid.UUID
	newDelegate uuid.UUID
}

type dlgfixHeld struct {
	instance   *domain.Instance
	task       *domain.Task
	assignment *domain.TaskAssignment
	versionID  uuid.UUID
}

func dlgfixNewClient(t *testing.T) *dlgfixClient {
	t.Helper()
	repo := newFakeAssignmentRepo()
	h := &dlgfixClient{
		instances:   newFakeInstanceRepo(),
		tasks:       newFakeTaskRepo(),
		assignments: &dlgfixAssignments{fakeAssignmentRepo: repo, vacateFails: map[uuid.UUID]error{}, createFails: map[uuid.UUID]error{}},
		outbox:      &fakeOutbox{},
		tx:          &dlgfixTx{repo: repo},
		definitions: &dlgfixDefinitions{byVersion: map[uuid.UUID]*port.CompiledWorkflow{}, failing: map[uuid.UUID]error{}},
		tenantID:    uuid.New(), oldDelegate: uuid.New(), newDelegate: uuid.New(),
	}
	h.assignments.tx = h.tx
	h.tx.outbox = h.outbox
	h.svc = &service.WorkflowClient{
		Instances: h.instances, Tasks: h.tasks, Assignments: h.assignments,
		Transactor: h.tx, Outbox: h.outbox, Signals: h.outbox, Validator: realValidator(), Ended: newFakeEndedDelegations(), Locks: &fakeLocker{}, Log: &fakeLogger{},
	}
	h.withEligibility(func(string) (bool, error) { return true, nil })
	return h
}

// withEligibility turns the eligibility check on; answer is asked for every
// distinct department and level.
func (h *dlgfixClient) withEligibility(answer func(level string) (bool, error)) {
	h.svc.Definitions = h.definitions
	h.svc.Eligibility = &fakeEligibilityChecker{check: func(_ context.Context, _, _, _ uuid.UUID, level string) (bool, error) {
		return answer(level)
	}}
}

// seed gives the old delegate one active assignment, created by a delegation,
// on a task of a new instance whose plan has the given stage role.
func (h *dlgfixClient) seed(t *testing.T, role string) dlgfixHeld {
	t.Helper()
	versionID := uuid.New()
	h.definitions.byVersion[versionID] = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, dsl.StageDef{NodeID: "review", Role: role}))
	inst := &domain.Instance{
		ID: uuid.New(), TenantID: h.tenantID, WorkflowVersionID: versionID,
		TemporalWorkflowID: h.tenantID.String() + ":" + uuid.NewString(), RecordVersion: 1,
	}
	h.instances.byID[inst.ID] = inst
	task := &domain.Task{ID: uuid.New(), TenantID: h.tenantID, WorkflowInstanceID: inst.ID, NodeKey: "finance/review", RecordVersion: 1}
	h.tasks.byID[task.ID] = task
	a := &domain.TaskAssignment{ID: uuid.New(), TenantID: h.tenantID, TaskID: task.ID, UserID: h.oldDelegate, IsActive: true, Reason: "delegation:" + uuid.NewString()}
	h.assignments.add(a)
	return dlgfixHeld{instance: inst, task: task, assignment: a, versionID: versionID}
}

func (h *dlgfixClient) reassign() (int, error) {
	return h.svc.ReassignDelegate(context.Background(), port.ReassignDelegateInput{
		TenantID: h.tenantID, OldDelegateID: h.oldDelegate, NewDelegateID: h.newDelegate,
	})
}

func (h *dlgfixClient) cancel() (int, error) {
	return h.svc.CancelByDelegate(context.Background(), port.CancelByDelegateInput{
		TenantID: h.tenantID, DelegateUserID: h.oldDelegate,
	})
}

func (h *dlgfixClient) holdsActive(taskID, userID uuid.UUID) bool {
	for _, a := range h.assignments.byID {
		if a.TaskID == taskID && a.UserID == userID && a.IsActive {
			return true
		}
	}
	return false
}

func (h *dlgfixClient) requireUntouched(t *testing.T, rows ...dlgfixHeld) {
	t.Helper()
	for _, held := range rows {
		assert.True(t, h.holdsActive(held.task.ID, h.oldDelegate), "the old delegate keeps the task")
		assert.False(t, h.holdsActive(held.task.ID, h.newDelegate), "the new delegate was given nothing")
	}
	assert.Empty(t, h.outbox.signals, "no instance is told about a change that did not happen")
}

func TestDelegationCancel_VacatesEveryRowInOneTransactionOnTheTenantContext(t *testing.T) {
	h := dlgfixNewClient(t)
	first, second := h.seed(t, "reviewer"), h.seed(t, "reviewer")

	count, err := h.cancel()

	require.NoError(t, err)
	assert.Equal(t, 2, count)
	assert.False(t, first.assignment.IsActive)
	assert.False(t, second.assignment.IsActive)
	assert.Equal(t, []string{h.tenantID.String()}, h.tx.tenants, "one transaction, opened on a context that carries the tenant")
	assert.Equal(t, 1, h.tx.commits)
}

func TestDelegationCancel_AFailedVacateChangesNothingAndFailsTheCall(t *testing.T) {
	h := dlgfixNewClient(t)
	first, second := h.seed(t, "reviewer"), h.seed(t, "reviewer")
	h.assignments.vacateFails[second.assignment.ID] = assert.AnError

	count, err := h.cancel()

	require.ErrorIs(t, err, assert.AnError, "the caller retries instead of reading a lower count as the answer")
	assert.Zero(t, count)
	assert.True(t, first.assignment.IsActive, "the row vacated before the failure is put back")
	assert.True(t, second.assignment.IsActive)
	assert.Equal(t, 1, h.tx.rollbacks)
}

func TestDelegationReassign_MovesEveryRowAndRecordsEverySignalInOneTransaction(t *testing.T) {
	h := dlgfixNewClient(t)
	first, second := h.seed(t, "reviewer"), h.seed(t, "reviewer")
	var runningAtRecord []bool
	var commitsAtRecord []int
	h.outbox.onSignal = func() {
		runningAtRecord = append(runningAtRecord, h.tx.running)
		commitsAtRecord = append(commitsAtRecord, h.tx.commits)
	}

	count, err := h.reassign()

	require.NoError(t, err)
	assert.Equal(t, 2, count)
	assert.Equal(t, []string{h.tenantID.String()}, h.tx.tenants, "one transaction on a tenant context")
	assert.True(t, h.holdsActive(first.task.ID, h.newDelegate))
	assert.True(t, h.holdsActive(second.task.ID, h.newDelegate))
	assert.Equal(t, []bool{true, true}, runningAtRecord, "every signal is recorded while the transaction is open")
	assert.Equal(t, []int{0, 0}, commitsAtRecord, "and before it commits, so a rollback takes the signals with it")
	assert.Equal(t, []uuid.UUID{h.tenantID, h.tenantID}, h.outbox.signalTenants)
}

func TestDelegationReassign_AFailedWriteChangesNothingAndFailsTheCall(t *testing.T) {
	cases := []struct {
		name   string
		breaks func(h *dlgfixClient, second dlgfixHeld)
	}{
		{"the vacate of the second row fails", func(h *dlgfixClient, second dlgfixHeld) {
			h.assignments.vacateFails[second.assignment.ID] = assert.AnError
		}},
		{"the replacement of the second row cannot be created", func(h *dlgfixClient, second dlgfixHeld) {
			h.assignments.createFails[second.task.ID] = assert.AnError
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := dlgfixNewClient(t)
			first, second := h.seed(t, "reviewer"), h.seed(t, "reviewer")
			tc.breaks(h, second)

			count, err := h.reassign()

			require.ErrorIs(t, err, assert.AnError)
			assert.Zero(t, count)
			h.requireUntouched(t, first, second)
			assert.Equal(t, 1, h.tx.rollbacks)
		})
	}
}

func TestDelegationReassign_AReadThatFailsChangesNothingAndFailsTheCall(t *testing.T) {
	t.Run("a task that cannot be read", func(t *testing.T) {
		h := dlgfixNewClient(t)
		first := h.seed(t, "reviewer")
		orphan := &domain.TaskAssignment{ID: uuid.New(), TenantID: h.tenantID, TaskID: uuid.New(), UserID: h.oldDelegate, IsActive: true, Reason: "delegation:" + uuid.NewString()}
		h.assignments.add(orphan)

		count, err := h.reassign()

		require.ErrorIs(t, err, domain.ErrNotFound)
		assert.Zero(t, count)
		h.requireUntouched(t, first)
		assert.True(t, orphan.IsActive)
		assert.Zero(t, h.tx.commits)
	})

	t.Run("an instance that cannot be read", func(t *testing.T) {
		h := dlgfixNewClient(t)
		first := h.seed(t, "reviewer")
		second := h.seed(t, "reviewer")
		delete(h.instances.byID, second.instance.ID)

		count, err := h.reassign()

		require.ErrorIs(t, err, domain.ErrNotFound)
		assert.Zero(t, count)
		h.requireUntouched(t, first, second)
	})

	t.Run("a compiled plan that cannot be read", func(t *testing.T) {
		h := dlgfixNewClient(t)
		h.withEligibility(func(string) (bool, error) { return true, nil })
		first, second := h.seed(t, "reviewer"), h.seed(t, "reviewer")
		h.definitions.failing[second.versionID] = port.ErrDefinitionUnavailable

		count, err := h.reassign()

		require.ErrorIs(t, err, port.ErrDefinitionUnavailable, "the caller sees the dependency failure and retries")
		assert.Zero(t, count)
		h.requireUntouched(t, first, second)
		assert.Zero(t, h.tx.commits)
	})

	t.Run("an eligibility answer that cannot be read for a later row", func(t *testing.T) {
		h := dlgfixNewClient(t)
		h.withEligibility(func(level string) (bool, error) {
			if level == "approver" {
				return false, assert.AnError
			}
			return true, nil
		})
		first, second := h.seed(t, "reviewer"), h.seed(t, "approver")

		count, err := h.reassign()

		require.ErrorIs(t, err, assert.AnError)
		assert.Zero(t, count)
		h.requireUntouched(t, first, second)
	})
}

func TestDelegationReassign_AnIneligibleRowStaysAndTheRestMove(t *testing.T) {
	h := dlgfixNewClient(t)
	h.withEligibility(func(level string) (bool, error) { return level == "reviewer", nil })
	moved, held := h.seed(t, "reviewer"), h.seed(t, "approver")

	count, err := h.reassign()

	require.NoError(t, err)
	assert.Equal(t, 1, count, "the count is the number of assignments the call vacated")
	assert.True(t, h.holdsActive(moved.task.ID, h.newDelegate))
	assert.True(t, h.holdsActive(held.task.ID, h.oldDelegate), "a row stays only because the new delegate is ineligible for it")
	require.Len(t, h.outbox.signals, 1)
	assert.Equal(t, moved.instance.ID, h.outbox.signals[0].InstanceID)
}

func TestDelegationReassign_AFailedSignalRecordingChangesNothingAndFailsTheCall(t *testing.T) {
	h := dlgfixNewClient(t)
	first, second := h.seed(t, "reviewer"), h.seed(t, "reviewer")
	h.outbox.signalErr = assert.AnError

	count, err := h.reassign()

	require.ErrorIs(t, err, assert.AnError, "a move whose signal cannot be recorded is not committed, so the caller retries it")
	assert.Zero(t, count)
	h.requireUntouched(t, first, second)
	assert.Equal(t, 1, h.tx.rollbacks)
}

func TestDelegationReassign_ADelegateWhoAlreadyHoldsTheTaskKeepsOneRow(t *testing.T) {
	h := dlgfixNewClient(t)
	held := h.seed(t, "reviewer")
	h.assignments.add(&domain.TaskAssignment{ID: uuid.New(), TenantID: h.tenantID, TaskID: held.task.ID, UserID: h.newDelegate, IsActive: true})

	count, err := h.reassign()

	require.NoError(t, err)
	assert.Equal(t, 1, count)
	assert.False(t, held.assignment.IsActive, "the old delegate's row is vacated")
	active := 0
	for _, a := range h.assignments.byID {
		if a.TaskID == held.task.ID && a.UserID == h.newDelegate && a.IsActive {
			active++
		}
	}
	assert.Equal(t, 1, active)
}

func TestDelegationReassign_ADelegateWhoAlreadyHoldsTheTaskInheritsTheLead(t *testing.T) {
	h := dlgfixNewClient(t)
	held := h.seed(t, "reviewer")
	held.assignment.IsLead = true
	existing := &domain.TaskAssignment{ID: uuid.New(), TenantID: h.tenantID, TaskID: held.task.ID, UserID: h.newDelegate, IsActive: true}
	h.assignments.add(existing)

	count, err := h.reassign()

	require.NoError(t, err)
	assert.Equal(t, 1, count)
	assert.True(t, existing.IsLead, "the claim moves to the row the delegate already holds")
	assert.Empty(t, h.assignments.created, "no second row is attempted, so the transaction is never aborted")
}

func TestDelegationReassign_AnAssignmentThatEndedBetweenTheReadAndTheWriteIsLeftAlone(t *testing.T) {
	h := dlgfixNewClient(t)
	ended, live := h.seed(t, "reviewer"), h.seed(t, "reviewer")
	h.tx.onStart = func() { ended.assignment.IsActive = false }

	count, err := h.reassign()

	require.NoError(t, err)
	assert.Equal(t, 1, count, "only the assignment this call vacated is counted")
	assert.NotContains(t, h.assignments.vacated, ended.assignment.ID, "an assignment that is no longer active is not vacated again")
	assert.False(t, h.holdsActive(ended.task.ID, h.newDelegate), "no row is created for a task whose assignment ended")
	assert.True(t, h.holdsActive(live.task.ID, h.newDelegate))
	require.Len(t, h.outbox.signals, 1, "only the moved assignment is signalled")
	assert.Equal(t, live.instance.ID, h.outbox.signals[0].InstanceID)
}

func TestDelegationCancel_AnAssignmentThatEndedBetweenTheReadAndTheWriteIsNotVacatedOrCounted(t *testing.T) {
	h := dlgfixNewClient(t)
	ended, live := h.seed(t, "reviewer"), h.seed(t, "reviewer")
	h.tx.onStart = func() { ended.assignment.IsActive = false }

	count, err := h.cancel()

	require.NoError(t, err)
	assert.Equal(t, 1, count)
	assert.Equal(t, []uuid.UUID{live.assignment.ID}, h.assignments.vacated)
}
