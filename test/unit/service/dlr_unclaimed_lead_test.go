package service_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type dlrLeadCall struct {
	tenantID, taskID, assignmentID uuid.UUID
	version                        int64
}

// dlrLeadRecorder records which lead moves the code asks the repository for.
type dlrLeadRecorder struct {
	*fakeAssignmentRepo
	setLead []dlrLeadCall
	claimed []dlrLeadCall

	setLeadErr error
}

func (r *dlrLeadRecorder) SetLead(ctx context.Context, tenantID, taskID, id uuid.UUID, version int64) (*domain.TaskAssignment, error) {
	r.setLead = append(r.setLead, dlrLeadCall{tenantID, taskID, id, version})
	if r.setLeadErr != nil {
		return nil, r.setLeadErr
	}
	return r.fakeAssignmentRepo.SetLead(ctx, tenantID, taskID, id, version)
}

func (r *dlrLeadRecorder) ClaimLead(ctx context.Context, tenantID, taskID, id uuid.UUID, version int64) (*domain.TaskAssignment, error) {
	r.claimed = append(r.claimed, dlrLeadCall{tenantID, taskID, id, version})
	return r.fakeAssignmentRepo.SetLead(ctx, tenantID, taskID, id, version)
}

type dlrUnclaimed struct {
	h        *dlgfixRecon
	leads    *dlrLeadRecorder
	row      dlgfixRecRow
	delegate *domain.TaskAssignment
}

// dlrNewUnclaimed is an all-assignee task nobody has claimed, held by the
// delegator and by the delegate.
func dlrNewUnclaimed(t *testing.T, mode string) *dlrUnclaimed {
	t.Helper()
	h := dlgfixNewRecon()
	row := h.seed(t, "reviewer")
	row.task.AssigneeMode = mode
	row.task.RecordVersion = 7
	delegate := &domain.TaskAssignment{ID: uuid.New(), TenantID: h.tenantID, TaskID: row.task.ID, UserID: h.delegate, IsActive: true}
	h.assignments.add(delegate)
	leads := &dlrLeadRecorder{fakeAssignmentRepo: h.assignments}
	h.svc.Assignments = leads
	return &dlrUnclaimed{h: h, leads: leads, row: row, delegate: delegate}
}

func TestDelegationStarted_UnclaimedAllAssigneeTaskToAnExistingHolderMakesThemLead(t *testing.T) {
	w := dlrNewUnclaimed(t, "all")
	other := &domain.TaskAssignment{ID: uuid.New(), TenantID: w.h.tenantID, TaskID: w.row.task.ID, UserID: uuid.New(), IsActive: true}
	w.h.assignments.add(other)

	require.NoError(t, w.h.reroute())

	assert.True(t, w.delegate.IsActive)
	assert.True(t, w.delegate.IsLead, "the delegate leads the task")
	assert.False(t, w.row.assignment.IsActive, "the delegator's assignment is handed over")
	assert.True(t, other.IsActive, "nobody else is vacated")
	assert.False(t, other.IsLead, "nobody else is changed")
	assert.Equal(t, []dlrLeadCall{{w.h.tenantID, w.row.task.ID, w.delegate.ID, 7}}, w.leads.setLead)
	assert.Empty(t, w.leads.claimed, "a delegation is not a claim by the delegate")
	assert.Len(t, w.h.outbox.enqueued, 1)
	require.Len(t, w.h.outbox.signalsNamed(port.SignalInstanceReassign), 1)
	assert.Len(t, w.h.outbox.signals, 1)
}

func TestDelegationStarted_TaskWithALeadAlreadyKeepsItsLead(t *testing.T) {
	w := dlrNewUnclaimed(t, "all")
	lead := &domain.TaskAssignment{ID: uuid.New(), TenantID: w.h.tenantID, TaskID: w.row.task.ID, UserID: uuid.New(), IsActive: true, IsLead: true}
	w.h.assignments.add(lead)

	require.NoError(t, w.h.reroute())

	assert.False(t, w.delegate.IsLead, "a claimed task keeps its lead")
	assert.True(t, lead.IsLead)
	assert.Empty(t, w.leads.setLead)
}

func TestDelegationStarted_ATaskThatIsNotAllAssigneeIsNotLedByTheDelegate(t *testing.T) {
	w := dlrNewUnclaimed(t, "any")

	require.NoError(t, w.h.reroute())

	assert.False(t, w.delegate.IsLead)
	assert.Empty(t, w.leads.setLead)
}

// dlrClaimLands hands the first read of a task's active assignments an answer with no lead,
// then lets a claim by another holder commit: every later read shows that holder as lead.
type dlrClaimLands struct {
	port.TaskAssignmentRepository
	claimer *domain.TaskAssignment
	ops     *[]string
	reads   int
}

func (r *dlrClaimLands) ListActiveByTask(ctx context.Context, tenantID, taskID uuid.UUID) ([]*domain.TaskAssignment, error) {
	*r.ops = append(*r.ops, "list")
	rows, err := r.TaskAssignmentRepository.ListActiveByTask(ctx, tenantID, taskID)
	r.reads++
	if r.reads > 1 {
		return rows, err
	}
	seen := make([]*domain.TaskAssignment, len(rows))
	for i, a := range rows {
		c := *a
		seen[i] = &c
	}
	r.claimer.IsLead = true
	return seen, err
}

// dlrOrderedTasks records the reads of a task in the same log as the assignment reads.
type dlrOrderedTasks struct {
	port.TaskRepository
	ops *[]string
}

func (r *dlrOrderedTasks) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Task, error) {
	*r.ops = append(*r.ops, "task")
	return r.TaskRepository.GetByID(ctx, tenantID, id)
}

func TestDelegationStarted_AClaimThatLandsBeforeTheLeadMoveKeepsItsLead(t *testing.T) {
	w := dlrNewUnclaimed(t, "all")
	claimer := &domain.TaskAssignment{ID: uuid.New(), TenantID: w.h.tenantID, TaskID: w.row.task.ID, UserID: uuid.New(), IsActive: true}
	w.h.assignments.add(claimer)
	var ops []string
	w.h.svc.Assignments = &dlrClaimLands{TaskAssignmentRepository: w.leads, claimer: claimer, ops: &ops}
	w.h.svc.Tasks = &dlrOrderedTasks{TaskRepository: w.h.tasks, ops: &ops}

	require.NoError(t, w.h.reroute())

	assert.True(t, claimer.IsLead, "the claimer keeps the lead")
	assert.False(t, w.delegate.IsLead, "the delegate does not take it")
	assert.Empty(t, w.leads.setLead, "no lead move is attempted over a lead that landed in between")
	assert.Equal(t, "task", ops[len(ops)-2], "the task version is read before the assignments are re-read")
	assert.Equal(t, "list", ops[len(ops)-1])
}

func TestDelegationStarted_ALeadMoveThatFindsTheTaskVersionMovedFailsTheEvent(t *testing.T) {
	w := dlrNewUnclaimed(t, "all")
	w.leads.setLeadErr = domain.ErrRecordVersionConflict

	err := w.h.reroute()

	require.ErrorIs(t, err, domain.ErrRecordVersionConflict, "the event fails so it is retried")
	require.Len(t, w.leads.setLead, 1)
	assert.Equal(t, int64(7), w.leads.setLead[0].version, "the version read first is the one the move is stamped with")
}
