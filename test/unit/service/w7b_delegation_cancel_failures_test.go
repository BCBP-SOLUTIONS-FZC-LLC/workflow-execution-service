package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// w7bAssignments records the lead moves and can fail the n-th read of a task's
// active assignments.
type w7bAssignments struct {
	*dlgfixAssignments
	listCalls   int
	failListAt  int
	listErr     error
	listTenants []uuid.UUID
	setLeads    []w7bSetLeadCall
	tasks       *w7bCopyTasks
}

// w7bCopyTasks answers every read with a copy of the stored task, as a
// database does, and records the tenant of each read.
type w7bCopyTasks struct {
	*fakeTaskRepo
	tenants []uuid.UUID
}

func (r *w7bCopyTasks) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Task, error) {
	r.tenants = append(r.tenants, tenantID)
	task, err := r.fakeTaskRepo.GetByID(ctx, tenantID, id)
	if err != nil {
		return nil, err
	}
	copied := *task
	return &copied, nil
}

type w7bSetLeadCall struct {
	tenantID, taskID, assignmentID uuid.UUID
	version                        int64
}

func (r *w7bAssignments) ListActiveByTask(ctx context.Context, tenantID, taskID uuid.UUID) ([]*domain.TaskAssignment, error) {
	r.listCalls++
	r.listTenants = append(r.listTenants, tenantID)
	if r.failListAt > 0 && r.listCalls >= r.failListAt {
		return nil, r.listErr
	}
	return r.dlgfixAssignments.ListActiveByTask(ctx, tenantID, taskID)
}

func (r *w7bAssignments) SetLead(ctx context.Context, tenantID, taskID, id uuid.UUID, version int64) (*domain.TaskAssignment, error) {
	r.setLeads = append(r.setLeads, w7bSetLeadCall{tenantID: tenantID, taskID: taskID, assignmentID: id, version: version})
	return r.dlgfixAssignments.SetLead(ctx, tenantID, taskID, id, version)
}

// w7bEnded fails the write of the delegate's cancel marker and records its arguments.
type w7bEnded struct {
	*fakeEndedDelegations
	markDelegateErr error
	delegateMarks   []endedDelegateKey
}

func (e *w7bEnded) MarkDelegateEnded(ctx context.Context, tenantID, delegateID uuid.UUID, endedAt time.Time) error {
	e.delegateMarks = append(e.delegateMarks, endedDelegateKey{tenantID, delegateID})
	if e.markDelegateErr != nil {
		return e.markDelegateErr
	}
	return e.fakeEndedDelegations.MarkDelegateEnded(ctx, tenantID, delegateID, endedAt)
}

func w7bClient(t *testing.T) (*dlgfixClient, *w7bAssignments, *w7bEnded) {
	t.Helper()
	h := dlgfixNewClient(t)
	tasks := &w7bCopyTasks{fakeTaskRepo: h.tasks}
	assignments := &w7bAssignments{dlgfixAssignments: h.assignments, tasks: tasks}
	h.svc.Tasks = tasks
	ended := &w7bEnded{fakeEndedDelegations: newFakeEndedDelegations()}
	h.svc.Assignments = assignments
	h.svc.Ended = ended
	return h, assignments, ended
}

func w7bActiveRows(h *dlgfixClient, taskID, userID uuid.UUID) []*domain.TaskAssignment {
	var rows []*domain.TaskAssignment
	for _, a := range h.assignments.byID {
		if a.TaskID == taskID && a.UserID == userID && a.IsActive {
			rows = append(rows, a)
		}
	}
	return rows
}

func TestDelegationCancel_DelegatorAlreadyHoldsTheTask_KeepsOneRowAndBecomesLead(t *testing.T) {
	cases := []struct {
		name        string
		delegateLed bool
	}{
		{name: "the delegate was the lead", delegateLed: true},
		{name: "the delegate was not the lead", delegateLed: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, assignments, _ := w7bClient(t)
			delegationID, delegator := uuid.New(), uuid.New()
			held := h.seed(t, "reviewer")
			held.assignment.Reason = dcrHop(delegationID, delegator)
			held.assignment.IsLead = tc.delegateLed
			coAssignee := &domain.TaskAssignment{ID: uuid.New(), TenantID: h.tenantID, TaskID: held.task.ID, UserID: delegator, IsActive: true}
			h.assignments.add(coAssignee)
			const versionInTransaction = 9
			h.tx.onStart = func() { held.task.RecordVersion = versionInTransaction }

			count, err := h.cancel()

			require.NoError(t, err)
			assert.Equal(t, 1, count)
			assert.False(t, held.assignment.IsActive, "the delegate lost the task")
			assert.Equal(t, []*domain.TaskAssignment{coAssignee}, w7bActiveRows(h, held.task.ID, delegator), "the delegator keeps its one row")
			assert.Empty(t, h.assignments.created, "no second row is created")
			assert.Equal(t, tc.delegateLed, coAssignee.IsLead, "the lead flag moves to the existing row only when the delegate held it")
			if tc.delegateLed {
				assert.Equal(t, []w7bSetLeadCall{{tenantID: h.tenantID, taskID: held.task.ID, assignmentID: coAssignee.ID, version: versionInTransaction}}, assignments.setLeads,
					"the lead move is stamped with the task version read inside the transaction, not the earlier read")
			} else {
				assert.Empty(t, assignments.setLeads, "a non-lead row moves no lead flag")
			}
			require.Len(t, h.outbox.signals, 1)
			assert.Equal(t, port.SignalInstanceReassign, h.outbox.signals[0].SignalName)
			assert.Equal(t, csaReassignWire{TaskID: held.task.ID.String(), OldUserID: h.oldDelegate.String(), NewUserID: delegator.String()}, csaDecodeReassign(t, h.outbox.signals[0].Payload))
			for _, tenant := range assignments.tasks.tenants {
				assert.Equal(t, h.tenantID, tenant, "every task read names the caller's tenant")
			}
			assert.NotEmpty(t, assignments.tasks.tenants)
		})
	}
}

func TestDelegationCancel_ReadOfActiveAssignmentsFailsInTransaction_RollsBackEarlierVacates(t *testing.T) {
	cases := []struct {
		name string
		// failAt is the number of the read of a task's active assignments that fails.
		failAt int
	}{
		{name: "the second row's re-read fails after the first row was vacated", failAt: 2},
		{name: "the first row's re-read fails", failAt: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, assignments, _ := w7bClient(t)
			assignments.failListAt, assignments.listErr = tc.failAt, assert.AnError
			first, second := h.seed(t, "reviewer"), h.seed(t, "reviewer")

			count, err := h.cancel()

			require.ErrorIs(t, err, assert.AnError)
			assert.Zero(t, count, "nothing is counted")
			assert.True(t, first.assignment.IsActive, "a row vacated earlier in the call is put back")
			assert.True(t, second.assignment.IsActive)
			assert.Equal(t, 1, h.tx.rollbacks)
			assert.Zero(t, h.tx.commits)
			assert.Empty(t, h.outbox.signals, "no signal outlives the rollback")
			assert.Equal(t, []string{h.tenantID.String()}, h.tx.tenants)
			require.Len(t, assignments.listTenants, tc.failAt, "the reads stop at the failing one")
			for _, tenant := range assignments.listTenants {
				assert.Equal(t, h.tenantID, tenant, "every re-read names the caller's tenant")
			}
		})
	}
}

func TestDelegationCancel_PlanOrEligibilityReadFails_VacatesNothingAndRecordsNoEnd(t *testing.T) {
	cases := []struct {
		name         string
		failPlan     bool
		wantEligible bool
	}{
		{name: "the compiled plan cannot be read", failPlan: true},
		{name: "the delegator's eligibility cannot be read", wantEligible: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, _, ended := w7bClient(t)
			delegator := uuid.New()
			held := h.seed(t, "reviewer")
			held.assignment.Reason = dcrHop(uuid.New(), delegator)
			var asked []w7bEligibilityAsk
			h.svc.Definitions = h.definitions
			h.svc.Eligibility = &fakeEligibilityChecker{check: func(_ context.Context, tenantID, userID, departmentID uuid.UUID, level string) (bool, error) {
				asked = append(asked, w7bEligibilityAsk{tenantID: tenantID, userID: userID, departmentID: departmentID, level: level})
				return false, assert.AnError
			}}
			if tc.failPlan {
				h.definitions.failing[held.versionID] = assert.AnError
			}

			count, err := h.cancel()

			require.ErrorIs(t, err, assert.AnError)
			if tc.wantEligible {
				assert.ErrorIs(t, err, port.ErrEligibilityRead, "an eligibility failure is told apart from a plan failure")
				assert.Equal(t, []w7bEligibilityAsk{{tenantID: h.tenantID, userID: delegator, departmentID: held.task.DepartmentID, level: "reviewer"}}, asked)
			} else {
				assert.NotErrorIs(t, err, port.ErrEligibilityRead)
				assert.Empty(t, asked, "no one is asked when the plan is unknown")
			}
			assert.Zero(t, count)
			assert.True(t, held.assignment.IsActive, "the delegate keeps the row")
			assert.Empty(t, h.tx.tenants, "no transaction was opened")
			assert.Empty(t, h.assignments.vacated)
			assert.Empty(t, h.outbox.signals)
			assert.Empty(t, h.outbox.enqueued)
			assert.Empty(t, ended.delegateMarks, "no end is recorded for the delegate")
			assert.Empty(t, ended.marked)
		})
	}
}

type w7bEligibilityAsk struct {
	tenantID     uuid.UUID
	userID       uuid.UUID
	departmentID uuid.UUID
	level        string
}

func TestDelegationCancel_GivingARowBackFails_RollsBackTheWholeCancel(t *testing.T) {
	cases := []struct {
		name   string
		breaks func(h *dlgfixClient, assignments *w7bAssignments, second dlgfixHeld, delegator uuid.UUID)
	}{
		{"the delegator's new row cannot be created", func(h *dlgfixClient, _ *w7bAssignments, second dlgfixHeld, _ uuid.UUID) {
			h.assignments.createFails[second.task.ID] = assert.AnError
		}},
		{"the lead flag cannot move to the delegator's existing row", func(h *dlgfixClient, _ *w7bAssignments, second dlgfixHeld, delegator uuid.UUID) {
			second.assignment.IsLead = true
			h.assignments.add(&domain.TaskAssignment{ID: uuid.New(), TenantID: h.tenantID, TaskID: second.task.ID, UserID: delegator, IsActive: true})
			h.assignments.setLeadErr = assert.AnError
		}},
		{"the reassigned event cannot be recorded", func(h *dlgfixClient, _ *w7bAssignments, _ dlgfixHeld, _ uuid.UUID) {
			h.outbox.enqueueErr = assert.AnError
		}},
		{"the workflow signal cannot be recorded", func(h *dlgfixClient, _ *w7bAssignments, _ dlgfixHeld, _ uuid.UUID) {
			h.outbox.signalErr = assert.AnError
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, assignments, _ := w7bClient(t)
			delegator := uuid.New()
			first, second := h.seed(t, "reviewer"), h.seed(t, "reviewer")
			first.assignment.Reason = dcrHop(uuid.New(), delegator)
			second.assignment.Reason = dcrHop(uuid.New(), delegator)
			tc.breaks(h, assignments, second, delegator)

			count, err := h.cancel()

			require.ErrorIs(t, err, assert.AnError)
			assert.Zero(t, count)
			assert.True(t, first.assignment.IsActive, "the row given back before the failure is put back")
			assert.True(t, second.assignment.IsActive)
			assert.Empty(t, w7bActiveRows(h, first.task.ID, delegator), "the delegator holds nothing the call created")
			assert.Equal(t, 1, h.tx.rollbacks)
			assert.Zero(t, h.tx.commits)
			assert.Empty(t, h.outbox.signals, "no signal outlives the rollback")
			assert.Empty(t, h.outbox.enqueued, "no event outlives the rollback")
		})
	}
}

func TestDelegationCancel_RecordingTheDelegatesEndFails_VacatesNothing(t *testing.T) {
	cases := []struct {
		name     string
		withRows bool
	}{
		{name: "the delegate holds nothing", withRows: false},
		{name: "the delegate holds rows", withRows: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, _, ended := w7bClient(t)
			ended.markDelegateErr = assert.AnError
			var held []dlgfixHeld
			if tc.withRows {
				held = append(held, h.seed(t, "reviewer"), h.seed(t, "reviewer"))
			}

			count, err := h.cancel()

			require.ErrorIs(t, err, assert.AnError)
			assert.ErrorContains(t, err, h.oldDelegate.String(), "the error names the delegate whose end was not recorded")
			assert.Zero(t, count)
			assert.Equal(t, []endedDelegateKey{{h.tenantID, h.oldDelegate}}, ended.delegateMarks)
			assert.Empty(t, ended.marked, "no named delegation is marked")
			for _, row := range held {
				assert.True(t, row.assignment.IsActive, "the delegate keeps the row")
			}
			assert.Empty(t, h.assignments.vacated, "nothing is vacated")
			assert.Empty(t, h.outbox.signals)
			assert.Zero(t, h.tx.commits)
		})
	}
}
