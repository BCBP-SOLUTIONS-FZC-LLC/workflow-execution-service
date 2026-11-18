package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"
)

var (
	errW12aStoreDown = errors.New("assignment store down")
)

type w12aLogLine struct {
	msg    string
	fields map[string]any
}

// w12aLogger keeps the message and fields of every error line.
type w12aLogger struct{ errors []w12aLogLine }

func (l *w12aLogger) Debug(string, map[string]any) {}
func (l *w12aLogger) Info(string, map[string]any)  {}
func (l *w12aLogger) Warn(string, map[string]any)  {}
func (l *w12aLogger) Error(msg string, fields map[string]any) {
	l.errors = append(l.errors, w12aLogLine{msg: msg, fields: fields})
}

type w12aAssignmentCall struct{ tenantID, id uuid.UUID }

type w12aSetLeadCall struct {
	tenantID, taskID, id uuid.UUID
	version              int64
}

// w12aMoveAssignments records the tenant and ids of every call a delegation move makes
// and fails the reads and vacates it is told to. SetLead keeps the real contract: the
// task version given must be the stored one, the task must exist, and the move bumps it.
type w12aMoveAssignments struct {
	*fakeAssignmentRepo
	tasks     *fakeTaskRepo
	listErr   map[uuid.UUID]error
	vacateErr map[uuid.UUID]error
	lists     []w12aAssignmentCall
	vacates   []w12aAssignmentCall
	creates   []*domain.TaskAssignment
	leadMoves []w12aSetLeadCall
}

func (r *w12aMoveAssignments) ListActiveByTask(ctx context.Context, tenantID, taskID uuid.UUID) ([]*domain.TaskAssignment, error) {
	r.lists = append(r.lists, w12aAssignmentCall{tenantID: tenantID, id: taskID})
	if err := r.listErr[taskID]; err != nil {
		return nil, err
	}
	return r.fakeAssignmentRepo.ListActiveByTask(ctx, tenantID, taskID)
}

func (r *w12aMoveAssignments) VacateActive(ctx context.Context, tenantID, id uuid.UUID) (*domain.TaskAssignment, error) {
	r.vacates = append(r.vacates, w12aAssignmentCall{tenantID: tenantID, id: id})
	if err := r.vacateErr[id]; err != nil {
		return nil, err
	}
	return r.fakeAssignmentRepo.VacateActive(ctx, tenantID, id)
}

func (r *w12aMoveAssignments) Create(ctx context.Context, a *domain.TaskAssignment) error {
	err := r.fakeAssignmentRepo.Create(ctx, a)
	if err == nil {
		r.creates = append(r.creates, a)
	}
	return err
}

func (r *w12aMoveAssignments) SetLead(ctx context.Context, tenantID, taskID, id uuid.UUID, version int64) (*domain.TaskAssignment, error) {
	r.leadMoves = append(r.leadMoves, w12aSetLeadCall{tenantID: tenantID, taskID: taskID, id: id, version: version})
	task, ok := r.tasks.byID[taskID]
	if !ok || task.TenantID != tenantID {
		return nil, domain.ErrNotFound
	}
	if task.RecordVersion != version {
		return nil, domain.ErrRecordVersionConflict
	}
	task.RecordVersion++
	for _, other := range r.byID {
		if other.TaskID == taskID && other.ID != id {
			other.IsLead = false
		}
	}
	return r.fakeAssignmentRepo.SetLead(ctx, tenantID, taskID, id, version)
}

// w12aOutbox records whether each event was enqueued while the move's transaction ran,
// and fails the enqueue of the call numbered failOn (1-based).
type w12aOutbox struct {
	*fakeOutbox
	tx       *dlgfixTx
	insideTx []bool
	calls    int
	failOn   int
	failWith error
}

func (o *w12aOutbox) Enqueue(ctx context.Context, env events.Envelope[json.RawMessage]) error {
	o.calls++
	o.insideTx = append(o.insideTx, o.tx.running)
	if o.failOn == o.calls {
		return o.failWith
	}
	return o.fakeOutbox.Enqueue(ctx, env)
}

// w12aMoveRig is the reconciler of the shared reroute harness, with its assignments,
// outbox and transactor replaced by recording ones.
type w12aMoveRig struct {
	*dlgfixRecon
	tx           *dlgfixTx
	repo         *w12aMoveAssignments
	out          *w12aOutbox
	logger       *w12aLogger
	delegationID uuid.UUID
	asked        []w12aEligibilityAsk
}

type w12aEligibilityAsk struct {
	tenantID uuid.UUID
	requests []port.EligibilityCheckRequest
}

func w12aNewMoveRig() *w12aMoveRig {
	h := dlgfixNewRecon()
	rig := &w12aMoveRig{dlgfixRecon: h, delegationID: uuid.New(), logger: &w12aLogger{}}
	rig.tx = &dlgfixTx{repo: h.assignments, outbox: h.outbox}
	rig.repo = &w12aMoveAssignments{
		fakeAssignmentRepo: h.assignments, tasks: h.tasks,
		listErr: map[uuid.UUID]error{}, vacateErr: map[uuid.UUID]error{},
	}
	rig.out = &w12aOutbox{fakeOutbox: h.outbox, tx: rig.tx}
	h.svc.Transactor, h.svc.Assignments, h.svc.Outbox, h.svc.Log = rig.tx, rig.repo, rig.out, rig.logger
	h.eligibility.batchCheck = func(_ context.Context, tenantID uuid.UUID, reqs []port.EligibilityCheckRequest) ([]port.EligibilityResult, error) {
		rig.asked = append(rig.asked, w12aEligibilityAsk{tenantID: tenantID, requests: reqs})
		out := make([]port.EligibilityResult, len(reqs))
		for i := range out {
			out[i].Eligible = true
		}
		return out, nil
	}
	return rig
}

// w12aMove is one way work changes hands: a start moves it from the delegator to
// the delegate, an end moves it back.
type w12aMove struct {
	name string
	// hold makes the row one the move acts on.
	hold func(rig *w12aMoveRig, row dlgfixRecRow)
	run  func(rig *w12aMoveRig) error
	// from and to are the users the work leaves and reaches.
	from, to func(rig *w12aMoveRig) uuid.UUID
}

func w12aMoves() []w12aMove {
	return []w12aMove{
		{
			name: "delegation started",
			hold: func(*w12aMoveRig, dlgfixRecRow) {},
			run: func(rig *w12aMoveRig) error {
				return rig.svc.Reroute(context.Background(), port.DelegationRerouteInput{
					TenantID: rig.tenantID, DelegationID: rig.delegationID, DelegatorID: rig.delegator, DelegateID: rig.delegate, Scope: "all",
				})
			},
			from: func(rig *w12aMoveRig) uuid.UUID { return rig.delegator },
			to:   func(rig *w12aMoveRig) uuid.UUID { return rig.delegate },
		},
		{
			name: "delegation ended",
			hold: func(rig *w12aMoveRig, row dlgfixRecRow) {
				row.assignment.UserID = rig.delegate
				row.assignment.Reason = "delegation:" + rig.delegationID.String() + ":" + rig.delegator.String()
			},
			run: func(rig *w12aMoveRig) error {
				return rig.svc.Reverse(context.Background(), port.DelegationReversalInput{
					TenantID: rig.tenantID, DelegationID: rig.delegationID, DelegatorID: rig.delegator, DelegateID: rig.delegate,
				})
			},
			from: func(rig *w12aMoveRig) uuid.UUID { return rig.delegate },
			to:   func(rig *w12aMoveRig) uuid.UUID { return rig.delegator },
		},
	}
}

// seedRows gives the user the move takes work from one row per role, each on its own task.
func (rig *w12aMoveRig) seedRows(t *testing.T, mv w12aMove, roles ...string) []dlgfixRecRow {
	t.Helper()
	rows := make([]dlgfixRecRow, 0, len(roles))
	for _, role := range roles {
		row := rig.seed(t, role)
		mv.hold(rig, row)
		rows = append(rows, row)
	}
	return rows
}

func (rig *w12aMoveRig) holders(taskID uuid.UUID) []uuid.UUID {
	var users []uuid.UUID
	for _, a := range rig.assignments.byID {
		if a.TaskID == taskID && a.IsActive {
			users = append(users, a.UserID)
		}
	}
	return users
}

// requireRolledBack checks that every row is back with the user it was taken from, that
// nobody was given anything, and that nothing was recorded for the relay or the bus.
func (rig *w12aMoveRig) requireRolledBack(t *testing.T, mv w12aMove, rows []dlgfixRecRow) {
	t.Helper()
	assert.Len(t, rig.assignments.byID, len(rows), "no row is left behind by the failed move")
	for _, row := range rows {
		assert.Equal(t, []uuid.UUID{mv.from(rig)}, rig.holders(row.task.ID), "the task is still held by the user it was taken from")
	}
	assert.Empty(t, rig.outbox.enqueued, "no reassigned event")
	assert.Empty(t, rig.outbox.signals, "no signal for the relay")
	assert.Empty(t, rig.temporal.signals, "no signal sent directly")
	assert.Zero(t, rig.tx.commits)
	assert.Equal(t, 1, rig.tx.rollbacks)
	assert.Equal(t, []string{rig.tenantID.String()}, rig.tx.tenants, "the move runs once, on the event's tenant")
}

func TestDelegationMove_ReadOfTheTasksAssignmentsOrTheVacateFails_FailsTheEventAndRollsBackTheBatch(t *testing.T) {
	type failure struct {
		name string
		// inject makes the call fail for the given rows.
		inject func(rig *w12aMoveRig, rows []dlgfixRecRow)
		// calls says which reads and vacates were made before the failure stopped the move.
		wantLists   func(rows []dlgfixRecRow) []uuid.UUID
		wantVacates func(rows []dlgfixRecRow) []uuid.UUID
		wantCreates int
	}
	failures := []failure{
		{
			name:        "the first row's read of the task's active assignments fails",
			inject:      func(rig *w12aMoveRig, rows []dlgfixRecRow) { rig.repo.listErr[rows[0].task.ID] = errW12aStoreDown },
			wantLists:   func(rows []dlgfixRecRow) []uuid.UUID { return []uuid.UUID{rows[0].task.ID} },
			wantVacates: func([]dlgfixRecRow) []uuid.UUID { return nil },
		},
		{
			name: "the first row's vacate fails",
			inject: func(rig *w12aMoveRig, rows []dlgfixRecRow) {
				rig.repo.vacateErr[rows[0].assignment.ID] = errW12aStoreDown
			},
			wantLists:   func(rows []dlgfixRecRow) []uuid.UUID { return []uuid.UUID{rows[0].task.ID} },
			wantVacates: func(rows []dlgfixRecRow) []uuid.UUID { return []uuid.UUID{rows[0].assignment.ID} },
		},
		{
			name: "the second row's vacate fails after the first row moved",
			inject: func(rig *w12aMoveRig, rows []dlgfixRecRow) {
				rig.repo.vacateErr[rows[1].assignment.ID] = errW12aStoreDown
			},
			wantLists: func(rows []dlgfixRecRow) []uuid.UUID {
				return []uuid.UUID{rows[0].task.ID, rows[1].task.ID}
			},
			wantVacates: func(rows []dlgfixRecRow) []uuid.UUID {
				return []uuid.UUID{rows[0].assignment.ID, rows[1].assignment.ID}
			},
			wantCreates: 1,
		},
	}
	for _, mv := range w12aMoves() {
		for _, f := range failures {
			t.Run(mv.name+": "+f.name, func(t *testing.T) {
				rig := w12aNewMoveRig()
				rows := rig.seedRows(t, mv, "reviewer", "approver")
				f.inject(rig, rows)

				err := mv.run(rig)

				require.ErrorIs(t, err, errW12aStoreDown, "the event fails, so the consumer redelivers it")
				var lists, vacates []uuid.UUID
				for _, c := range rig.repo.lists {
					assert.Equal(t, rig.tenantID, c.tenantID)
					lists = append(lists, c.id)
				}
				for _, c := range rig.repo.vacates {
					assert.Equal(t, rig.tenantID, c.tenantID)
					vacates = append(vacates, c.id)
				}
				assert.Equal(t, f.wantLists(rows), lists)
				assert.Equal(t, f.wantVacates(rows), vacates)
				assert.Len(t, rig.repo.creates, f.wantCreates, "a row that moved before the failure was written, then rolled back")
				rig.requireRolledBack(t, mv, rows)
			})
		}

		t.Run(mv.name+": with no failure both rows move in one committed transaction", func(t *testing.T) {
			rig := w12aNewMoveRig()
			rows := rig.seedRows(t, mv, "reviewer", "approver")

			require.NoError(t, mv.run(rig))

			for _, row := range rows {
				assert.Equal(t, []uuid.UUID{mv.to(rig)}, rig.holders(row.task.ID))
			}
			assert.Len(t, rig.outbox.enqueued, 2)
			assert.Len(t, rig.outbox.signals, 2)
			assert.Equal(t, 1, rig.tx.commits)
			assert.Zero(t, rig.tx.rollbacks)
		})
	}
}

func TestDelegationMove_AnAssignmentDeletedAfterItWasRead_IsSkippedAndTheOthersMove(t *testing.T) {
	for _, mv := range w12aMoves() {
		t.Run(mv.name, func(t *testing.T) {
			rig := w12aNewMoveRig()
			rows := rig.seedRows(t, mv, "reviewer", "approver")
			rig.repo.vacateErr[rows[1].assignment.ID] = domain.ErrNotFound

			require.NoError(t, mv.run(rig), "a row that is gone is not a failure")

			assert.Equal(t, []uuid.UUID{mv.to(rig)}, rig.holders(rows[0].task.ID), "the first row moves")
			assert.Equal(t, []uuid.UUID{mv.from(rig)}, rig.holders(rows[1].task.ID), "the missing row gets no new holder")
			require.Len(t, rig.outbox.enqueued, 1, "one event, for the row that moved")
			assert.Contains(t, string(rig.outbox.enqueued[0].Payload), rows[0].task.ID.String())
			assert.NotContains(t, string(rig.outbox.enqueued[0].Payload), rows[1].task.ID.String())
			require.Len(t, rig.outbox.signals, 1)
			assert.Equal(t, rows[0].instance.ID, rig.outbox.signals[0].InstanceID)
			assert.Equal(t, 1, rig.tx.commits)
		})
	}
}

func TestDelegationMove_EligibilityAnswersOfTheWrongCount_FailTheEventBeforeAnythingMoves(t *testing.T) {
	cases := []struct {
		name    string
		answers int
	}{
		{name: "fewer answers than requests", answers: 1},
		{name: "more answers than requests", answers: 3},
		{name: "no answers at all", answers: 0},
	}
	for _, mv := range w12aMoves() {
		for _, tc := range cases {
			t.Run(mv.name+": "+tc.name, func(t *testing.T) {
				rig := w12aNewMoveRig()
				rows := rig.seedRows(t, mv, "reviewer", "approver")
				rig.eligibility.batchCheck = func(_ context.Context, tenantID uuid.UUID, reqs []port.EligibilityCheckRequest) ([]port.EligibilityResult, error) {
					rig.asked = append(rig.asked, w12aEligibilityAsk{tenantID: tenantID, requests: reqs})
					out := make([]port.EligibilityResult, tc.answers)
					for i := range out {
						out[i].Eligible = true
					}
					return out, nil
				}

				err := mv.run(rig)

				require.ErrorIs(t, err, port.ErrEligibilityAnswerCount, "the handler answers 503 for this, so the event is redelivered")
				require.Len(t, rig.asked, 1)
				assert.Equal(t, rig.tenantID, rig.asked[0].tenantID)
				require.Len(t, rig.asked[0].requests, 2, "two distinct levels are asked")
				for _, req := range rig.asked[0].requests {
					assert.Equal(t, mv.to(rig), req.NewUserID, "the question is about the user who would receive the work")
				}
				require.Len(t, rig.logger.errors, 1, "the mismatch is logged once, at error level")
				assert.Equal(t, 2, rig.logger.errors[0].fields["requests"])
				assert.Equal(t, tc.answers, rig.logger.errors[0].fields["answers"])
				for _, row := range rows {
					assert.Equal(t, []uuid.UUID{mv.from(rig)}, rig.holders(row.task.ID), "no row moves")
				}
				assert.Len(t, rig.assignments.byID, len(rows))
				assert.Empty(t, rig.outbox.enqueued)
				assert.Empty(t, rig.outbox.signals)
				assert.Empty(t, rig.tx.tenants, "no transaction is opened")
				assert.Empty(t, rig.repo.vacates)
			})
		}

		t.Run(mv.name+": an answer for every request moves the rows", func(t *testing.T) {
			rig := w12aNewMoveRig()
			rows := rig.seedRows(t, mv, "reviewer", "approver")

			require.NoError(t, mv.run(rig))

			require.Len(t, rig.asked, 1)
			for _, row := range rows {
				assert.Equal(t, []uuid.UUID{mv.to(rig)}, rig.holders(row.task.ID))
			}
			assert.Empty(t, rig.logger.errors)
		})
	}
}

func w12aDecodeReassigned(t *testing.T, env events.Envelope[json.RawMessage]) domain.WorkflowTaskReassignedPayload {
	t.Helper()
	var payload domain.WorkflowTaskReassignedPayload
	require.NoError(t, json.Unmarshal(env.Payload, &payload))
	return payload
}

func TestDelegationMove_RecordsOneTaskReassignedEventPerMovedRowInsideTheTransaction(t *testing.T) {
	for _, mv := range w12aMoves() {
		t.Run(mv.name, func(t *testing.T) {
			rig := w12aNewMoveRig()
			rows := rig.seedRows(t, mv, "reviewer", "approver")

			require.NoError(t, mv.run(rig))

			require.Len(t, rig.outbox.enqueued, 2, "one event per moved row")
			assert.Equal(t, []bool{true, true}, rig.out.insideTx, "each event is enqueued while the move's transaction runs")
			assert.Equal(t, 1, rig.tx.commits)
			byTask := map[uuid.UUID]events.Envelope[json.RawMessage]{}
			for _, env := range rig.outbox.enqueued {
				byTask[w12aDecodeReassigned(t, env).TaskID] = env
			}
			for _, row := range rows {
				env, ok := byTask[row.task.ID]
				require.True(t, ok, "an event for each moved task")
				assert.Equal(t, "WorkflowTaskReassigned", env.Type)
				assert.Equal(t, rig.tenantID.String(), env.TenantID)
				assert.Equal(t, "instances/"+row.instance.ID.String()+"/tasks/"+row.task.ID.String(), env.Subject)
				p := w12aDecodeReassigned(t, env)
				assert.Equal(t, row.instance.ID, p.WorkflowInstanceID)
				assert.Equal(t, row.instance.SubjectID, p.SubjectID)
				assert.Equal(t, row.instance.BusinessKey, p.BusinessKey)
				assert.Equal(t, row.versionID, p.WorkflowVersionID)
				assert.Equal(t, row.task.NodeKey, p.NodeKey)
				require.NotNil(t, p.OldUserID)
				assert.Equal(t, mv.from(rig), *p.OldUserID)
				assert.Equal(t, mv.to(rig), p.NewUserID)
				assert.Equal(t, []uuid.UUID{mv.to(rig)}, p.AssigneeUserIDs)
				assert.Equal(t, "delegation", p.Initiator)
				require.NotNil(t, p.DelegationID)
				assert.Equal(t, rig.delegationID, *p.DelegationID)
			}
		})
	}
}

func TestDelegationMove_AnEventThatCannotBeBuiltOrEnqueued_AbortsTheTransaction(t *testing.T) {
	cases := []struct {
		name     string
		failWith func(rig *w12aMoveRig)
		// enqueues is how many enqueues are attempted before the move stops.
		enqueues int
		cause    error
	}{
		{
			name:     "the envelope fails validation",
			failWith: func(rig *w12aMoveRig) { rig.svc.Validator = failingValidator{} },
			enqueues: 0, cause: errValidationFailed,
		},
		{
			name:     "the first enqueue fails",
			failWith: func(rig *w12aMoveRig) { rig.out.failOn, rig.out.failWith = 1, errW12aStoreDown },
			enqueues: 1, cause: errW12aStoreDown,
		},
		{
			name:     "the second enqueue fails after the first event was recorded",
			failWith: func(rig *w12aMoveRig) { rig.out.failOn, rig.out.failWith = 2, errW12aStoreDown },
			enqueues: 2, cause: errW12aStoreDown,
		},
	}
	for _, mv := range w12aMoves() {
		for _, tc := range cases {
			t.Run(mv.name+": "+tc.name, func(t *testing.T) {
				rig := w12aNewMoveRig()
				rows := rig.seedRows(t, mv, "reviewer", "approver")
				tc.failWith(rig)

				err := mv.run(rig)

				require.ErrorIs(t, err, tc.cause)
				assert.Equal(t, tc.enqueues, rig.out.calls)
				rig.requireRolledBack(t, mv, rows)
			})
		}
	}
}

func TestDelegationMove_LeadHandedToADelegateWhoHoldsTheTask_ChecksTheTaskVersionReadInsideTheTransaction(t *testing.T) {
	cases := []struct {
		name string
		// afterInTxRead changes the stored task right after the move read it inside the transaction.
		afterInTxRead func(tasks *fakeTaskRepo, task *domain.Task)
		wantErr       error
	}{
		{name: "the version is current", afterInTxRead: func(*fakeTaskRepo, *domain.Task) {}},
		{
			name:          "the task changes after the in-transaction read",
			afterInTxRead: func(_ *fakeTaskRepo, task *domain.Task) { task.RecordVersion++ },
			wantErr:       domain.ErrRecordVersionConflict,
		},
		{
			name:          "the task is deleted after the in-transaction read",
			afterInTxRead: func(tasks *fakeTaskRepo, task *domain.Task) { delete(tasks.byID, task.ID) },
			wantErr:       domain.ErrNotFound,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w12aNewMoveRig()
			mv := w12aMoves()[0]
			row := rig.seed(t, "reviewer")
			row.assignment.IsLead = true
			holder := &domain.TaskAssignment{ID: uuid.New(), TenantID: rig.tenantID, TaskID: row.task.ID, UserID: rig.delegate, IsActive: true}
			rig.assignments.add(holder)
			changed := false
			rig.svc.Tasks = &w12aMutatingTasks{fakeTaskRepo: rig.tasks, onRead: func(task *domain.Task) {
				if rig.tx.running && !changed {
					changed = true
					tc.afterInTxRead(rig.tasks, task)
				}
			}}
			versionRead := row.task.RecordVersion

			err := mv.run(rig)

			if tc.wantErr == nil {
				require.NoError(t, err)
				assert.False(t, row.assignment.IsActive, "the delegator's row is vacated")
				assert.True(t, holder.IsLead, "the lead flag moves to the delegate's own row")
				assert.True(t, holder.IsActive)
				assert.Empty(t, rig.repo.creates, "no second row is inserted for the delegate")
				assert.Equal(t, versionRead+1, row.task.RecordVersion, "the lead move bumps the task version once")
				require.Len(t, rig.repo.leadMoves, 1)
				assert.Equal(t, w12aSetLeadCall{tenantID: rig.tenantID, taskID: row.task.ID, id: holder.ID, version: versionRead}, rig.repo.leadMoves[0])
				assert.Len(t, rig.outbox.enqueued, 1)
				assert.Equal(t, 1, rig.tx.commits)
				return
			}
			require.ErrorIs(t, err, tc.wantErr, "the event fails and is redelivered")
			require.Len(t, rig.repo.leadMoves, 1, "the failure comes from the lead move")
			assert.Equal(t, w12aSetLeadCall{tenantID: rig.tenantID, taskID: row.task.ID, id: holder.ID, version: versionRead}, rig.repo.leadMoves[0])
			assert.True(t, changed, "the task was changed right after the read made inside the transaction")
			assert.True(t, row.assignment.IsActive, "the delegator's row is back")
			assert.True(t, row.assignment.IsLead, "and is still the lead")
			assert.False(t, holder.IsLead, "the delegate's own row did not take the lead")
			assert.Empty(t, rig.outbox.enqueued)
			assert.Empty(t, rig.outbox.signals)
			assert.Zero(t, rig.tx.commits)
			assert.Equal(t, 1, rig.tx.rollbacks)
		})
	}
}

// w12aMutatingTasks hands out copies of the stored task and calls onRead with the stored one after each read.
type w12aMutatingTasks struct {
	*fakeTaskRepo
	onRead func(stored *domain.Task)
}

func (r *w12aMutatingTasks) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Task, error) {
	stored, err := r.fakeTaskRepo.GetByID(ctx, tenantID, id)
	if err != nil {
		return nil, err
	}
	read := *stored
	r.onRead(stored)
	return &read, nil
}
