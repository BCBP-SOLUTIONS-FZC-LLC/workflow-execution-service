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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// dlgfixTasks fails the read of chosen tasks with an error other than "not found".
type dlgfixTasks struct {
	*fakeTaskRepo
	failing map[uuid.UUID]error
}

func (r *dlgfixTasks) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Task, error) {
	if err := r.failing[id]; err != nil {
		return nil, err
	}
	return r.fakeTaskRepo.GetByID(ctx, tenantID, id)
}

// dlgfixInstances fails the read of chosen instances with an error other than "not found".
type dlgfixInstances struct {
	*fakeInstanceRepo
	failing map[uuid.UUID]error
}

func (r *dlgfixInstances) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	if err := r.failing[id]; err != nil {
		return nil, err
	}
	return r.fakeInstanceRepo.GetByID(ctx, tenantID, id)
}

type dlgfixRecon struct {
	svc         *service.DelegationReconciler
	instances   *fakeInstanceRepo
	tasks       *fakeTaskRepo
	assignments *fakeAssignmentRepo
	outbox      *fakeOutbox
	temporal    *fakeTemporalClient
	definitions *dlgfixDefinitions
	eligibility *fakeEligibilityChecker
	store       *fakeUserAvailability
	locker      *fakeLocker
	log         *fakeLogger
	tenantID    uuid.UUID
	delegator   uuid.UUID
	delegate    uuid.UUID
}

func dlgfixNewRecon() *dlgfixRecon {
	h := &dlgfixRecon{
		instances: newFakeInstanceRepo(), tasks: newFakeTaskRepo(), assignments: newFakeAssignmentRepo(),
		outbox: &fakeOutbox{}, temporal: &fakeTemporalClient{}, eligibility: &fakeEligibilityChecker{},
		definitions: &dlgfixDefinitions{byVersion: map[uuid.UUID]*port.CompiledWorkflow{}, failing: map[uuid.UUID]error{}},
		store:       newFakeUserAvailability(), locker: &fakeLocker{}, log: &fakeLogger{},
		tenantID: uuid.New(), delegator: uuid.New(), delegate: uuid.New(),
	}
	h.svc = &service.DelegationReconciler{
		Instances: h.instances, Tasks: h.tasks, Assignments: h.assignments, Outbox: h.outbox,
		Transactor: fakeTransactor{}, Temporal: h.temporal, Definitions: h.definitions, Eligibility: h.eligibility,
		Validator: realValidator(), Availability: h.store, Locks: h.locker, Log: h.log,
	}
	return h
}

type dlgfixRecRow struct {
	instance   *domain.Instance
	task       *domain.Task
	assignment *domain.TaskAssignment
	versionID  uuid.UUID
}

// seed gives the delegator one assignment on a task of a new instance whose
// plan has the given stage role.
func (h *dlgfixRecon) seed(t *testing.T, role string) dlgfixRecRow {
	t.Helper()
	versionID := uuid.New()
	h.definitions.byVersion[versionID] = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, dsl.StageDef{Type: "userTask", NodeID: role, Role: role}))
	inst := &domain.Instance{
		ID: uuid.New(), TenantID: h.tenantID, WorkflowVersionID: versionID, SubjectID: "subject", BusinessKey: uuid.NewString(),
		TemporalWorkflowID: h.tenantID.String() + ":" + uuid.NewString(), Status: domain.InstanceStatusRunning, RecordVersion: 1,
	}
	h.instances.byID[inst.ID] = inst
	return h.seedOn(t, inst, role)
}

// seedOn gives the delegator another task on an existing instance.
func (h *dlgfixRecon) seedOn(t *testing.T, inst *domain.Instance, role string) dlgfixRecRow {
	t.Helper()
	if _, ok := h.definitions.byVersion[inst.WorkflowVersionID]; !ok || role != "" {
		h.definitions.byVersion[inst.WorkflowVersionID] = publishedCompiledWorkflow(uuid.New(), inst.WorkflowVersionID, compiledPlanJSON(t,
			dsl.StageDef{Type: "userTask", NodeID: "reviewer", Role: "reviewer"},
			dsl.StageDef{Type: "userTask", NodeID: "approver", Role: "approver"}))
	}
	task := &domain.Task{ID: uuid.New(), TenantID: h.tenantID, WorkflowInstanceID: inst.ID, NodeKey: "finance/" + role, RecordVersion: 1}
	h.tasks.byID[task.ID] = task
	a := &domain.TaskAssignment{ID: uuid.New(), TenantID: h.tenantID, TaskID: task.ID, UserID: h.delegator, IsActive: true}
	h.assignments.add(a)
	return dlgfixRecRow{instance: inst, task: task, assignment: a, versionID: inst.WorkflowVersionID}
}

func (h *dlgfixRecon) reroute() error {
	return h.svc.Reroute(context.Background(), port.DelegationRerouteInput{
		TenantID: h.tenantID, DelegationID: uuid.New(), DelegatorID: h.delegator, DelegateID: h.delegate, Scope: "all",
	})
}

func (h *dlgfixRecon) requireNothingMoved(t *testing.T, rows ...dlgfixRecRow) {
	t.Helper()
	for _, r := range rows {
		assert.True(t, r.assignment.IsActive, "the delegator keeps the task")
	}
	for _, a := range h.assignments.byID {
		assert.NotEqual(t, h.delegate, a.UserID, "the delegate was given nothing")
	}
	assert.Empty(t, h.outbox.enqueued)
	assert.Empty(t, h.temporal.signals)
}

func TestDelegationReroute_ARowThatCannotBeReadFailsTheEventAndChangesNothing(t *testing.T) {
	t.Run("a task that does not exist", func(t *testing.T) {
		h := dlgfixNewRecon()
		good := h.seed(t, "reviewer")
		h.assignments.add(&domain.TaskAssignment{ID: uuid.New(), TenantID: h.tenantID, TaskID: uuid.New(), UserID: h.delegator, IsActive: true})

		require.ErrorIs(t, h.reroute(), domain.ErrNotFound)
		h.requireNothingMoved(t, good)
	})

	t.Run("a task whose read fails", func(t *testing.T) {
		h := dlgfixNewRecon()
		good, bad := h.seed(t, "reviewer"), h.seed(t, "reviewer")
		h.svc.Tasks = &dlgfixTasks{fakeTaskRepo: h.tasks, failing: map[uuid.UUID]error{bad.task.ID: assert.AnError}}

		require.ErrorIs(t, h.reroute(), assert.AnError)
		h.requireNothingMoved(t, good, bad)
	})

	t.Run("an instance whose read fails", func(t *testing.T) {
		h := dlgfixNewRecon()
		good, bad := h.seed(t, "reviewer"), h.seed(t, "reviewer")
		h.svc.Instances = &dlgfixInstances{fakeInstanceRepo: h.instances, failing: map[uuid.UUID]error{bad.instance.ID: assert.AnError}}

		require.ErrorIs(t, h.reroute(), assert.AnError)
		h.requireNothingMoved(t, good, bad)
	})

	t.Run("an instance that does not exist", func(t *testing.T) {
		h := dlgfixNewRecon()
		good, bad := h.seed(t, "reviewer"), h.seed(t, "reviewer")
		delete(h.instances.byID, bad.instance.ID)

		require.ErrorIs(t, h.reroute(), domain.ErrNotFound)
		h.requireNothingMoved(t, good, bad)
	})
}

func TestDelegationReroute_ACompiledPlanThatCannotBeReadFailsTheEventAndChangesNothing(t *testing.T) {
	causes := []struct {
		name    string
		breakAt func(h *dlgfixRecon, versionID uuid.UUID)
		match   func(t *testing.T, err error)
	}{
		{"the fetch fails", func(h *dlgfixRecon, v uuid.UUID) { h.definitions.failing[v] = port.ErrDefinitionUnavailable },
			func(t *testing.T, err error) { assert.ErrorIs(t, err, port.ErrDefinitionUnavailable) }},
		{"the plan cannot be decoded", func(h *dlgfixRecon, v uuid.UUID) { h.definitions.byVersion[v].CompiledPlanJSON = "not json" },
			func(t *testing.T, err error) { assert.ErrorContains(t, err, "unmarshal compiled plan") }},
		{"the main plan is missing", func(h *dlgfixRecon, v uuid.UUID) {
			h.definitions.byVersion[v].CompiledPlanJSON = `{"main_plan":"ghost","plans":[]}`
		}, func(t *testing.T, err error) { assert.ErrorContains(t, err, "ghost") }},
	}
	for _, tc := range causes {
		t.Run("scope all, later row: "+tc.name, func(t *testing.T) {
			h := dlgfixNewRecon()
			good, bad := h.seed(t, "reviewer"), h.seed(t, "reviewer")
			tc.breakAt(h, bad.versionID)

			err := h.reroute()

			require.Error(t, err)
			tc.match(t, err)
			h.requireNothingMoved(t, good, bad)
		})
	}

	t.Run("department scope", func(t *testing.T) {
		h := dlgfixNewRecon()
		only := h.seed(t, "reviewer")
		h.definitions.failing[only.versionID] = assert.AnError
		dept := uuid.NewString()

		err := h.svc.Reroute(context.Background(), port.DelegationRerouteInput{
			TenantID: h.tenantID, DelegationID: uuid.New(), DelegatorID: h.delegator, DelegateID: h.delegate, Scope: "department", ScopeID: &dept,
		})

		require.ErrorIs(t, err, assert.AnError, "a row is never left out of a scope because its plan could not be read")
		h.requireNothingMoved(t, only)
	})
}

func TestDelegationReverse_ARowThatCannotBeReadFailsTheEventAndChangesNothing(t *testing.T) {
	setup := func(t *testing.T) (*dlgfixRecon, dlgfixRecRow, dlgfixRecRow, port.DelegationReversalInput) {
		h := dlgfixNewRecon()
		delegationID := uuid.New()
		first, second := h.seed(t, "reviewer"), h.seed(t, "reviewer")
		for _, r := range []dlgfixRecRow{first, second} {
			r.assignment.UserID = h.delegate
			r.assignment.Reason = "delegation:" + delegationID.String()
		}
		return h, first, second, port.DelegationReversalInput{
			TenantID: h.tenantID, DelegationID: delegationID, DelegatorID: h.delegator, DelegateID: h.delegate,
		}
	}
	holdsOnlyDelegate := func(t *testing.T, h *dlgfixRecon, rows ...dlgfixRecRow) {
		for _, r := range rows {
			assert.True(t, r.assignment.IsActive)
			assert.Equal(t, h.delegate, r.assignment.UserID)
		}
		assert.Len(t, h.assignments.byID, len(rows), "no row was created")
		assert.Empty(t, h.outbox.enqueued)
		assert.Empty(t, h.temporal.signals)
	}

	t.Run("a task whose read fails", func(t *testing.T) {
		h, first, second, in := setup(t)
		h.svc.Tasks = &dlgfixTasks{fakeTaskRepo: h.tasks, failing: map[uuid.UUID]error{second.task.ID: assert.AnError}}

		require.ErrorIs(t, h.svc.Reverse(context.Background(), in), assert.AnError)
		holdsOnlyDelegate(t, h, first, second)
	})

	t.Run("a compiled plan that cannot be read", func(t *testing.T) {
		h, first, second, in := setup(t)
		h.definitions.failing[second.versionID] = assert.AnError

		require.ErrorIs(t, h.svc.Reverse(context.Background(), in), assert.AnError)
		holdsOnlyDelegate(t, h, first, second)
	})
}

func TestDelegationReroute_AnEligibilityAnswerListOfTheWrongLengthIsAnError(t *testing.T) {
	for _, tc := range []struct {
		name    string
		answers int
	}{
		{"fewer answers than requests", 1},
		{"more answers than requests", 3},
		{"no answers at all", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := dlgfixNewRecon()
			first, second := h.seed(t, "reviewer"), h.seed(t, "approver")
			h.eligibility.batchCheck = func(_ context.Context, _ uuid.UUID, reqs []port.EligibilityCheckRequest) ([]port.EligibilityResult, error) {
				require.Len(t, reqs, 2, "two distinct levels are asked")
				out := make([]port.EligibilityResult, tc.answers)
				for i := range out {
					out[i].Eligible = true
				}
				return out, nil
			}

			var err error
			require.NotPanics(t, func() { err = h.reroute() })

			require.Error(t, err, "the run is skipped and retried, never answered from a malformed list")
			assert.NotEmpty(t, h.log.errorCalls, "the mismatch is logged")
			h.requireNothingMoved(t, first, second)
		})
	}

	t.Run("the same holds on a reversal", func(t *testing.T) {
		h := dlgfixNewRecon()
		delegationID := uuid.New()
		row := h.seed(t, "reviewer")
		row.assignment.UserID = h.delegate
		row.assignment.Reason = "delegation:" + delegationID.String()
		h.eligibility.batchCheck = func(context.Context, uuid.UUID, []port.EligibilityCheckRequest) ([]port.EligibilityResult, error) {
			return nil, nil
		}

		var err error
		require.NotPanics(t, func() {
			err = h.svc.Reverse(context.Background(), port.DelegationReversalInput{
				TenantID: h.tenantID, DelegationID: delegationID, DelegatorID: h.delegator, DelegateID: h.delegate,
			})
		})

		require.Error(t, err)
		assert.True(t, row.assignment.IsActive)
		assert.Equal(t, h.delegate, row.assignment.UserID)
	})
}

// dlgfixSnapshotInstances hands out copies, so a status change after the read
// is not visible to the caller, as with a row read from the database.
type dlgfixSnapshotInstances struct{ *fakeInstanceRepo }

func (r *dlgfixSnapshotInstances) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	inst, err := r.fakeInstanceRepo.GetByID(ctx, tenantID, id)
	if err != nil {
		return nil, err
	}
	snapshot := *inst
	return &snapshot, nil
}

// transactional gives the reconciler a transactor that rolls back and aborts on
// a unique violation, with the assignment repository wired to it.
func (h *dlgfixRecon) transactional() (*dlgfixTx, *dlgfixAssignments) {
	tx := &dlgfixTx{repo: h.assignments}
	repo := &dlgfixAssignments{fakeAssignmentRepo: h.assignments, tx: tx, vacateFails: map[uuid.UUID]error{}, createFails: map[uuid.UUID]error{}}
	h.svc.Assignments = repo
	h.svc.Transactor = tx
	return tx, repo
}

func TestDelegationReroute_ADelegateWhoAlreadyHoldsTheTaskKeepsOneRowAndTheTransactionLives(t *testing.T) {
	h := dlgfixNewRecon()
	tx, repo := h.transactional()
	row := h.seed(t, "reviewer")
	row.assignment.IsLead = true
	existing := &domain.TaskAssignment{ID: uuid.New(), TenantID: h.tenantID, TaskID: row.task.ID, UserID: h.delegate, IsActive: true}
	h.assignments.add(existing)

	require.NoError(t, h.reroute())

	assert.False(t, row.assignment.IsActive)
	assert.True(t, existing.IsLead, "the claim moves to the row the delegate already holds")
	assert.Empty(t, repo.created, "no second row is attempted, so the unique violation never aborts the transaction")
	assert.Equal(t, 1, tx.commits)
	assert.Len(t, h.outbox.enqueued, 1)
}

func TestDelegationReroute_AnAssignmentThatEndedBetweenTheReadAndTheWriteIsLeftAlone(t *testing.T) {
	h := dlgfixNewRecon()
	tx, repo := h.transactional()
	ended, live := h.seed(t, "reviewer"), h.seed(t, "reviewer")
	tx.onStart = func() { ended.assignment.IsActive = false }

	require.NoError(t, h.reroute())

	assert.NotContains(t, repo.vacated, ended.assignment.ID, "an assignment that is no longer active is not vacated again")
	for _, a := range h.assignments.byID {
		assert.False(t, a.TaskID == ended.task.ID && a.UserID == h.delegate, "no delegate row for a task whose assignment ended")
	}
	assert.False(t, live.assignment.IsActive)
	assert.Len(t, h.outbox.enqueued, 1, "one event, for the row that moved")
	require.Len(t, h.temporal.signals, 1)
	assert.Equal(t, live.instance.ID, h.temporal.signals[0].InstanceID)
}

func TestDelegationReverse_AnAssignmentThatEndedBetweenTheReadAndTheWriteIsLeftAlone(t *testing.T) {
	h := dlgfixNewRecon()
	tx, repo := h.transactional()
	delegationID := uuid.New()
	ended := h.seed(t, "reviewer")
	ended.assignment.UserID = h.delegate
	ended.assignment.Reason = "delegation:" + delegationID.String()
	tx.onStart = func() { ended.assignment.IsActive = false }

	require.NoError(t, h.svc.Reverse(context.Background(), port.DelegationReversalInput{
		TenantID: h.tenantID, DelegationID: delegationID, DelegatorID: h.delegator, DelegateID: h.delegate,
	}))

	assert.Empty(t, repo.vacated)
	for _, a := range h.assignments.byID {
		assert.NotEqual(t, h.delegator, a.UserID, "nothing is handed back for an assignment that ended")
	}
	assert.Empty(t, h.outbox.enqueued)
	assert.Empty(t, h.temporal.signals)
}

func dlgfixResumeCount(h *dlgfixRecon, instanceID uuid.UUID) int {
	n := 0
	for _, s := range h.temporal.signals {
		if s.SignalName == port.SignalInstanceResume && s.InstanceID == instanceID {
			n++
		}
	}
	return n
}

func TestDelegationReroute_ResumesAnInstancePausedForTheDelegatorsAbsence(t *testing.T) {
	t.Run("a paused instance of an absent delegator is resumed once, as out of office", func(t *testing.T) {
		h := dlgfixNewRecon()
		first := h.seed(t, "reviewer")
		first.instance.Status = domain.InstanceStatusPaused
		second := h.seedOn(t, first.instance, "reviewer")
		require.NoError(t, h.store.byRecord(h.tenantID, h.delegator, "ooo"))

		require.NoError(t, h.reroute())

		assert.False(t, first.assignment.IsActive)
		assert.False(t, second.assignment.IsActive)
		assert.Equal(t, 1, dlgfixResumeCount(h, first.instance.ID), "one resume for the instance, not one per task")
		for _, s := range h.temporal.signals {
			if s.SignalName != port.SignalInstanceResume {
				continue
			}
			raw, err := json.Marshal(s.Payload)
			require.NoError(t, err)
			assert.Contains(t, string(raw), `"Initiator":"`+domain.InitiatorOOO+`"`, "only the pause that out of office made can be released by it")
			assert.Equal(t, first.instance.TemporalWorkflowID, s.TemporalWorkflowID)
		}
		assert.Equal(t, []string{port.UserAvailabilityScope(h.tenantID, h.delegator)}, h.locker.keys, "the status is read under the delegator's availability lock")
	})

	t.Run("a paused instance of a delegator who is available is left paused", func(t *testing.T) {
		h := dlgfixNewRecon()
		row := h.seed(t, "reviewer")
		row.instance.Status = domain.InstanceStatusPaused

		require.NoError(t, h.reroute())

		assert.False(t, row.assignment.IsActive)
		assert.Zero(t, dlgfixResumeCount(h, row.instance.ID), "an administrator's pause is not the delegator's absence")
	})

	t.Run("an instance read as running still gets the resume, because its pause may land after the read", func(t *testing.T) {
		h := dlgfixNewRecon()
		row := h.seed(t, "reviewer")
		h.svc.Instances = &dlgfixSnapshotInstances{fakeInstanceRepo: h.instances}
		require.NoError(t, h.store.byRecord(h.tenantID, h.delegator, "ooo"))
		h.store.onGet = func() { row.instance.Status = domain.InstanceStatusPaused }

		require.NoError(t, h.reroute())

		assert.False(t, row.assignment.IsActive)
		assert.Equal(t, 1, dlgfixResumeCount(h, row.instance.ID), "the workflow drops a resume it cannot use, so none is held back")
	})

	t.Run("an instance where the delegator keeps a task is left paused", func(t *testing.T) {
		h := dlgfixNewRecon()
		moved := h.seed(t, "reviewer")
		moved.instance.Status = domain.InstanceStatusPaused
		kept := h.seedOn(t, moved.instance, "approver")
		require.NoError(t, h.store.byRecord(h.tenantID, h.delegator, "ooo"))
		h.eligibility.batchCheck = func(_ context.Context, _ uuid.UUID, reqs []port.EligibilityCheckRequest) ([]port.EligibilityResult, error) {
			out := make([]port.EligibilityResult, len(reqs))
			for i, r := range reqs {
				out[i].Eligible = r.RequiredLevel == "reviewer"
			}
			return out, nil
		}

		require.NoError(t, h.reroute())

		assert.False(t, moved.assignment.IsActive)
		assert.True(t, kept.assignment.IsActive)
		assert.Zero(t, dlgfixResumeCount(h, moved.instance.ID), "the delegator is still absent from work on this instance")
	})

	t.Run("a resume that cannot be sent fails the event before anything changes", func(t *testing.T) {
		h := dlgfixNewRecon()
		row := h.seed(t, "reviewer")
		row.instance.Status = domain.InstanceStatusPaused
		require.NoError(t, h.store.byRecord(h.tenantID, h.delegator, "ooo"))
		h.temporal.signalFunc = func(context.Context, string, uuid.UUID, string, any) error { return errors.New("temporal down") }

		require.Error(t, h.reroute())

		assert.True(t, row.assignment.IsActive, "the redelivered event finds the work still with the delegator")
		assert.Empty(t, h.outbox.enqueued)
	})

	t.Run("an availability read that fails fails the event before anything changes", func(t *testing.T) {
		h := dlgfixNewRecon()
		row := h.seed(t, "reviewer")
		row.instance.Status = domain.InstanceStatusPaused
		h.store.getErr = assert.AnError

		require.ErrorIs(t, h.reroute(), assert.AnError)

		h.requireNothingMoved(t, row)
	})
}

func dlgfixNewOOO() (*service.OOOAvailabilityReconciler, *dlgfixOOO) {
	w := &dlgfixOOO{
		instances: newFakeInstanceRepo(), tasks: newFakeTaskRepo(), assignments: newFakeAssignmentRepo(),
		temporal: &fakeTemporalClient{}, store: newFakeUserAvailability(), log: &fakeLogger{},
		tenantID: uuid.New(), userID: uuid.New(),
	}
	svc := &service.OOOAvailabilityReconciler{
		Instances: w.instances, Tasks: w.tasks, Assignments: w.assignments,
		Temporal: w.temporal, Availability: w.store, Log: w.log,
	}
	w.svc = svc
	return svc, w
}

type dlgfixOOO struct {
	svc         *service.OOOAvailabilityReconciler
	instances   *fakeInstanceRepo
	tasks       *fakeTaskRepo
	assignments *fakeAssignmentRepo
	temporal    *fakeTemporalClient
	store       *fakeUserAvailability
	log         *fakeLogger
	tenantID    uuid.UUID
	userID      uuid.UUID
}

func (w *dlgfixOOO) seed() (*domain.Instance, *domain.Task) {
	inst := &domain.Instance{ID: uuid.New(), TenantID: w.tenantID, Status: domain.InstanceStatusRunning, TemporalWorkflowID: "tenant:" + uuid.NewString(), RecordVersion: 1}
	w.instances.byID[inst.ID] = inst
	task := &domain.Task{ID: uuid.New(), TenantID: w.tenantID, WorkflowInstanceID: inst.ID}
	w.tasks.byID[task.ID] = task
	w.assignments.add(&domain.TaskAssignment{ID: uuid.New(), TenantID: w.tenantID, TaskID: task.ID, UserID: w.userID, IsActive: true})
	return inst, task
}

func (w *dlgfixOOO) apply(status string) error {
	return w.svc.Apply(context.Background(), port.UserAvailabilityInput{TenantID: w.tenantID, UserID: w.userID, Status: status})
}

func TestOOOAvailability_APauseOrResumeThatCannotBeDeliveredFailsTheEvent(t *testing.T) {
	for _, tc := range []struct{ status, signal string }{
		{"ooo", port.SignalInstancePause},
		{"available", port.SignalInstanceResume},
	} {
		t.Run(tc.status+": a signal that fails fails the event, and the other instances are still told", func(t *testing.T) {
			_, w := dlgfixNewOOO()
			first, _ := w.seed()
			second, _ := w.seed()
			w.temporal.signalFunc = func(_ context.Context, _ string, id uuid.UUID, _ string, _ any) error {
				if id == first.ID || id == second.ID {
					return assert.AnError
				}
				return nil
			}

			err := w.apply(tc.status)

			require.ErrorIs(t, err, assert.AnError, "the consumer redelivers the event, and the signals are sent again")
			require.Len(t, w.temporal.signals, 2, "one failure does not stop the others")
			assert.Equal(t, tc.signal, w.temporal.signals[0].SignalName)
		})
	}

	t.Run("a signal to a workflow that no longer exists is not a failure", func(t *testing.T) {
		_, w := dlgfixNewOOO()
		w.seed()
		w.temporal.signalFunc = func(context.Context, string, uuid.UUID, string, any) error { return port.ErrWorkflowNotFound }

		require.NoError(t, w.apply("ooo"))
	})
}

func TestOOOAvailability_ARowThatCannotBeReadFailsTheEventUnlessItNoLongerExists(t *testing.T) {
	t.Run("a task whose read fails", func(t *testing.T) {
		_, w := dlgfixNewOOO()
		_, task := w.seed()
		other, _ := w.seed()
		w.svc.Tasks = &dlgfixTasks{fakeTaskRepo: w.tasks, failing: map[uuid.UUID]error{task.ID: assert.AnError}}

		err := w.apply("ooo")

		require.ErrorIs(t, err, assert.AnError)
		require.Len(t, w.temporal.signals, 1, "the readable instance is still paused")
		assert.Equal(t, other.ID, w.temporal.signals[0].InstanceID)
	})

	t.Run("an instance whose read fails", func(t *testing.T) {
		_, w := dlgfixNewOOO()
		inst, _ := w.seed()
		w.svc.Instances = &dlgfixInstances{fakeInstanceRepo: w.instances, failing: map[uuid.UUID]error{inst.ID: assert.AnError}}

		require.ErrorIs(t, w.apply("available"), assert.AnError)
		assert.Empty(t, w.temporal.signals)
	})

	t.Run("a task or instance that no longer exists is skipped", func(t *testing.T) {
		_, w := dlgfixNewOOO()
		goneInstance, _ := w.seed()
		delete(w.instances.byID, goneInstance.ID)
		_, goneTask := w.seed()
		delete(w.tasks.byID, goneTask.ID)
		live, _ := w.seed()

		require.NoError(t, w.apply("ooo"))

		require.Len(t, w.temporal.signals, 1)
		assert.Equal(t, live.ID, w.temporal.signals[0].InstanceID)
	})
}
