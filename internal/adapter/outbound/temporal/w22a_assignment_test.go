package temporal_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// w22aAssignmentsOf lists the assignments of one task that belong to one user,
// in creation order.
func w22aAssignmentsOf(rig *w22aRig, taskID, userID uuid.UUID) []*domain.TaskAssignment {
	var out []*domain.TaskAssignment
	for _, id := range rig.assignmentOrder {
		if a := rig.assignments[id]; a.TaskID == taskID && a.UserID == userID {
			out = append(out, a)
		}
	}
	return out
}

func TestClaimAssignment_AssignmentOfAnotherTenant_NonRetryableNotFoundUnderTheCallersTenant(t *testing.T) {
	t.Parallel()
	rig := w22aNewRig()
	caller, other := uuid.New(), uuid.New()
	inst := rig.addInstance(other, domain.InstanceStatusRunning)
	task := rig.addTask(other, inst, "sales/review", domain.TaskStatusReady)
	foreign := rig.addAssignment(other, task, uuid.New(), false, true)

	err := w22aNewDeps(t, rig).ClaimAssignment(context.Background(), port.ClaimAssignmentInput{TenantID: caller.String(), AssignmentID: foreign.ID.String()})

	w22aRequireNonRetryable(t, err, "NotFoundError")
	assert.ErrorIs(t, err, domain.ErrNotFound)
	rig.requireEveryCallScopedTo(t, caller)
	assert.Equal(t, []string{"Assignments.GetByID"}, rig.ops())
	assert.Equal(t, foreign.ID, rig.calls[0].ID)
	assert.False(t, foreign.IsLead, "the other tenant's assignment must not be claimed")
	assert.Empty(t, rig.outbox)
	assert.Equal(t, 1, rig.rolledBack)
}

// The tenant GUC is set before the transaction opens, because a repository
// call cannot set it for its siblings inside the same transaction.
func TestClaimAssignment_OwnAssignment_EveryCallRunsInTheTransactionUnderTheTenantGUC(t *testing.T) {
	t.Parallel()
	rig := w22aNewRig()
	tenant := uuid.New()
	inst := rig.addInstance(tenant, domain.InstanceStatusRunning)
	task := rig.addTask(tenant, inst, "sales/review", domain.TaskStatusReady)
	own := rig.addAssignment(tenant, task, uuid.New(), false, true)

	err := w22aNewDeps(t, rig).ClaimAssignment(context.Background(), port.ClaimAssignmentInput{TenantID: tenant.String(), AssignmentID: own.ID.String()})

	require.NoError(t, err)
	rig.requireEveryCallScopedTo(t, tenant)
	assert.Contains(t, rig.ops(), "Assignments.ClaimLead")
	assert.Contains(t, rig.ops(), "Outbox.Enqueue")
	assert.True(t, own.IsLead)
}

// A lost record_version race on the claim is a plain error, so Temporal
// retries; the failed attempt leaves nothing behind and the retry reads the
// task again and wins.
func TestClaimAssignment_SetLeadLosesVersionRace_RollsBackAndTheRetrySucceeds(t *testing.T) {
	t.Parallel()
	rig := w22aNewRig()
	tenant := uuid.New()
	inst := rig.addInstance(tenant, domain.InstanceStatusRunning)
	task := rig.addTask(tenant, inst, "sales/review", domain.TaskStatusReady)
	assignment := rig.addAssignment(tenant, task, uuid.New(), false, true)
	staleVersion := task.RecordVersion
	rig.afterRead["Tasks.GetByID"] = func() { rig.tasks[task.ID].RecordVersion++ }
	in := port.ClaimAssignmentInput{TenantID: tenant.String(), AssignmentID: assignment.ID.String()}
	deps := w22aNewDeps(t, rig)

	err := deps.ClaimAssignment(context.Background(), in)

	w22aRequireRetryable(t, err)
	assert.ErrorIs(t, err, domain.ErrRecordVersionConflict)
	assert.Equal(t, 1, rig.rolledBack)
	assert.False(t, assignment.IsLead, "the lost claim must not leave a lead behind")
	assert.Empty(t, rig.outbox, "the lost claim must not announce anything")
	assert.Equal(t, staleVersion+1, task.RecordVersion)

	require.NoError(t, deps.ClaimAssignment(context.Background(), in))

	claims := rig.callsTo("Assignments.ClaimLead")
	require.Len(t, claims, 2)
	assert.Equal(t, staleVersion, claims[0].Version, "the first attempt carried the version it read")
	assert.Equal(t, staleVersion+1, claims[1].Version, "the retry must read the task again")
	assert.Len(t, rig.callsTo("Tasks.GetByID"), 2)
	assert.True(t, assignment.IsLead)
	assert.Equal(t, 1, rig.committed)
	require.Len(t, rig.eventsOfType(domain.EventWorkflowTaskClaimed), 1)

	require.NoError(t, deps.ClaimAssignment(context.Background(), in), "a further retry finds the claim done")
	assert.Len(t, rig.callsTo("Assignments.ClaimLead"), 2, "a claim already made must not be made again")
	assert.Len(t, rig.eventsOfType(domain.EventWorkflowTaskClaimed), 1)
}

// w22aReassignScenario is one task led by oldUser, with the given user either
// already holding the task or not.
type w22aReassignScenario struct {
	rig       *w22aRig
	tenant    uuid.UUID
	task      *domain.Task
	oldLead   *domain.TaskAssignment
	newHolder *domain.TaskAssignment
	in        port.ReassignAssignmentInput
}

func w22aNewReassignScenario(newUserHoldsTask bool) w22aReassignScenario {
	rig := w22aNewRig()
	tenant := uuid.New()
	inst := rig.addInstance(tenant, domain.InstanceStatusRunning)
	task := rig.addTask(tenant, inst, "sales/review", domain.TaskStatusReady)
	oldUser, newUser := uuid.New(), uuid.New()
	s := w22aReassignScenario{rig: rig, tenant: tenant, task: task}
	s.oldLead = rig.addAssignment(tenant, task, oldUser, true, true)
	if newUserHoldsTask {
		s.newHolder = rig.addAssignment(tenant, task, newUser, false, true)
	}
	s.in = port.ReassignAssignmentInput{
		TenantID: tenant.String(), TaskID: task.ID.String(), OldUserID: oldUser.String(),
		NewUserID: newUser.String(), AdminUserID: uuid.NewString(), RecordVersion: task.RecordVersion - 1,
	}
	return s
}

func TestReassignAssignment_StaleRecordVersion_NonRetryableVersionConflictAndRollback(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name         string
		newUserHolds bool
		versionOp    string
	}{
		{name: "new user does not hold the task yet", newUserHolds: false, versionOp: "Tasks.BumpRecordVersion"},
		{name: "new user already holds the task", newUserHolds: true, versionOp: "Assignments.SetLead"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := w22aNewReassignScenario(tc.newUserHolds)
			staleVersion := s.in.RecordVersion

			err := w22aNewDeps(t, s.rig).ReassignAssignment(context.Background(), s.in)

			w22aRequireNonRetryable(t, err, "VersionConflict")
			assert.ErrorIs(t, err, domain.ErrRecordVersionConflict)
			versioned := s.rig.callsTo(tc.versionOp)
			require.Len(t, versioned, 1)
			assert.Equal(t, staleVersion, versioned[0].Version, "the version the caller sent is the one that was checked")
			assert.Equal(t, 1, s.rig.rolledBack)
			assert.Equal(t, 0, s.rig.committed)
			if tc.newUserHolds {
				assert.True(t, s.oldLead.IsActive, "the replaced lead must still hold the task after the rollback")
				assert.True(t, s.oldLead.IsLead)
			}
			assert.Empty(t, s.rig.callsTo("Assignments.Create"), "no assignment is created once the version check fails")
			assert.Empty(t, s.rig.outbox)
			assert.Equal(t, staleVersion+1, s.task.RecordVersion, "the task's record version is back to what the other writer left")
		})
	}
}

// When the version check lost the race the vacate of the old lead had already
// run on the holder path: only the rollback puts the lead back.
func TestReassignAssignment_HolderPathLosesVersionRace_UndoesTheVacate(t *testing.T) {
	t.Parallel()
	s := w22aNewReassignScenario(true)

	err := w22aNewDeps(t, s.rig).ReassignAssignment(context.Background(), s.in)

	w22aRequireNonRetryable(t, err, "VersionConflict")
	assert.Equal(t, []uuid.UUID{s.oldLead.ID}, s.rig.idsOf("Assignments.Vacate"), "the old lead was vacated before the version check failed")
	assert.True(t, s.oldLead.IsActive, "the vacate must not survive the failed attempt")
	assert.False(t, s.newHolder.IsLead)
}

func TestReassignAssignment_VersionCheckFailsForAnotherReason_PlainRetryableError(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name         string
		newUserHolds bool
		failingOp    string
	}{
		{name: "bump of the task version", newUserHolds: false, failingOp: "Tasks.BumpRecordVersion"},
		{name: "set the lead", newUserHolds: true, failingOp: "Assignments.SetLead"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := w22aNewReassignScenario(tc.newUserHolds)
			s.in.RecordVersion = s.task.RecordVersion
			s.rig.failOn(tc.failingOp, errW22aStorage, 0)

			err := w22aNewDeps(t, s.rig).ReassignAssignment(context.Background(), s.in)

			w22aRequireRetryable(t, err)
			assert.ErrorIs(t, err, errW22aStorage)
			assert.Equal(t, 1, s.rig.rolledBack)
			if tc.newUserHolds {
				assert.True(t, s.oldLead.IsActive)
			}
			assert.Empty(t, s.rig.outbox)
		})
	}
}

// The helpers that pick the assignments to vacate and the lead to inherit are
// exercised through a reassignment to a user who does not hold the task yet.
func TestReassignAssignment_OldUserAmongActiveAssignments_VacatesExactlyTheirs(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		// seed adds the active assignments and returns the ids that must be vacated
		// and whether the new assignment must take over the lead.
		seed     func(rig *w22aRig, tenant uuid.UUID, task *domain.Task, oldUser uuid.UUID) (vacated []uuid.UUID, otherUsers []uuid.UUID, leads bool)
		oldNamed bool
	}{
		{
			name:     "no active assignment at all",
			oldNamed: true,
			seed: func(*w22aRig, uuid.UUID, *domain.Task, uuid.UUID) ([]uuid.UUID, []uuid.UUID, bool) {
				return nil, nil, false
			},
		},
		{
			name:     "the old user is not among the active assignments",
			oldNamed: true,
			seed: func(rig *w22aRig, tenant uuid.UUID, task *domain.Task, _ uuid.UUID) ([]uuid.UUID, []uuid.UUID, bool) {
				other := rig.addAssignment(tenant, task, uuid.New(), true, true)
				return nil, []uuid.UUID{other.ID}, false
			},
		},
		{
			name:     "nobody is named as the old user",
			oldNamed: false,
			seed: func(rig *w22aRig, tenant uuid.UUID, task *domain.Task, _ uuid.UUID) ([]uuid.UUID, []uuid.UUID, bool) {
				other := rig.addAssignment(tenant, task, uuid.New(), true, true)
				return nil, []uuid.UUID{other.ID}, false
			},
		},
		{
			name:     "two entries for the old user, the first leading",
			oldNamed: true,
			seed: func(rig *w22aRig, tenant uuid.UUID, task *domain.Task, oldUser uuid.UUID) ([]uuid.UUID, []uuid.UUID, bool) {
				first := rig.addAssignment(tenant, task, oldUser, true, true)
				other := rig.addAssignment(tenant, task, uuid.New(), false, true)
				second := rig.addAssignment(tenant, task, oldUser, false, true)
				return []uuid.UUID{first.ID, second.ID}, []uuid.UUID{other.ID}, true
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rig := w22aNewRig()
			tenant := uuid.New()
			inst := rig.addInstance(tenant, domain.InstanceStatusRunning)
			task := rig.addTask(tenant, inst, "sales/review", domain.TaskStatusReady)
			oldUser, newUser, admin := uuid.New(), uuid.New(), uuid.New()
			vacated, others, leads := tc.seed(rig, tenant, task, oldUser)
			in := port.ReassignAssignmentInput{
				TenantID: tenant.String(), TaskID: task.ID.String(),
				NewUserID: newUser.String(), AdminUserID: admin.String(), RecordVersion: task.RecordVersion,
			}
			if tc.oldNamed {
				in.OldUserID = oldUser.String()
			}

			require.NoError(t, w22aNewDeps(t, rig).ReassignAssignment(context.Background(), in))

			assert.Equal(t, vacated, rig.idsOf("Assignments.Vacate"))
			for _, id := range vacated {
				assert.False(t, rig.assignments[id].IsActive)
			}
			for _, id := range others {
				assert.True(t, rig.assignments[id].IsActive, "a user other than the old one must keep the task")
			}
			created := w22aAssignmentsOf(rig, task.ID, newUser)
			require.Len(t, created, 1)
			assert.True(t, created[0].IsActive)
			assert.Equal(t, leads, created[0].IsLead)
			require.NotNil(t, created[0].AssignedBy)
			assert.Equal(t, admin, *created[0].AssignedBy)
			assert.Len(t, rig.eventsOfType(domain.EventWorkflowTaskReassigned), 1)
		})
	}
}

// Vacating stops at the first failure: the second entry is never reached,
// nothing is created, and the rollback leaves both entries active.
func TestReassignAssignment_VacateFailsOnTheFirstOfTwoEntries_StopsAndRollsBack(t *testing.T) {
	t.Parallel()
	rig := w22aNewRig()
	tenant := uuid.New()
	inst := rig.addInstance(tenant, domain.InstanceStatusRunning)
	task := rig.addTask(tenant, inst, "sales/review", domain.TaskStatusReady)
	oldUser := uuid.New()
	first := rig.addAssignment(tenant, task, oldUser, true, true)
	second := rig.addAssignment(tenant, task, oldUser, false, true)
	rig.failOn("Assignments.Vacate", errW22aStorage, 1)

	err := w22aNewDeps(t, rig).ReassignAssignment(context.Background(), port.ReassignAssignmentInput{
		TenantID: tenant.String(), TaskID: task.ID.String(), OldUserID: oldUser.String(),
		NewUserID: uuid.NewString(), AdminUserID: uuid.NewString(), RecordVersion: task.RecordVersion,
	})

	w22aRequireRetryable(t, err)
	assert.ErrorIs(t, err, errW22aStorage)
	assert.Equal(t, []uuid.UUID{first.ID}, rig.idsOf("Assignments.Vacate"), "the second entry must not be tried after the first failed")
	assert.Empty(t, rig.callsTo("Assignments.Create"))
	assert.True(t, first.IsActive)
	assert.True(t, second.IsActive)
	assert.Equal(t, 1, rig.rolledBack)
	assert.Empty(t, rig.outbox)
}

func TestUpdateTaskStatus_ToSuperseded_ActsOnlyOnReadyAndInProgressTasks(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		from       domain.TaskStatus
		supersedes bool
	}{
		{name: "ready", from: domain.TaskStatusReady, supersedes: true},
		{name: "in progress", from: domain.TaskStatusInProgress, supersedes: true},
		{name: "deferred", from: domain.TaskStatusDeferred, supersedes: false},
		{name: "completed", from: domain.TaskStatusCompleted, supersedes: false},
		{name: "failed", from: domain.TaskStatusFailed, supersedes: false},
		{name: "already superseded", from: domain.TaskStatusSuperseded, supersedes: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rig := w22aNewRig()
			tenant := uuid.New()
			inst := rig.addInstance(tenant, domain.InstanceStatusRunning)
			task := rig.addTask(tenant, inst, "sales/review", tc.from)
			startVersion := task.RecordVersion
			activeA := rig.addAssignment(tenant, task, uuid.New(), true, true)
			activeB := rig.addAssignment(tenant, task, uuid.New(), false, true)
			settled := rig.addAssignment(tenant, task, uuid.New(), false, false)
			in := port.UpdateTaskStatusInput{TenantID: tenant.String(), TaskID: task.ID.String(), Status: domain.TaskStatusSuperseded, RecordVersion: startVersion}
			deps := w22aNewDeps(t, rig)

			require.NoError(t, deps.UpdateTaskStatus(context.Background(), in))
			require.NoError(t, deps.UpdateTaskStatus(context.Background(), in), "a retry of the same call must also succeed")

			if !tc.supersedes {
				assert.Equal(t, tc.from, task.Status)
				assert.Equal(t, startVersion, task.RecordVersion)
				assert.Empty(t, rig.callsTo("Tasks.UpdateStatus"))
				assert.Empty(t, rig.callsTo("Assignments.Vacate"))
				assert.Empty(t, rig.outbox)
				assert.True(t, activeA.IsActive)
				assert.True(t, activeB.IsActive)
				return
			}
			assert.Equal(t, domain.TaskStatusSuperseded, task.Status)
			assert.Equal(t, startVersion+1, task.RecordVersion)
			assert.Equal(t, []uuid.UUID{task.ID}, rig.idsOf("Tasks.UpdateStatus"), "the retry must not write the task again")
			assert.Equal(t, startVersion, rig.callsTo("Tasks.UpdateStatus")[0].Version)
			assert.ElementsMatch(t, []uuid.UUID{activeA.ID, activeB.ID}, rig.idsOf("Assignments.Vacate"))
			assert.NotContains(t, rig.idsOf("Assignments.Vacate"), settled.ID)
			assert.False(t, activeA.IsActive)
			assert.False(t, activeB.IsActive)

			require.Len(t, rig.outbox, 1, "the retry must not announce the supersede a second time")
			require.Equal(t, domain.EventWorkflowTaskSuperseded, rig.outbox[0].Type)
			payload := w22aDecode[domain.WorkflowTaskSupersededPayload](t, rig.outbox[0])
			assert.Equal(t, task.ID, payload.TaskID)
			assert.Equal(t, "sales/review", payload.NodeKey)
			assert.Equal(t, inst.ID, payload.WorkflowInstanceID)
			assert.ElementsMatch(t, []uuid.UUID{activeA.UserID, activeB.UserID}, payload.AssigneeUserIDs)
		})
	}
}
