package temporal_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	outboundtemporal "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

const w22aDegradedCounter = "workflow_execution_service_instance_degraded_total"

func w22aStatusInput(tenant uuid.UUID, inst *domain.Instance, status domain.InstanceStatus) port.UpdateInstanceStatusInput {
	return port.UpdateInstanceStatusInput{InstanceID: inst.ID.String(), TenantID: tenant.String(), Status: status}
}

func TestUpdateInstanceStatus_Completed_FinishedEventCarriesTheGivenTimeOrTheRowsUpdatedAt(t *testing.T) {
	t.Parallel()
	given := time.Date(2026, 5, 6, 7, 8, 9, 0, time.UTC)
	cases := []struct {
		name        string
		completedAt *time.Time
		// wantRowTime is true when the event must carry the time the repository
		// stamped on the updated row.
		wantRowTime bool
	}{
		{name: "completion time given", completedAt: &given},
		{name: "no completion time", completedAt: nil, wantRowTime: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rig := w22aNewRig()
			tenant := uuid.New()
			inst := rig.addInstance(tenant, domain.InstanceStatusRunning)
			seededUpdatedAt, seededVersion := inst.UpdatedAt, inst.RecordVersion
			in := w22aStatusInput(tenant, inst, domain.InstanceStatusCompleted)
			in.CompletedAt = tc.completedAt

			require.NoError(t, w22aNewDeps(t, rig).UpdateInstanceStatus(context.Background(), in))

			assert.Equal(t, domain.InstanceStatusCompleted, inst.Status)
			assert.Equal(t, seededVersion+1, inst.RecordVersion)
			writes := rig.callsTo("Instances.UpdateStatus")
			require.Len(t, writes, 1)
			assert.Equal(t, seededVersion, writes[0].Version)
			require.NotEqual(t, seededUpdatedAt, inst.UpdatedAt, "the update must stamp the row for the test to tell the sources apart")

			finished := rig.eventsOfType(domain.EventWorkflowInstanceFinished)
			require.Len(t, finished, 1)
			payload := w22aDecode[domain.WorkflowInstanceFinishedPayload](t, finished[0])
			want := given
			if tc.wantRowTime {
				want = inst.UpdatedAt
			}
			assert.True(t, want.Equal(payload.CompletedAt), "completed_at is %s, want %s", payload.CompletedAt, want)
			assert.Equal(t, inst.StartedByUserID, payload.StartedByUserID)
			assert.Equal(t, inst.SubjectID, payload.SubjectID)
			assert.Equal(t, inst.BusinessKey, payload.BusinessKey)
			assert.Equal(t, inst.WorkflowVersionID, payload.WorkflowVersionID)
		})
	}
}

// Both targets are retried by Temporal after a lost acknowledgement; the second
// attempt finds the instance already there and changes nothing. Serial: the
// degraded counter is process-wide.
func TestUpdateInstanceStatus_AlreadyInTheTargetStatus_NoWriteNoEventNoMetric(t *testing.T) {
	obstest.Init(t)
	for _, status := range []domain.InstanceStatus{domain.InstanceStatusDegraded, domain.InstanceStatusRunning} {
		t.Run(string(status), func(t *testing.T) {
			rig := w22aNewRig()
			tenant := uuid.New()
			inst := rig.addInstance(tenant, status)
			startVersion := inst.RecordVersion
			before := obstest.Sum(t, w22aDegradedCounter, nil)

			err := w22aNewDeps(t, rig).UpdateInstanceStatus(context.Background(), w22aStatusInput(tenant, inst, status))

			require.NoError(t, err)
			assert.Empty(t, rig.callsTo("Instances.UpdateStatus"), "an instance already in the status is not written")
			assert.Empty(t, rig.outbox)
			assert.Equal(t, startVersion, inst.RecordVersion)
			assert.Equal(t, status, inst.Status)
			assert.Equal(t, before, obstest.Sum(t, w22aDegradedCounter, nil))
			assert.Equal(t, 1, rig.committed)
		})
	}
}

func TestUpdateInstanceStatus_ResumeAndQuietTargets_EmitOnlyWhatTheTargetCalls(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		from, to  domain.InstanceStatus
		wantEvent string
	}{
		{name: "degraded to running", from: domain.InstanceStatusDegraded, to: domain.InstanceStatusRunning, wantEvent: domain.EventWorkflowInstanceResumed},
		{name: "running to paused", from: domain.InstanceStatusRunning, to: domain.InstanceStatusPaused},
		{name: "degraded to paused", from: domain.InstanceStatusDegraded, to: domain.InstanceStatusPaused},
		{name: "running to terminated", from: domain.InstanceStatusRunning, to: domain.InstanceStatusTerminated},
		{name: "paused to terminated", from: domain.InstanceStatusPaused, to: domain.InstanceStatusTerminated},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rig := w22aNewRig()
			tenant := uuid.New()
			inst := rig.addInstance(tenant, tc.from)
			startVersion := inst.RecordVersion

			require.NoError(t, w22aNewDeps(t, rig).UpdateInstanceStatus(context.Background(), w22aStatusInput(tenant, inst, tc.to)))

			assert.Equal(t, tc.to, inst.Status, "the status is written")
			assert.Equal(t, startVersion+1, inst.RecordVersion)
			writes := rig.callsTo("Instances.UpdateStatus")
			require.Len(t, writes, 1)
			assert.Equal(t, startVersion, writes[0].Version)
			if tc.wantEvent == "" {
				assert.Empty(t, rig.outbox, "no event is defined for reaching %s through this activity", tc.to)
				return
			}
			require.Equal(t, []string{tc.wantEvent}, rig.eventTypes())
			payload := w22aDecode[domain.WorkflowInstanceResumedPayload](t, rig.outbox[0])
			assert.Equal(t, domain.InitiatorDegradedRecovery, payload.Initiator)
			assert.Nil(t, payload.ActorUserID, "a recovery has no acting user")
			assert.Equal(t, inst.StartedByUserID, payload.StartedByUserID)
			assert.Equal(t, inst.ID, payload.WorkflowInstanceID)
		})
	}
}

// Pause, resume and the status update all read the instance first: one that is
// not there (or belongs to another tenant) ends the activity for good.
func TestInstanceActivities_UnknownInstance_NonRetryableNotFoundAndNothingWritten(t *testing.T) {
	t.Parallel()
	calls := map[string]func(d *outboundtemporal.Deps, tenant, instance string) error{
		"update": func(d *outboundtemporal.Deps, tenant, instance string) error {
			return d.UpdateInstanceStatus(context.Background(), port.UpdateInstanceStatusInput{TenantID: tenant, InstanceID: instance, Status: domain.InstanceStatusCompleted})
		},
		"pause": func(d *outboundtemporal.Deps, tenant, instance string) error {
			return d.PauseInstance(context.Background(), port.PauseInstanceInput{TenantID: tenant, InstanceID: instance, Initiator: domain.InitiatorAdmin})
		},
		"resume": func(d *outboundtemporal.Deps, tenant, instance string) error {
			return d.ResumeInstance(context.Background(), port.ResumeInstanceInput{TenantID: tenant, InstanceID: instance, Initiator: domain.InitiatorAdmin})
		},
	}
	for name, call := range calls {
		for _, owner := range []string{"nobody", "another tenant"} {
			t.Run(name+"/"+owner, func(t *testing.T) {
				t.Parallel()
				rig := w22aNewRig()
				caller, other := uuid.New(), uuid.New()
				instanceID := uuid.New()
				var stored *domain.Instance
				if owner == "another tenant" {
					stored = rig.addInstance(other, domain.InstanceStatusPaused)
					instanceID = stored.ID
				}

				err := call(w22aNewDeps(t, rig), caller.String(), instanceID.String())

				w22aRequireNonRetryable(t, err, "NotFoundError")
				assert.ErrorIs(t, err, domain.ErrNotFound)
				assert.Equal(t, []string{"Instances.GetByID"}, rig.ops())
				assert.Equal(t, instanceID, rig.calls[0].ID)
				assert.Equal(t, caller, rig.calls[0].Tenant)
				assert.Empty(t, rig.outbox)
				if stored != nil {
					assert.Equal(t, domain.InstanceStatusPaused, stored.Status)
				}
			})
		}
	}
}

func TestCancelInstance_OpenTasks_FailsThemTerminatesAndAnnouncesCancelledOnce(t *testing.T) {
	t.Parallel()
	reason := "customer withdrew"
	cases := []struct {
		name       string
		from       domain.InstanceStatus
		reason     *string
		wantFailed int
	}{
		{name: "running with a reason", from: domain.InstanceStatusRunning, reason: &reason, wantFailed: 2},
		{name: "paused without a reason", from: domain.InstanceStatusPaused, reason: nil, wantFailed: 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rig := w22aNewRig()
			tenant, admin := uuid.New(), uuid.New()
			inst := rig.addInstance(tenant, tc.from)
			startVersion := inst.RecordVersion
			inProgress := rig.addTask(tenant, inst, "sales/review", domain.TaskStatusInProgress)
			ready := rig.addTask(tenant, inst, "sales/approve", domain.TaskStatusReady)
			done := rig.addTask(tenant, inst, "sales/intake", domain.TaskStatusCompleted)
			holderA := rig.addAssignment(tenant, inProgress, uuid.New(), true, true)
			holderB := rig.addAssignment(tenant, ready, uuid.New(), true, true)
			in := port.CancelInstanceInput{
				InstanceID: inst.ID.String(), TenantID: tenant.String(), AdminUserID: admin.String(), Reason: tc.reason,
				RecordVersion: 99, // stale by design: the activity reads the instance itself
			}
			deps := w22aNewDeps(t, rig)

			require.NoError(t, deps.CancelInstance(context.Background(), in))

			assert.Equal(t, domain.TaskStatusFailed, inProgress.Status)
			assert.Equal(t, domain.TaskStatusFailed, ready.Status)
			assert.Equal(t, domain.TaskStatusCompleted, done.Status, "a settled task is left alone")
			assert.False(t, holderA.IsActive)
			assert.False(t, holderB.IsActive)
			assert.Equal(t, domain.InstanceStatusTerminated, inst.Status)
			writes := rig.callsTo("Instances.UpdateStatus")
			require.Len(t, writes, 1)
			assert.Equal(t, startVersion, writes[0].Version, "the version written is the one read, not the caller's")

			require.Len(t, rig.outbox, tc.wantFailed+1)
			failed := rig.eventsOfType(domain.EventWorkflowTaskFailed)
			require.Len(t, failed, tc.wantFailed)
			assigneesByTask := map[uuid.UUID]uuid.UUID{}
			for _, env := range failed {
				p := w22aDecode[domain.WorkflowTaskFailedPayload](t, env)
				assert.Equal(t, "instance_cancel", p.CascadeSource)
				require.Len(t, p.AssigneeUserIDs, 1)
				assigneesByTask[p.TaskID] = p.AssigneeUserIDs[0]
			}
			assert.Equal(t, map[uuid.UUID]uuid.UUID{inProgress.ID: holderA.UserID, ready.ID: holderB.UserID}, assigneesByTask)

			last := rig.outbox[len(rig.outbox)-1]
			require.Equal(t, domain.EventWorkflowInstanceCancelled, last.Type, "the cancellation is announced after the tasks it failed")
			cancelled := w22aDecode[domain.WorkflowInstanceCancelledPayload](t, last)
			assert.Equal(t, admin, cancelled.ActorUserID)
			assert.Equal(t, inst.StartedByUserID, cancelled.StartedByUserID)
			assert.Equal(t, tc.reason, cancelled.Reason)
			assert.Len(t, rig.eventsOfType(domain.EventWorkflowInstanceCancelled), 1)
			rig.requireEveryCallScopedTo(t, tenant)

			taskWrites, instanceWrites, events := rig.idsOf("Tasks.UpdateStatus"), rig.idsOf("Instances.UpdateStatus"), len(rig.outbox)
			require.NoError(t, deps.CancelInstance(context.Background(), in), "a retry of the same cancel must succeed")
			assert.Equal(t, taskWrites, rig.idsOf("Tasks.UpdateStatus"), "the retry must not write a task again")
			assert.Equal(t, instanceWrites, rig.idsOf("Instances.UpdateStatus"), "the retry must not write the instance again")
			assert.Len(t, rig.outbox, events, "the retry must not announce again")
		})
	}
}

// w22aSeedOpenTasks adds count ready tasks to the instance and returns them in
// creation order.
func w22aSeedOpenTasks(rig *w22aRig, tenant uuid.UUID, inst *domain.Instance, count int) []*domain.Task {
	tasks := make([]*domain.Task, count)
	for i := range tasks {
		tasks[i] = rig.addTask(tenant, inst, "sales/review", domain.TaskStatusReady)
	}
	return tasks
}

func TestUpdateInstanceStatus_Failed_FailsEveryOpenTaskAcrossPagesAndSkipsTheRest(t *testing.T) {
	t.Parallel()
	rig := w22aNewRig()
	tenant := uuid.New()
	inst := rig.addInstance(tenant, domain.InstanceStatusRunning)
	var wantFailed []uuid.UUID
	untouched := map[uuid.UUID]domain.TaskStatus{}
	seed := func(count int, status domain.TaskStatus, fails bool) {
		for range count {
			task := rig.addTask(tenant, inst, "sales/review", status)
			if fails {
				wantFailed = append(wantFailed, task.ID)
			} else {
				untouched[task.ID] = status
			}
		}
	}
	seed(40, domain.TaskStatusReady, true)
	seed(20, domain.TaskStatusCompleted, false)
	seed(30, domain.TaskStatusInProgress, true)
	seed(10, domain.TaskStatusDeferred, false)
	seed(10, domain.TaskStatusSuperseded, false)
	seed(10, domain.TaskStatusFailed, false)
	seed(5, domain.TaskStatusReady, true)
	require.Len(t, rig.taskOrder, 125)

	require.NoError(t, w22aNewDeps(t, rig).UpdateInstanceStatus(context.Background(), w22aStatusInput(tenant, inst, domain.InstanceStatusFailed)))

	require.Len(t, rig.pageRequests, 3, "125 tasks at 50 a page need three listings")
	for _, page := range rig.pageRequests {
		assert.Equal(t, w22aPageSize, page.Limit)
	}
	assert.Nil(t, rig.pageRequests[0].After)
	require.NotNil(t, rig.pageRequests[1].After)
	assert.Equal(t, rig.taskOrder[49], rig.pageRequests[1].After.ID)
	require.NotNil(t, rig.pageRequests[2].After)
	assert.Equal(t, rig.taskOrder[99], rig.pageRequests[2].After.ID)

	assert.ElementsMatch(t, wantFailed, rig.idsOf("Tasks.UpdateStatus"))
	for _, id := range wantFailed {
		assert.Equal(t, domain.TaskStatusFailed, rig.tasks[id].Status)
	}
	for id, status := range untouched {
		assert.Equal(t, status, rig.tasks[id].Status)
	}
	failedEvents := rig.eventsOfType(domain.EventWorkflowTaskFailed)
	assert.Len(t, failedEvents, len(wantFailed))
	for _, env := range failedEvents {
		assert.Equal(t, "instance_failed", w22aDecode[domain.WorkflowTaskFailedPayload](t, env).CascadeSource)
	}
	assert.Equal(t, domain.EventWorkflowInstanceFailed, rig.outbox[len(rig.outbox)-1].Type)
}

// An error on any page ends the walk at once, and because the whole activity is
// one transaction, nothing done on the earlier pages survives.
func TestUpdateInstanceStatus_Failed_ErrorOnALaterPageAbortsAndRollsEverythingBack(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name         string
		fail         func(rig *w22aRig)
		wantListings int
		wantWrites   int
	}{
		{
			name:         "listing the second page fails",
			fail:         func(rig *w22aRig) { rig.failOn("Tasks.ListByInstance", errW22aStorage, 2) },
			wantListings: 2,
			wantWrites:   w22aPageSize,
		},
		{
			name:         "writing a task of the second page fails",
			fail:         func(rig *w22aRig) { rig.failOn("Tasks.UpdateStatus", errW22aStorage, 60) },
			wantListings: 2,
			wantWrites:   60,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rig := w22aNewRig()
			tenant := uuid.New()
			inst := rig.addInstance(tenant, domain.InstanceStatusRunning)
			tasks := w22aSeedOpenTasks(rig, tenant, inst, 120)
			tc.fail(rig)

			err := w22aNewDeps(t, rig).UpdateInstanceStatus(context.Background(), w22aStatusInput(tenant, inst, domain.InstanceStatusFailed))

			w22aRequireRetryable(t, err)
			assert.ErrorIs(t, err, errW22aStorage)
			assert.Len(t, rig.pageRequests, tc.wantListings, "no page is read after the failure")
			assert.Len(t, rig.callsTo("Tasks.UpdateStatus"), tc.wantWrites, "no task is written after the failure")
			assert.Equal(t, 1, rig.rolledBack)
			assert.Empty(t, rig.outbox)
			assert.Equal(t, domain.InstanceStatusRunning, inst.Status, "the instance status write is rolled back too")
			for _, task := range tasks {
				assert.Equal(t, domain.TaskStatusReady, task.Status)
			}
		})
	}
}

// The tasks are written at the versions the listing showed. When another writer
// moves one of them in between, the attempt rolls back as a plain error and the
// retry, which lists again, succeeds without repeating anything.
func TestCancelInstance_TaskVersionMovedAfterTheListing_RollsBackAndTheRetryFailsEachTaskOnce(t *testing.T) {
	t.Parallel()
	rig := w22aNewRig()
	tenant, admin := uuid.New(), uuid.New()
	inst := rig.addInstance(tenant, domain.InstanceStatusRunning)
	tasks := w22aSeedOpenTasks(rig, tenant, inst, 3)
	moved := tasks[1]
	listedVersion := moved.RecordVersion
	rig.afterRead["Tasks.ListByInstance"] = func() { rig.tasks[moved.ID].RecordVersion++ }
	in := port.CancelInstanceInput{InstanceID: inst.ID.String(), TenantID: tenant.String(), AdminUserID: admin.String()}
	deps := w22aNewDeps(t, rig)

	err := deps.CancelInstance(context.Background(), in)

	w22aRequireRetryable(t, err)
	assert.ErrorIs(t, err, domain.ErrRecordVersionConflict)
	assert.Equal(t, 1, rig.rolledBack)
	for _, task := range tasks {
		assert.Equal(t, domain.TaskStatusReady, task.Status, "the task that was failed before the conflict is restored")
	}
	assert.Equal(t, listedVersion+1, moved.RecordVersion, "the other writer's change stays")
	assert.Equal(t, domain.InstanceStatusRunning, inst.Status)
	assert.Empty(t, rig.outbox)

	require.NoError(t, deps.CancelInstance(context.Background(), in))

	for _, task := range tasks {
		assert.Equal(t, domain.TaskStatusFailed, task.Status)
	}
	var movedWrites []int64
	for _, c := range rig.callsTo("Tasks.UpdateStatus") {
		if c.ID == moved.ID {
			movedWrites = append(movedWrites, c.Version)
		}
	}
	assert.Equal(t, []int64{listedVersion, listedVersion + 1}, movedWrites, "the retry wrote the moved task at the version it listed again")
	assert.Equal(t, domain.InstanceStatusTerminated, inst.Status)
	assert.Len(t, rig.eventsOfType(domain.EventWorkflowTaskFailed), len(tasks), "one failed event per task, none repeated")
	assert.Len(t, rig.eventsOfType(domain.EventWorkflowInstanceCancelled), 1)

	writes, events := len(rig.callsTo("Tasks.UpdateStatus")), len(rig.outbox)
	require.NoError(t, deps.CancelInstance(context.Background(), in), "a further retry finds the instance cancelled")
	assert.Len(t, rig.callsTo("Tasks.UpdateStatus"), writes)
	assert.Len(t, rig.outbox, events)
}

// Serial: the degraded counter is process-wide.
func TestUpdateInstanceStatus_Degraded_EventCarriesFailedBranchesAndCountsOnlyACommittedEntry(t *testing.T) {
	obstest.Init(t)
	branches := []domain.FailedBranch{
		{DepartmentID: uuid.New(), LastNodeKey: "legal/review"},
		{DepartmentID: uuid.New(), LastNodeKey: "finance/approve"},
	}
	cases := []struct {
		name  string
		fault func(rig *w22aRig)
		heal  func(rig *w22aRig)
	}{
		{
			name:  "commit refused",
			fault: func(rig *w22aRig) { rig.commitErr = errW22aCommit },
			heal:  func(rig *w22aRig) { rig.commitErr = nil },
		},
		{
			name:  "enqueue fails",
			fault: func(rig *w22aRig) { rig.failOn("Outbox.Enqueue", errW22aStorage, 0) },
			heal:  func(rig *w22aRig) { delete(rig.failures, "Outbox.Enqueue") },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w22aNewRig()
			tenant := uuid.New()
			inst := rig.addInstance(tenant, domain.InstanceStatusRunning)
			startVersion := inst.RecordVersion
			in := w22aStatusInput(tenant, inst, domain.InstanceStatusDegraded)
			in.FailedBranches = branches
			deps := w22aNewDeps(t, rig)
			before := obstest.Sum(t, w22aDegradedCounter, nil)
			tc.fault(rig)

			err := deps.UpdateInstanceStatus(context.Background(), in)

			w22aRequireRetryable(t, err)
			assert.Equal(t, before, obstest.Sum(t, w22aDegradedCounter, nil), "an entry that did not commit is not counted")
			assert.Equal(t, 1, rig.rolledBack)
			assert.Equal(t, domain.InstanceStatusRunning, inst.Status)
			assert.Equal(t, startVersion, inst.RecordVersion)
			assert.Empty(t, rig.outbox)

			tc.heal(rig)
			require.NoError(t, deps.UpdateInstanceStatus(context.Background(), in))

			assert.Equal(t, before+1, obstest.Sum(t, w22aDegradedCounter, nil))
			assert.Equal(t, domain.InstanceStatusDegraded, inst.Status)
			degraded := rig.eventsOfType(domain.EventWorkflowInstanceDegraded)
			require.Len(t, degraded, 1)
			payload := w22aDecode[domain.WorkflowInstanceDegradedPayload](t, degraded[0])
			assert.Equal(t, branches, payload.FailedBranches)
			assert.Equal(t, inst.StartedByUserID, payload.StartedByUserID)
			rig.requireEveryCallScopedTo(t, tenant)
		})
	}
}
