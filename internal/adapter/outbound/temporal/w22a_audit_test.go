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

const (
	w22aBreachCounter = "workflow_execution_service_sla_breaches_total"
	w22aReviewNode    = "sales/review"
	w22aApproveNode   = "sales/approve"
)

func w22aForceRouteInput(tenant uuid.UUID, inst *domain.Instance, admin uuid.UUID, oldKeys ...domain.NodeKey) port.RecordForceRouteInput {
	return port.RecordForceRouteInput{
		TenantID: tenant.String(), InstanceID: inst.ID.String(), AdminUserID: admin.String(),
		OldNodeKeys: oldKeys, TargetNodeID: w22aApproveNode,
	}
}

func TestRecordForceRoute_Direction_DefaultsToForwardAndOtherwisePassesThrough(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		direction string
		want      string
	}{
		{name: "empty direction", direction: "", want: "forward"},
		{name: "forward", direction: "forward", want: "forward"},
		{name: "back", direction: "back", want: "back"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rig := w22aNewRig()
			tenant, admin := uuid.New(), uuid.New()
			inst := rig.addInstance(tenant, domain.InstanceStatusRunning)
			rig.addTask(tenant, inst, w22aReviewNode, domain.TaskStatusReady)
			in := w22aForceRouteInput(tenant, inst, admin, w22aReviewNode)
			in.Direction = tc.direction

			require.NoError(t, w22aNewDeps(t, rig).RecordForceRoute(context.Background(), in))

			routed := rig.eventsOfType(domain.EventWorkflowInstanceForceRouted)
			require.Len(t, routed, 1)
			payload := w22aDecode[domain.WorkflowInstanceForceRoutedPayload](t, routed[0])
			assert.Equal(t, tc.want, payload.Direction)
			assert.Equal(t, admin, payload.ActorUserID)
			assert.Equal(t, []string{w22aReviewNode}, payload.FromNodeKeys)
			assert.Equal(t, w22aApproveNode, payload.ToNodeKey)
			assert.Equal(t, inst.ID, payload.WorkflowInstanceID)
			assert.Equal(t, inst.SubjectID, payload.SubjectID)
			assert.Equal(t, inst.BusinessKey, payload.BusinessKey)
			assert.Equal(t, inst.WorkflowVersionID, payload.WorkflowVersionID)
			rig.requireEveryCallScopedTo(t, tenant)
		})
	}
}

// Routing away from no node supersedes nothing, yet the route is still
// announced, with an empty list of from-keys rather than a missing one.
func TestRecordForceRoute_NoOldNodeKeys_SupersedesNothingAndAnnouncesEmptyFromKeys(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		keys []domain.NodeKey
	}{
		{name: "nil keys", keys: nil},
		{name: "empty keys", keys: []domain.NodeKey{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rig := w22aNewRig()
			tenant, admin := uuid.New(), uuid.New()
			inst := rig.addInstance(tenant, domain.InstanceStatusRunning)
			open := rig.addTask(tenant, inst, w22aReviewNode, domain.TaskStatusReady)
			rig.addAssignment(tenant, open, uuid.New(), true, true)

			require.NoError(t, w22aNewDeps(t, rig).RecordForceRoute(context.Background(), w22aForceRouteInput(tenant, inst, admin, tc.keys...)))

			assert.Equal(t, domain.TaskStatusReady, open.Status, "no task is bypassed, so none is superseded")
			assert.Empty(t, rig.callsTo("Tasks.UpdateStatus"))
			assert.Empty(t, rig.callsTo("Assignments.Vacate"))
			assert.Equal(t, []string{domain.EventWorkflowInstanceForceRouted}, rig.eventTypes())
			fields := w22aRawFields(t, rig.outbox[0])
			assert.JSONEq(t, `[]`, string(fields["from_node_keys"]))
			assert.JSONEq(t, `"forward"`, string(fields["direction"]))
		})
	}
}

// The instance is read first, so a route for an instance that is not there ends
// the activity for good and changes nothing.
func TestRecordForceRoute_UnknownInstance_NonRetryableNotFoundBeforeAnyTaskIsTouched(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name               string
		otherTenantHoldsIt bool
	}{
		{name: "no such instance", otherTenantHoldsIt: false},
		{name: "instance of another tenant", otherTenantHoldsIt: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rig := w22aNewRig()
			caller, other, admin := uuid.New(), uuid.New(), uuid.New()
			inst := &domain.Instance{ID: uuid.New()}
			taskTenant := caller
			if tc.otherTenantHoldsIt {
				inst = rig.addInstance(other, domain.InstanceStatusRunning)
				taskTenant = other
			}
			bypassed := rig.addTask(taskTenant, inst, w22aReviewNode, domain.TaskStatusReady)

			err := w22aNewDeps(t, rig).RecordForceRoute(context.Background(), w22aForceRouteInput(caller, inst, admin, w22aReviewNode))

			w22aRequireNonRetryable(t, err, "NotFoundError")
			assert.ErrorIs(t, err, domain.ErrNotFound)
			assert.Equal(t, []string{"Instances.GetByID"}, rig.ops(), "nothing else may run before the instance is known")
			assert.Equal(t, inst.ID, rig.calls[0].ID)
			assert.Equal(t, caller, rig.calls[0].Tenant)
			assert.Equal(t, domain.TaskStatusReady, bypassed.Status)
			assert.Empty(t, rig.outbox)
		})
	}
}

// A bypassed node's tasks are superseded wherever they sit in a listing that
// runs over several pages; tasks that are settled, or on another node, are left
// alone.
func TestRecordForceRoute_TasksAcrossSeveralPages_SupersedesOnlyOpenTasksOfBypassedNodes(t *testing.T) {
	t.Parallel()
	rig := w22aNewRig()
	tenant, admin := uuid.New(), uuid.New()
	inst := rig.addInstance(tenant, domain.InstanceStatusRunning)
	wantFinal := map[uuid.UUID]domain.TaskStatus{}
	var wantSuperseded []uuid.UUID
	seed := func(count int, nodeKey string, status domain.TaskStatus, supersedes bool) {
		for range count {
			task := rig.addTask(tenant, inst, nodeKey, status)
			wantFinal[task.ID] = status
			if supersedes {
				wantFinal[task.ID] = domain.TaskStatusSuperseded
				wantSuperseded = append(wantSuperseded, task.ID)
			}
		}
	}
	seed(30, "sales/one", domain.TaskStatusReady, true)
	seed(30, "sales/one", domain.TaskStatusCompleted, false)
	seed(25, "sales/two", domain.TaskStatusInProgress, true)
	seed(15, "sales/two", domain.TaskStatusFailed, false)
	seed(10, "sales/one", domain.TaskStatusDeferred, false)
	seed(10, "sales/two", domain.TaskStatusSuperseded, false)
	seed(10, "sales/other", domain.TaskStatusReady, false)
	seed(5, "sales/one", domain.TaskStatusReady, true)
	require.Len(t, rig.taskOrder, 135)

	err := w22aNewDeps(t, rig).RecordForceRoute(context.Background(), w22aForceRouteInput(tenant, inst, admin, "sales/one", "sales/two"))

	require.NoError(t, err)
	require.Len(t, rig.pageRequests, 3, "135 tasks at 50 a page need three listings")
	for _, page := range rig.pageRequests {
		assert.Equal(t, w22aPageSize, page.Limit)
	}
	assert.Nil(t, rig.pageRequests[0].After)
	require.NotNil(t, rig.pageRequests[1].After)
	assert.Equal(t, rig.taskOrder[49], rig.pageRequests[1].After.ID, "the second page starts after the first page's last task")
	require.NotNil(t, rig.pageRequests[2].After)
	assert.Equal(t, rig.taskOrder[99], rig.pageRequests[2].After.ID)

	assert.ElementsMatch(t, wantSuperseded, rig.idsOf("Tasks.UpdateStatus"))
	for _, c := range rig.callsTo("Tasks.UpdateStatus") {
		assert.Equal(t, int64(7), c.Version, "each task is written at the version the listing showed")
	}
	for id, want := range wantFinal {
		assert.Equal(t, want, rig.tasks[id].Status)
	}
	superseded := rig.eventsOfType(domain.EventWorkflowTaskSuperseded)
	assert.Len(t, superseded, len(wantSuperseded))
	for _, env := range superseded {
		assert.Equal(t, admin, w22aDecode[domain.WorkflowTaskSupersededPayload](t, env).ActorUserID, "a force route supersedes in the admin's name")
	}
	assert.Len(t, rig.eventsOfType(domain.EventWorkflowInstanceForceRouted), 1)
	assert.Equal(t, domain.EventWorkflowInstanceForceRouted, rig.outbox[len(rig.outbox)-1].Type, "the route is announced after the supersedes")
}

func TestRecordForceRoute_BypassedTaskWithAssignees_VacatesThemAndNamesThemInTheSupersededEvent(t *testing.T) {
	t.Parallel()
	rig := w22aNewRig()
	tenant, admin := uuid.New(), uuid.New()
	inst := rig.addInstance(tenant, domain.InstanceStatusRunning)
	task := rig.addTask(tenant, inst, w22aReviewNode, domain.TaskStatusInProgress)
	lead := rig.addAssignment(tenant, task, uuid.New(), true, true)
	second := rig.addAssignment(tenant, task, uuid.New(), false, true)
	third := rig.addAssignment(tenant, task, uuid.New(), false, true)
	earlier := rig.addAssignment(tenant, task, uuid.New(), false, false)
	in := w22aForceRouteInput(tenant, inst, admin, w22aReviewNode)
	deps := w22aNewDeps(t, rig)

	require.NoError(t, deps.RecordForceRoute(context.Background(), in))

	assert.Equal(t, domain.TaskStatusSuperseded, task.Status)
	assert.ElementsMatch(t, []uuid.UUID{lead.ID, second.ID, third.ID}, rig.idsOf("Assignments.Vacate"))
	assert.NotContains(t, rig.idsOf("Assignments.Vacate"), earlier.ID, "an assignment that was already closed is not vacated again")
	for _, a := range []*domain.TaskAssignment{lead, second, third} {
		assert.False(t, a.IsActive)
	}
	superseded := rig.eventsOfType(domain.EventWorkflowTaskSuperseded)
	require.Len(t, superseded, 1)
	payload := w22aDecode[domain.WorkflowTaskSupersededPayload](t, superseded[0])
	assert.Equal(t, task.ID, payload.TaskID)
	assert.Equal(t, w22aReviewNode, payload.NodeKey)
	assert.Equal(t, task.DepartmentID, payload.DepartmentID)
	assert.Equal(t, admin, payload.ActorUserID)
	assert.ElementsMatch(t, []uuid.UUID{lead.UserID, second.UserID, third.UserID}, payload.AssigneeUserIDs)
	assert.Equal(t, inst.SubjectID, payload.SubjectID)
	assert.Equal(t, inst.BusinessKey, payload.BusinessKey)
	rig.requireEveryCallScopedTo(t, tenant)

	writesBefore, vacatesBefore, eventsBefore := rig.idsOf("Tasks.UpdateStatus"), rig.idsOf("Assignments.Vacate"), len(rig.outbox)
	require.NoError(t, deps.RecordForceRoute(context.Background(), in), "a retry of the same route must succeed")
	assert.Equal(t, writesBefore, rig.idsOf("Tasks.UpdateStatus"), "the retry must not write a task again")
	assert.Equal(t, vacatesBefore, rig.idsOf("Assignments.Vacate"), "the retry must not vacate again")
	assert.Len(t, rig.outbox, eventsBefore, "the retry must not announce again")
}

// The event's common core is read inside the supersede; when that read fails
// the whole attempt rolls back so the retry starts from the untouched task.
func TestRecordForceRoute_InstanceLookupFailsWhileSuperseding_RollsBackEveryWrite(t *testing.T) {
	t.Parallel()
	rig := w22aNewRig()
	tenant, admin := uuid.New(), uuid.New()
	inst := rig.addInstance(tenant, domain.InstanceStatusRunning)
	task := rig.addTask(tenant, inst, w22aReviewNode, domain.TaskStatusReady)
	assignee := rig.addAssignment(tenant, task, uuid.New(), true, true)
	startVersion := task.RecordVersion
	rig.failOn("Instances.GetByID", errW22aStorage, 2)

	err := w22aNewDeps(t, rig).RecordForceRoute(context.Background(), w22aForceRouteInput(tenant, inst, admin, w22aReviewNode))

	w22aRequireRetryable(t, err)
	assert.ErrorIs(t, err, errW22aStorage)
	assert.Equal(t, []uuid.UUID{task.ID}, rig.idsOf("Tasks.UpdateStatus"), "the supersede write was attempted before the lookup failed")
	assert.Equal(t, domain.TaskStatusReady, task.Status, "the rollback restores the task")
	assert.Equal(t, startVersion, task.RecordVersion)
	assert.True(t, assignee.IsActive, "the rollback restores the assignment")
	assert.Empty(t, rig.outbox)
	assert.Equal(t, 1, rig.rolledBack)
}

func w22aSeedSLATask(rig *w22aRig, tenant uuid.UUID, due, followUp *time.Time) (*domain.Instance, *domain.Task, []*domain.TaskAssignment) {
	inst := rig.addInstance(tenant, domain.InstanceStatusRunning)
	task := rig.addTask(tenant, inst, w22aReviewNode, domain.TaskStatusInProgress)
	task.DueAt, task.FollowUpAt = due, followUp
	active := []*domain.TaskAssignment{
		rig.addAssignment(tenant, task, uuid.New(), true, true),
		rig.addAssignment(tenant, task, uuid.New(), false, true),
	}
	rig.addAssignment(tenant, task, uuid.New(), false, false)
	return inst, task, active
}

func w22aUsersOf(assignments []*domain.TaskAssignment) []uuid.UUID {
	out := make([]uuid.UUID, len(assignments))
	for i, a := range assignments {
		out[i] = a.UserID
	}
	return out
}

// Metric tests read before and after counts of a process-wide counter, so they
// run one at a time.
func TestRecordSLABreach_FirstCall_EnqueuesOneEventWithDueAtAndCountsOnce(t *testing.T) {
	obstest.Init(t)
	rig := w22aNewRig()
	tenant := uuid.New()
	due := time.Date(2026, 4, 2, 10, 30, 0, 0, time.UTC)
	followUp := time.Date(2026, 4, 1, 10, 30, 0, 0, time.UTC)
	inst, task, active := w22aSeedSLATask(rig, tenant, &due, &followUp)
	in := port.RecordSLABreachInput{TenantID: tenant.String(), InstanceID: inst.ID.String(), TaskID: task.ID.String(), NodeKey: w22aReviewNode}
	deps := w22aNewDeps(t, rig)
	before := obstest.Sum(t, w22aBreachCounter, nil)

	require.NoError(t, deps.RecordSLABreach(context.Background(), in))

	require.Len(t, rig.outbox, 1)
	assert.Equal(t, domain.EventWorkflowTaskSLABreached, rig.outbox[0].Type)
	payload := w22aDecode[domain.WorkflowTaskSLABreachedPayload](t, rig.outbox[0])
	assert.True(t, due.Equal(payload.DueAt), "due_at is the task's due time, got %s", payload.DueAt)
	assert.Equal(t, task.ID, payload.TaskID)
	assert.ElementsMatch(t, w22aUsersOf(active), payload.AssigneeUserIDs, "only the active assignees are named")
	assert.Equal(t, domain.TaskStatusInProgress, task.Status, "an audit event changes no status")
	assert.Equal(t, before+1, obstest.Sum(t, w22aBreachCounter, nil))
	assert.Equal(t, []uuid.UUID{task.ID}, rig.idsOf("Outbox.ExistsForTask"), "the duplicate check runs for this task")
	rig.requireEveryCallScopedTo(t, tenant)

	require.NoError(t, deps.RecordSLABreach(context.Background(), in), "a retry of the same breach must succeed")
	assert.Len(t, rig.outbox, 1, "the retry must not record the breach again")
	assert.Equal(t, before+1, obstest.Sum(t, w22aBreachCounter, nil), "the retry must not count the breach again")
}

func TestRecordSLAWarning_FirstCall_EnqueuesOneEventWithFollowUpAtAndDoesNotCountABreach(t *testing.T) {
	obstest.Init(t)
	rig := w22aNewRig()
	tenant := uuid.New()
	due := time.Date(2026, 4, 2, 10, 30, 0, 0, time.UTC)
	followUp := time.Date(2026, 4, 1, 8, 15, 0, 0, time.UTC)
	inst, task, active := w22aSeedSLATask(rig, tenant, &due, &followUp)
	startVersion := task.RecordVersion
	in := port.RecordSLAWarningInput{TenantID: tenant.String(), InstanceID: inst.ID.String(), TaskID: task.ID.String(), NodeKey: w22aReviewNode}
	before := obstest.Sum(t, w22aBreachCounter, nil)

	require.NoError(t, w22aNewDeps(t, rig).RecordSLAWarning(context.Background(), in))

	require.Len(t, rig.outbox, 1)
	assert.Equal(t, domain.EventWorkflowTaskSLAWarning, rig.outbox[0].Type)
	payload := w22aDecode[domain.WorkflowTaskSLAWarningPayload](t, rig.outbox[0])
	assert.True(t, followUp.Equal(payload.FollowUpAt), "follow_up_at is the task's follow-up time, got %s", payload.FollowUpAt)
	assert.ElementsMatch(t, w22aUsersOf(active), payload.AssigneeUserIDs)
	assert.Equal(t, domain.TaskStatusInProgress, task.Status)
	assert.Equal(t, startVersion, task.RecordVersion)
	assert.Empty(t, rig.callsTo("Tasks.UpdateStatus"))
	assert.Equal(t, before, obstest.Sum(t, w22aBreachCounter, nil), "a warning is not a breach")
	rig.requireEveryCallScopedTo(t, tenant)
}

// The counter moves only after the transaction commits: a refused commit or a
// failed enqueue leaves it where it was, and the retry counts once.
func TestRecordSLABreach_TransactionFails_DoesNotCountAndTheRetryCountsOnce(t *testing.T) {
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
			obstest.Init(t)
			rig := w22aNewRig()
			tenant := uuid.New()
			due := time.Date(2026, 4, 2, 10, 30, 0, 0, time.UTC)
			inst, task, _ := w22aSeedSLATask(rig, tenant, &due, nil)
			in := port.RecordSLABreachInput{TenantID: tenant.String(), InstanceID: inst.ID.String(), TaskID: task.ID.String(), NodeKey: w22aReviewNode}
			deps := w22aNewDeps(t, rig)
			before := obstest.Sum(t, w22aBreachCounter, nil)
			tc.fault(rig)

			err := deps.RecordSLABreach(context.Background(), in)

			w22aRequireRetryable(t, err)
			assert.Empty(t, rig.outbox, "the failed attempt leaves no event behind")
			assert.Equal(t, 1, rig.rolledBack)
			assert.Equal(t, before, obstest.Sum(t, w22aBreachCounter, nil), "a breach that did not commit is not counted")

			tc.heal(rig)
			require.NoError(t, deps.RecordSLABreach(context.Background(), in))
			assert.Len(t, rig.eventsOfType(domain.EventWorkflowTaskSLABreached), 1)
			assert.Equal(t, before+1, obstest.Sum(t, w22aBreachCounter, nil))
		})
	}
}

func TestRecordSLAEvents_UnknownTask_NonRetryableNotFoundAndNothingRecorded(t *testing.T) {
	cases := []struct {
		name string
		run  func(deps *outboundtemporal.Deps, tenant, instance, task string) error
	}{
		{name: "warning", run: func(deps *outboundtemporal.Deps, tenant, instance, task string) error {
			return deps.RecordSLAWarning(context.Background(), port.RecordSLAWarningInput{TenantID: tenant, InstanceID: instance, TaskID: task})
		}},
		{name: "breach", run: func(deps *outboundtemporal.Deps, tenant, instance, task string) error {
			return deps.RecordSLABreach(context.Background(), port.RecordSLABreachInput{TenantID: tenant, InstanceID: instance, TaskID: task})
		}},
	}
	for _, tc := range cases {
		for _, ownedBy := range []string{"nobody", "another tenant"} {
			t.Run(tc.name+"/"+ownedBy, func(t *testing.T) {
				obstest.Init(t)
				rig := w22aNewRig()
				caller, other := uuid.New(), uuid.New()
				inst := &domain.Instance{ID: uuid.New()}
				task := &domain.Task{ID: uuid.New()}
				if ownedBy == "another tenant" {
					inst = rig.addInstance(other, domain.InstanceStatusRunning)
					task = rig.addTask(other, inst, w22aReviewNode, domain.TaskStatusReady)
				}
				before := obstest.Sum(t, w22aBreachCounter, nil)

				err := tc.run(w22aNewDeps(t, rig), caller.String(), inst.ID.String(), task.ID.String())

				w22aRequireNonRetryable(t, err, "NotFoundError")
				assert.ErrorIs(t, err, domain.ErrNotFound)
				assert.Equal(t, []string{"Tasks.GetByID"}, rig.ops())
				assert.Equal(t, task.ID, rig.calls[0].ID)
				assert.Equal(t, caller, rig.calls[0].Tenant)
				assert.Empty(t, rig.outbox)
				assert.Equal(t, before, obstest.Sum(t, w22aBreachCounter, nil))
			})
		}
	}
}
