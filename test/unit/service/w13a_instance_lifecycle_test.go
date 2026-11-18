package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

var errW13aOutboxDown = errors.New("outbox read unavailable")

type w13aInstanceCall struct{ tenantID, id uuid.UUID }

type w13aStatusUpdate struct {
	tenantID, id uuid.UUID
	status       domain.InstanceStatus
	version      int64
}

// w13aLifecycleInstances hides an instance from every other tenant and records each read and status write.
type w13aLifecycleInstances struct {
	*fakeInstanceRepo
	reads   []w13aInstanceCall
	updates []w13aStatusUpdate
}

func (r *w13aLifecycleInstances) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	r.reads = append(r.reads, w13aInstanceCall{tenantID, id})
	if inst, ok := r.byID[id]; ok && inst.TenantID != tenantID {
		return nil, domain.ErrNotFound
	}
	return r.fakeInstanceRepo.GetByID(ctx, tenantID, id)
}

func (r *w13aLifecycleInstances) UpdateStatus(ctx context.Context, tenantID, id uuid.UUID, status domain.InstanceStatus, version int64) (*domain.Instance, error) {
	r.updates = append(r.updates, w13aStatusUpdate{tenantID, id, status, version})
	if inst, ok := r.byID[id]; ok && inst.TenantID != tenantID {
		return nil, domain.ErrNotFound
	}
	return r.fakeInstanceRepo.UpdateStatus(ctx, tenantID, id, status, version)
}

type w13aTaskListing struct {
	tenantID, instanceID uuid.UUID
}

// w13aListedTasks records each listing of an instance's tasks.
type w13aListedTasks struct {
	*fakeTaskRepo
	listings []w13aTaskListing
}

func (r *w13aListedTasks) ListByInstance(ctx context.Context, tenantID, instanceID uuid.UUID, page port.PageRequest) ([]*domain.Task, *port.Cursor, error) {
	r.listings = append(r.listings, w13aTaskListing{tenantID, instanceID})
	return r.fakeTaskRepo.ListByInstance(ctx, tenantID, instanceID, page)
}

type w13aLifecycleRig struct {
	svc         *service.InstanceService
	instances   *w13aLifecycleInstances
	tasks       *w13aListedTasks
	assignments *fakeAssignmentRepo
	outbox      *fakeOutbox
	temporal    *fakeTemporalClient
	definitions *w13aDefinitions
	tenantID    uuid.UUID
}

func w13aNewLifecycleRig(t *testing.T, planJSON string) *w13aLifecycleRig {
	t.Helper()
	svc, instances, tasks, assignments, outbox, temporal, _, _ := newInstanceServiceHarness()
	r := &w13aLifecycleRig{
		svc:         svc,
		instances:   &w13aLifecycleInstances{fakeInstanceRepo: instances},
		tasks:       &w13aListedTasks{fakeTaskRepo: tasks},
		assignments: assignments,
		outbox:      outbox,
		temporal:    temporal,
		definitions: &w13aDefinitions{resp: publishedCompiledWorkflow(uuid.New(), uuid.New(), planJSON)},
		tenantID:    uuid.New(),
	}
	svc.Instances = r.instances
	svc.Tasks = r.tasks
	svc.Definitions = r.definitions
	return r
}

func (r *w13aLifecycleRig) seed(status domain.InstanceStatus, version int64) *domain.Instance {
	inst := &domain.Instance{
		ID: uuid.New(), TenantID: r.tenantID, Status: status, RecordVersion: version,
		TemporalWorkflowID: "tenant:" + uuid.NewString(), TemporalRunID: "run-" + uuid.NewString(),
	}
	r.instances.byID[inst.ID] = inst
	return inst
}

func TestInstanceService_Terminate_OnlyAFinishedInstanceIsRefusedAndBeforeAnyTemporalCall(t *testing.T) {
	cases := []struct {
		name     string
		status   domain.InstanceStatus
		accepted bool
	}{
		{"a RUNNING instance is terminated", domain.InstanceStatusRunning, true},
		{"a PAUSED instance is terminated", domain.InstanceStatusPaused, true},
		{"a DEGRADED instance is terminated", domain.InstanceStatusDegraded, true},
		{"a COMPLETED instance is refused", domain.InstanceStatusCompleted, false},
		{"a FAILED instance is refused", domain.InstanceStatusFailed, false},
		{"a TERMINATED instance is refused", domain.InstanceStatusTerminated, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w13aNewLifecycleRig(t, compiledPlanJSON(t, assignedStage("review")))
			inst := rig.seed(tc.status, 4)
			task := &domain.Task{ID: uuid.New(), TenantID: rig.tenantID, WorkflowInstanceID: inst.ID, Status: domain.TaskStatusReady, RecordVersion: 1}
			rig.tasks.byID[task.ID] = task
			assignment := &domain.TaskAssignment{ID: uuid.New(), TenantID: rig.tenantID, TaskID: task.ID, UserID: uuid.New(), IsActive: true}
			rig.assignments.byID[assignment.ID] = assignment
			actor := uuid.New()

			err := rig.svc.Terminate(context.Background(), rig.tenantID, inst.ID, actor, "no longer needed")

			for _, read := range rig.instances.reads {
				assert.Equal(t, w13aInstanceCall{rig.tenantID, inst.ID}, read)
			}
			if !tc.accepted {
				require.ErrorIs(t, err, port.ErrInstanceAlreadyTerminal)
				assert.Equal(t, tc.status, inst.Status)
				assert.Empty(t, rig.temporal.terminate, "no Temporal call is made for a finished instance")
				assert.Empty(t, rig.instances.updates)
				assert.Empty(t, rig.outbox.enqueued)
				assert.Empty(t, rig.tasks.listings, "no task is read for a finished instance")
				assert.Equal(t, domain.TaskStatusReady, task.Status)
				assert.True(t, assignment.IsActive)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, []w13aStatusUpdate{{rig.tenantID, inst.ID, domain.InstanceStatusTerminated, 4}}, rig.instances.updates)
			require.NotEmpty(t, rig.temporal.terminate)
			assert.Equal(t, terminateCall{TemporalWorkflowID: inst.TemporalWorkflowID, RunID: inst.TemporalRunID, Reason: "no longer needed"}, rig.temporal.terminate[0])
			assert.Equal(t, domain.TaskStatusFailed, task.Status, "the open task is failed with the instance")
			assert.False(t, assignment.IsActive)
			assert.Equal(t, []w13aTaskListing{{rig.tenantID, inst.ID}}, rig.tasks.listings, "the instance's tasks are listed once, in its tenant")
			assert.NotEmpty(t, rig.outbox.enqueued)
		})
	}
}

type w13aLifecycleCall struct {
	name    string
	signal  string
	live    domain.InstanceStatus
	refused domain.InstanceStatus
	call    func(svc *service.InstanceService, tenantID, instanceID uuid.UUID, version int64) error
}

func w13aLifecycleCalls() []w13aLifecycleCall {
	actor := uuid.New()
	ctx := context.Background()
	return []w13aLifecycleCall{
		{"pause", port.SignalInstancePause, domain.InstanceStatusRunning, domain.InstanceStatusPaused,
			func(s *service.InstanceService, tn, id uuid.UUID, v int64) error {
				return s.Pause(ctx, tn, id, actor, "why", v)
			}},
		{"resume", port.SignalInstanceResume, domain.InstanceStatusPaused, domain.InstanceStatusRunning,
			func(s *service.InstanceService, tn, id uuid.UUID, v int64) error {
				return s.Resume(ctx, tn, id, actor, v)
			}},
		{"cancel", port.SignalInstanceCancel, domain.InstanceStatusRunning, "",
			func(s *service.InstanceService, tn, id uuid.UUID, v int64) error {
				return s.Cancel(ctx, tn, id, actor, "why", v)
			}},
		{"force-forward", port.SignalInstanceForceFwd, domain.InstanceStatusDegraded, domain.InstanceStatusPaused,
			func(s *service.InstanceService, tn, id uuid.UUID, v int64) error {
				return s.ForceForward(ctx, tn, id, actor, "finance/review", v)
			}},
		{"force-back", port.SignalInstanceForceBack, domain.InstanceStatusDegraded, domain.InstanceStatusPaused,
			func(s *service.InstanceService, tn, id uuid.UUID, v int64) error {
				return s.ForceBack(ctx, tn, id, actor, "finance/review", v)
			}},
	}
}

func TestInstanceService_LifecycleSignals_ChecksRunInOrderAndTheFirstRefusalWins(t *testing.T) {
	type scenario struct {
		name    string
		status  func(c w13aLifecycleCall) domain.InstanceStatus
		missing bool
		stale   bool
		want    error
		unless  error
		skip    func(c w13aLifecycleCall) bool
	}
	never := func(w13aLifecycleCall) bool { return false }
	needsRefusedState := func(c w13aLifecycleCall) bool { return c.refused == "" }
	scenarios := []scenario{
		{name: "an unknown instance with a stale version is not found", missing: true, stale: true, want: port.ErrInstanceNotFound,
			unless: port.ErrRecordVersionConflict, skip: never},
		{name: "a COMPLETED instance with a stale version is already terminal", stale: true,
			status: func(w13aLifecycleCall) domain.InstanceStatus { return domain.InstanceStatusCompleted },
			want:   port.ErrInstanceAlreadyTerminal, unless: port.ErrRecordVersionConflict, skip: never},
		{name: "a TERMINATED instance with a stale version is already terminal", stale: true,
			status: func(w13aLifecycleCall) domain.InstanceStatus { return domain.InstanceStatusTerminated },
			want:   port.ErrInstanceAlreadyTerminal, unless: port.ErrRecordVersionConflict, skip: never},
		{name: "a FAILED instance with a stale version is already terminal", stale: true,
			status: func(w13aLifecycleCall) domain.InstanceStatus { return domain.InstanceStatusFailed },
			want:   port.ErrInstanceAlreadyTerminal, unless: port.ErrRecordVersionConflict, skip: never},
		{name: "a live instance in a refused state with a stale version is a version conflict", stale: true,
			status: func(c w13aLifecycleCall) domain.InstanceStatus { return c.refused },
			want:   port.ErrRecordVersionConflict, unless: port.ErrInvalidInstanceState, skip: needsRefusedState},
		{name: "a live instance in a refused state with the current version is an invalid state",
			status: func(c w13aLifecycleCall) domain.InstanceStatus { return c.refused },
			want:   port.ErrInvalidInstanceState, unless: port.ErrRecordVersionConflict, skip: needsRefusedState},
		{name: "a live instance in a permitted state with a stale version is a version conflict", stale: true,
			status: func(c w13aLifecycleCall) domain.InstanceStatus { return c.live },
			want:   port.ErrRecordVersionConflict, unless: port.ErrInvalidInstanceState, skip: never},
	}
	for _, call := range w13aLifecycleCalls() {
		for _, sc := range scenarios {
			t.Run(call.name+": "+sc.name, func(t *testing.T) {
				if sc.skip(call) {
					t.Skip("every live state is permitted for this call")
				}
				rig := w13aNewLifecycleRig(t, compiledPlanJSON(t, assignedStage("review")))
				id := uuid.New()
				version := int64(7)
				if sc.status != nil {
					id = rig.seed(sc.status(call), 7).ID
				}
				if sc.stale {
					version = 3
				}

				err := call.call(rig.svc, rig.tenantID, id, version)

				require.ErrorIs(t, err, sc.want)
				assert.NotErrorIs(t, err, sc.unless)
				assert.Empty(t, rig.temporal.signals, "no signal is sent on a refusal")
				assert.Equal(t, []w13aInstanceCall{{rig.tenantID, id}}, rig.instances.reads)
			})
		}
		t.Run(call.name+": control: a permitted state with the current version sends its one signal", func(t *testing.T) {
			rig := w13aNewLifecycleRig(t, compiledPlanJSON(t, assignedStage("review")))
			inst := rig.seed(call.live, 7)

			require.NoError(t, call.call(rig.svc, rig.tenantID, inst.ID, 7))

			require.Len(t, rig.temporal.signals, 1)
			assert.Equal(t, call.signal, rig.temporal.signals[0].SignalName)
			assert.Equal(t, inst.ID, rig.temporal.signals[0].InstanceID)
			assert.Equal(t, inst.TemporalWorkflowID, rig.temporal.signals[0].TemporalWorkflowID)
		})
	}
}

func TestInstanceService_ForceRoutes_VersionAndStateAreCheckedBeforeTheTargetWork(t *testing.T) {
	cases := []struct {
		name    string
		route   string
		status  domain.InstanceStatus
		version int64
		want    error
		reads   bool
	}{
		{"force-forward to an unknown node with a stale version", "forward", domain.InstanceStatusRunning, 3, port.ErrRecordVersionConflict, false},
		{"force-forward to an unknown node from a refused state", "forward", domain.InstanceStatusPaused, 7, port.ErrInvalidInstanceState, false},
		{"control: force-forward to an unknown node, state and version fine", "forward", domain.InstanceStatusRunning, 7, port.ErrTargetNodeNotFound, true},
		{"force-back of a RUNNING instance with no completed step and a stale version", "back", domain.InstanceStatusRunning, 3, port.ErrRecordVersionConflict, false},
		{"force-back of a PAUSED instance with no completed step", "back", domain.InstanceStatusPaused, 7, port.ErrInvalidInstanceState, false},
		{"control: force-back of a RUNNING instance with no completed step, state and version fine", "back", domain.InstanceStatusRunning, 7, port.ErrForceBackNoSavedBranch, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w13aNewLifecycleRig(t, compiledPlanJSON(t, assignedStage("review")))
			inst := rig.seed(tc.status, 7)
			actor := uuid.New()

			var err error
			if tc.route == "forward" {
				err = rig.svc.ForceForward(context.Background(), rig.tenantID, inst.ID, actor, "finance/nowhere", tc.version)
			} else {
				err = rig.svc.ForceBack(context.Background(), rig.tenantID, inst.ID, actor, "", tc.version)
			}

			require.ErrorIs(t, err, tc.want)
			assert.Empty(t, rig.temporal.signals)
			if tc.route == "forward" {
				if tc.reads {
					assert.Equal(t, []w13aDefinitionCall{{rig.tenantID, inst.WorkflowVersionID}}, rig.definitions.calls)
				} else {
					assert.Empty(t, rig.definitions.calls, "the plan is not read before the version and state checks")
				}
				return
			}
			if tc.reads {
				assert.Equal(t, []w13aTaskListing{{rig.tenantID, inst.ID}}, rig.tasks.listings)
			} else {
				assert.Empty(t, rig.tasks.listings, "the tasks are not listed before the version and state checks")
			}
		})
	}
}

func TestInstanceService_ForceForward_ATargetThatIsNotAStageOfTheMainPlanIsRefused(t *testing.T) {
	mainPlan := compiledPlanJSON(t, assignedStage("settlement"))
	cases := []struct {
		name     string
		planJSON string
		target   string
		accepted bool
	}{
		{"an empty key", mainPlan, "", false},
		{"a blank key", mainPlan, " ", false},
		{"a department with no stage", mainPlan, "finance", false},
		{"a department and an empty stage", mainPlan, "finance/", false},
		{"a stage with no department", mainPlan, "/settlement", false},
		{"a stage of another department", mainPlan, "legal/settlement", false},
		{"an unknown stage", mainPlan, "finance/typo", false},
		{"control: a stage of the main plan", mainPlan, "finance/settlement", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w13aNewLifecycleRig(t, tc.planJSON)
			inst := rig.seed(domain.InstanceStatusRunning, 1)
			actor := uuid.New()

			err := rig.svc.ForceForward(context.Background(), rig.tenantID, inst.ID, actor, tc.target, 1)

			assert.Equal(t, []w13aDefinitionCall{{rig.tenantID, inst.WorkflowVersionID}}, rig.definitions.calls, "the plan is read to judge the target")
			if !tc.accepted {
				require.ErrorIs(t, err, port.ErrTargetNodeNotFound)
				assert.Empty(t, rig.temporal.signals)
				return
			}
			require.NoError(t, err)
			require.Len(t, rig.temporal.signals, 1)
			raw, err := json.Marshal(rig.temporal.signals[0].Payload)
			require.NoError(t, err)
			var wire struct {
				AdminUserID   string
				TargetNodeKey string
			}
			require.NoError(t, json.Unmarshal(raw, &wire))
			assert.Equal(t, tc.target, wire.TargetNodeKey)
			assert.Equal(t, actor.String(), wire.AdminUserID)
		})
	}
}

type w13aEventListing struct {
	tenantID, instanceID uuid.UUID
	page                 port.PageRequest
}

// w13aEvents answers a fixed page of outbox rows with a next cursor, and records what it was asked.
type w13aEvents struct {
	*fakeOutbox
	rows     []*domain.OutboxEventRecord
	next     *port.Cursor
	err      error
	listings []w13aEventListing
}

func (o *w13aEvents) ListByInstance(_ context.Context, tenantID, instanceID uuid.UUID, page port.PageRequest) ([]*domain.OutboxEventRecord, *port.Cursor, error) {
	o.listings = append(o.listings, w13aEventListing{tenantID, instanceID, page})
	if o.err != nil {
		return nil, nil, o.err
	}
	return o.rows, o.next, nil
}

func TestInstanceService_ListEvents_RowsThatCannotBeRenderedAreSkippedWithAWarningAndTheCursorStays(t *testing.T) {
	tenantID := uuid.New()
	valid := func() *domain.OutboxEventRecord {
		raw, err := json.Marshal(map[string]any{"tenant_id": tenantID.String(), "data": map[string]any{}})
		require.NoError(t, err)
		return &domain.OutboxEventRecord{ID: uuid.New(), EventType: domain.EventWorkflowInstanceStarted, Payload: raw}
	}
	unknownType := func() *domain.OutboxEventRecord {
		return &domain.OutboxEventRecord{ID: uuid.New(), EventType: "workflow.something.unknown", Payload: []byte(`{}`)}
	}
	brokenEnvelope := func() *domain.OutboxEventRecord {
		return &domain.OutboxEventRecord{ID: uuid.New(), EventType: domain.EventWorkflowInstanceStarted, Payload: []byte(`{`)}
	}
	position := &port.Cursor{CreatedAt: time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC), ID: uuid.New()}
	cases := []struct {
		name     string
		rows     []*domain.OutboxEventRecord
		next     *port.Cursor
		wantKept []int
	}{
		{"an unknown event type is skipped among rendered rows", []*domain.OutboxEventRecord{valid(), unknownType(), valid()}, position, []int{0, 2}},
		{"a row with a broken envelope is skipped", []*domain.OutboxEventRecord{brokenEnvelope(), valid()}, position, []int{1}},
		{"a page where every row is unrenderable is empty but still has the next cursor", []*domain.OutboxEventRecord{unknownType(), brokenEnvelope()}, position, nil},
		{"control: a page of rendered rows with no next cursor has none", []*domain.OutboxEventRecord{valid(), valid()}, nil, []int{0, 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w13aNewLifecycleRig(t, compiledPlanJSON(t, assignedStage("review")))
			rig.tenantID = tenantID
			inst := rig.seed(domain.InstanceStatusRunning, 1)
			events := &w13aEvents{fakeOutbox: rig.outbox, rows: tc.rows, next: tc.next}
			rig.svc.Outbox = events
			log := &fakeLogger{}
			rig.svc.Log = log
			after := &port.CursorPosition{CreatedAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), ID: uuid.New()}

			res, err := rig.svc.ListEvents(context.Background(), rig.tenantID, inst.ID, port.ReadScope{IsAdmin: true}, port.Page{Cursor: after, Limit: 25})

			require.NoError(t, err)
			require.Len(t, events.listings, 1)
			assert.Equal(t, w13aEventListing{rig.tenantID, inst.ID, port.PageRequest{After: &port.Cursor{CreatedAt: after.CreatedAt, ID: after.ID}, Limit: 25}}, events.listings[0])
			require.Len(t, res.Items, len(tc.wantKept))
			for i, kept := range tc.wantKept {
				assert.Equal(t, tc.rows[kept].ID, res.Items[i].ID)
				assert.Equal(t, port.EventInstanceStarted, res.Items[i].EventType)
				assert.Equal(t, inst.ID, res.Items[i].WorkflowInstanceID)
				assert.Equal(t, rig.tenantID, res.Items[i].TenantID)
			}
			assert.NotNil(t, res.Items, "an empty page is an empty list, not none")
			skipped := len(tc.rows) - len(tc.wantKept)
			require.Len(t, log.warnCalls, skipped, "one warning for each skipped row")
			kept := map[int]bool{}
			for _, k := range tc.wantKept {
				kept[k] = true
			}
			var wantSkippedIDs []uuid.UUID
			for i, row := range tc.rows {
				if !kept[i] {
					wantSkippedIDs = append(wantSkippedIDs, row.ID)
				}
			}
			var gotSkippedIDs []uuid.UUID
			for _, fields := range log.warnFields {
				id, ok := fields["event_id"].(uuid.UUID)
				require.True(t, ok, "the warning names the skipped row")
				gotSkippedIDs = append(gotSkippedIDs, id)
			}
			assert.Equal(t, wantSkippedIDs, gotSkippedIDs)
			if tc.next == nil {
				assert.Empty(t, res.NextCursor)
				return
			}
			decoded, err := port.DecodeCursor(res.NextCursor)
			require.NoError(t, err, "the next cursor must decode")
			assert.True(t, decoded.CreatedAt.Equal(tc.next.CreatedAt))
			assert.Equal(t, tc.next.ID, decoded.ID)
		})
	}
}

func TestInstanceService_ListEvents_ARepositoryErrorIsReturnedUnwrappedWithNoPage(t *testing.T) {
	rig := w13aNewLifecycleRig(t, compiledPlanJSON(t, assignedStage("review")))
	inst := rig.seed(domain.InstanceStatusRunning, 1)
	events := &w13aEvents{fakeOutbox: rig.outbox, err: errW13aOutboxDown}
	rig.svc.Outbox = events
	log := &fakeLogger{}
	rig.svc.Log = log

	res, err := rig.svc.ListEvents(context.Background(), rig.tenantID, inst.ID, port.ReadScope{IsAdmin: true}, port.Page{Limit: 10})

	require.Error(t, err)
	assert.Same(t, errW13aOutboxDown, err)
	assert.Equal(t, port.PageResult[*port.WorkflowEvent]{}, res)
	assert.Equal(t, []w13aEventListing{{rig.tenantID, inst.ID, port.PageRequest{Limit: 10}}}, events.listings)
	assert.Empty(t, log.warnCalls)
}
