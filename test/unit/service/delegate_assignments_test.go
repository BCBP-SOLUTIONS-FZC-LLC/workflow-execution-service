package service_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

// csaUserCall and csaIDCall are what the recording repositories saw: the tenant
// every call was scoped to and the user or resource it named.
type csaUserCall struct{ tenantID, userID uuid.UUID }

type csaIDCall struct{ tenantID, id uuid.UUID }

type csaCreateCall struct{ tenantID, taskID, userID uuid.UUID }

// csaAssignments scopes every call to its tenant, as row-level security does,
// and records the tenant and ids of each call.
type csaAssignments struct {
	*fakeAssignmentRepo
	mu       sync.Mutex
	listed   []csaUserCall
	vacated  []csaIDCall
	created  []csaCreateCall
	creation []*domain.TaskAssignment
}

func csaWrapAssignments(inner *fakeAssignmentRepo) *csaAssignments {
	return &csaAssignments{fakeAssignmentRepo: inner}
}

func (r *csaAssignments) ListActiveByUser(ctx context.Context, tenantID, userID uuid.UUID) ([]*domain.TaskAssignment, error) {
	r.mu.Lock()
	r.listed = append(r.listed, csaUserCall{tenantID, userID})
	r.mu.Unlock()
	all, err := r.fakeAssignmentRepo.ListActiveByUser(ctx, tenantID, userID)
	if err != nil {
		return nil, err
	}
	var inTenant []*domain.TaskAssignment
	for _, a := range all {
		if a.TenantID == tenantID {
			inTenant = append(inTenant, a)
		}
	}
	return inTenant, nil
}

func (r *csaAssignments) Vacate(ctx context.Context, tenantID, id uuid.UUID) (*domain.TaskAssignment, error) {
	r.mu.Lock()
	r.vacated = append(r.vacated, csaIDCall{tenantID, id})
	r.mu.Unlock()
	if a, ok := r.byID[id]; ok && a.TenantID != tenantID {
		return nil, domain.ErrNotFound
	}
	return r.fakeAssignmentRepo.Vacate(ctx, tenantID, id)
}

func (r *csaAssignments) Create(ctx context.Context, a *domain.TaskAssignment) error {
	r.mu.Lock()
	r.created = append(r.created, csaCreateCall{a.TenantID, a.TaskID, a.UserID})
	r.creation = append(r.creation, a)
	r.mu.Unlock()
	return r.fakeAssignmentRepo.Create(ctx, a)
}

// activeFor returns the active assignments a user holds on a task.
func (r *csaAssignments) activeFor(taskID, userID uuid.UUID) []*domain.TaskAssignment {
	var out []*domain.TaskAssignment
	for _, a := range r.byID {
		if a.TaskID == taskID && a.UserID == userID && a.IsActive {
			out = append(out, a)
		}
	}
	return out
}

// csaTasks hides a task from every other tenant and records each read.
type csaTasks struct {
	*fakeTaskRepo
	mu    sync.Mutex
	reads []csaIDCall
}

func (r *csaTasks) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Task, error) {
	r.mu.Lock()
	r.reads = append(r.reads, csaIDCall{tenantID, id})
	r.mu.Unlock()
	if task, ok := r.byID[id]; ok && task.TenantID != tenantID {
		return nil, domain.ErrNotFound
	}
	return r.fakeTaskRepo.GetByID(ctx, tenantID, id)
}

// csaInstances hides an instance from every other tenant and records each read.
type csaInstances struct {
	*fakeInstanceRepo
	mu    sync.Mutex
	reads []csaIDCall
}

func (r *csaInstances) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	r.mu.Lock()
	r.reads = append(r.reads, csaIDCall{tenantID, id})
	r.mu.Unlock()
	if inst, ok := r.byID[id]; ok && inst.TenantID != tenantID {
		return nil, domain.ErrNotFound
	}
	return r.fakeInstanceRepo.GetByID(ctx, tenantID, id)
}

type csaClientHarness struct {
	svc         *service.WorkflowClient
	instances   *csaInstances
	tasks       *csaTasks
	assignments *csaAssignments
	temporal    *fakeTemporalClient
}

func csaNewClientHarness() *csaClientHarness {
	h := &csaClientHarness{
		instances:   &csaInstances{fakeInstanceRepo: newFakeInstanceRepo()},
		tasks:       &csaTasks{fakeTaskRepo: newFakeTaskRepo()},
		assignments: csaWrapAssignments(newFakeAssignmentRepo()),
		temporal:    &fakeTemporalClient{},
	}
	h.svc = &service.WorkflowClient{Instances: h.instances, Tasks: h.tasks, Assignments: h.assignments, Temporal: h.temporal}
	return h
}

type csaHeld struct {
	instance   *domain.Instance
	task       *domain.Task
	assignment *domain.TaskAssignment
}

// seed gives the user one active assignment, with the given reason, on a task of
// a new instance.
func (h *csaClientHarness) seed(tenantID, userID uuid.UUID, reason string, taskStatus domain.TaskStatus, instanceStatus domain.InstanceStatus) csaHeld {
	return h.seedOnInstance(tenantID, userID, uuid.New(), reason, taskStatus, instanceStatus)
}

func (h *csaClientHarness) seedOnInstance(tenantID, userID, instanceID uuid.UUID, reason string, taskStatus domain.TaskStatus, instanceStatus domain.InstanceStatus) csaHeld {
	inst, ok := h.instances.byID[instanceID]
	if !ok {
		inst = &domain.Instance{ID: instanceID, TenantID: tenantID, Status: instanceStatus, TemporalWorkflowID: tenantID.String() + ":" + instanceID.String(), RecordVersion: 1}
		h.instances.byID[instanceID] = inst
	}
	task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID, Status: taskStatus, RecordVersion: 3}
	h.tasks.byID[task.ID] = task
	a := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true, Reason: reason}
	h.assignments.add(a)
	return csaHeld{instance: inst, task: task, assignment: a}
}

// csaReassignWire is the part of the instance-reassign signal a test reads back.
type csaReassignWire struct {
	TaskID    string
	OldUserID string
	NewUserID string
}

func csaDecodeReassign(t *testing.T, payload any) csaReassignWire {
	t.Helper()
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	var wire csaReassignWire
	require.NoError(t, json.Unmarshal(raw, &wire))
	return wire
}

func TestWorkflowClient_ReassignDelegate_DelegationTagMatchesAsAPrefixOnly(t *testing.T) {
	delegationID := uuid.New()
	tag := "delegation:" + delegationID.String()
	cases := []struct {
		name      string
		reason    string
		wantMoved bool
	}{
		{"a suffix after the tag still matches", tag + "-suffix", true},
		{"the bare tag matches", tag, true},
		{"text before the tag does not match", "x " + tag, false},
		{"an empty reason never matches", "", false},
		{"another delegation's tag does not match", "delegation:" + uuid.NewString(), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := csaNewClientHarness()
			tenantID, oldDelegate, newDelegate := uuid.New(), uuid.New(), uuid.New()
			held := h.seed(tenantID, oldDelegate, tc.reason, domain.TaskStatusReady, domain.InstanceStatusRunning)

			count, err := h.svc.ReassignDelegate(context.Background(), port.ReassignDelegateInput{
				TenantID: tenantID, OldDelegateID: oldDelegate, NewDelegateID: newDelegate, DelegationID: &delegationID,
			})

			require.NoError(t, err)
			assert.Equal(t, []csaUserCall{{tenantID, oldDelegate}}, h.assignments.listed, "the old delegate's holdings are read in the caller's tenant")
			if !tc.wantMoved {
				assert.Zero(t, count)
				assert.True(t, held.assignment.IsActive, "an assignment the filter does not match stays with the old delegate")
				assert.Empty(t, h.assignments.created)
				assert.Empty(t, h.assignments.vacated)
				assert.Empty(t, h.temporal.signals)
				return
			}
			assert.Equal(t, 1, count)
			assert.False(t, held.assignment.IsActive)
			assert.Equal(t, []csaIDCall{{tenantID, held.assignment.ID}}, h.assignments.vacated)
			assert.Equal(t, []csaCreateCall{{tenantID, held.task.ID, newDelegate}}, h.assignments.created)
			require.Len(t, h.assignments.creation, 1)
			assert.Equal(t, tc.reason, h.assignments.creation[0].Reason, "the replacement keeps the original reason, suffix included")
			require.Len(t, h.temporal.signals, 1)
			assert.Equal(t, port.SignalInstanceReassign, h.temporal.signals[0].SignalName)
			assert.Equal(t, held.instance.ID, h.temporal.signals[0].InstanceID)
			assert.Equal(t, csaReassignWire{TaskID: held.task.ID.String(), OldUserID: oldDelegate.String(), NewUserID: newDelegate.String()},
				csaDecodeReassign(t, h.temporal.signals[0].Payload))
		})
	}
}

func TestWorkflowClient_CancelByDelegate_DelegationFilter(t *testing.T) {
	t.Run("the nil UUID is a delegation id like any other, so nothing else matches it", func(t *testing.T) {
		h := csaNewClientHarness()
		tenantID, delegate := uuid.New(), uuid.New()
		nilID := uuid.Nil
		others := []csaHeld{
			h.seed(tenantID, delegate, "delegation:"+uuid.NewString(), domain.TaskStatusReady, domain.InstanceStatusRunning),
			h.seed(tenantID, delegate, "", domain.TaskStatusReady, domain.InstanceStatusRunning),
			h.seed(tenantID, delegate, "manual-assign", domain.TaskStatusReady, domain.InstanceStatusRunning),
		}

		count, err := h.svc.CancelByDelegate(context.Background(), port.CancelByDelegateInput{TenantID: tenantID, DelegateUserID: delegate, DelegationID: &nilID})

		require.NoError(t, err)
		assert.Zero(t, count)
		assert.Equal(t, []csaUserCall{{tenantID, delegate}}, h.assignments.listed)
		assert.Empty(t, h.assignments.vacated, "a nil-UUID delegation id must not be read as no filter")
		for _, held := range others {
			assert.True(t, held.assignment.IsActive)
		}
	})

	t.Run("an assignment tagged with the nil UUID is the one it matches", func(t *testing.T) {
		h := csaNewClientHarness()
		tenantID, delegate := uuid.New(), uuid.New()
		nilID := uuid.Nil
		tagged := h.seed(tenantID, delegate, "delegation:"+uuid.Nil.String(), domain.TaskStatusReady, domain.InstanceStatusRunning)
		other := h.seed(tenantID, delegate, "delegation:"+uuid.NewString(), domain.TaskStatusReady, domain.InstanceStatusRunning)

		count, err := h.svc.CancelByDelegate(context.Background(), port.CancelByDelegateInput{TenantID: tenantID, DelegateUserID: delegate, DelegationID: &nilID})

		require.NoError(t, err)
		assert.Equal(t, 1, count)
		assert.Equal(t, []csaIDCall{{tenantID, tagged.assignment.ID}}, h.assignments.vacated)
		assert.False(t, tagged.assignment.IsActive)
		assert.True(t, other.assignment.IsActive)
	})

	t.Run("a reason that only begins with the tag is vacated, text before the tag is not", func(t *testing.T) {
		h := csaNewClientHarness()
		tenantID, delegate := uuid.New(), uuid.New()
		delegationID := uuid.New()
		tag := "delegation:" + delegationID.String()
		suffixed := h.seed(tenantID, delegate, tag+"-extra", domain.TaskStatusReady, domain.InstanceStatusRunning)
		prefixed := h.seed(tenantID, delegate, "x "+tag, domain.TaskStatusReady, domain.InstanceStatusRunning)

		count, err := h.svc.CancelByDelegate(context.Background(), port.CancelByDelegateInput{TenantID: tenantID, DelegateUserID: delegate, DelegationID: &delegationID})

		require.NoError(t, err)
		assert.Equal(t, 1, count)
		assert.Equal(t, []csaIDCall{{tenantID, suffixed.assignment.ID}}, h.assignments.vacated)
		assert.False(t, suffixed.assignment.IsActive)
		assert.True(t, prefixed.assignment.IsActive)
	})
}

func TestWorkflowClient_DelegateImpact_DelegationFilterExcludesOtherReasons(t *testing.T) {
	h := csaNewClientHarness()
	tenantID, delegate := uuid.New(), uuid.New()
	delegationID := uuid.New()
	tag := "delegation:" + delegationID.String()
	matching := h.seed(tenantID, delegate, tag, domain.TaskStatusReady, domain.InstanceStatusRunning)
	excluded := []csaHeld{
		h.seed(tenantID, delegate, "manual-assign", domain.TaskStatusReady, domain.InstanceStatusRunning),
		h.seed(tenantID, delegate, "delegation:"+uuid.NewString(), domain.TaskStatusReady, domain.InstanceStatusRunning),
		h.seed(tenantID, delegate, "", domain.TaskStatusReady, domain.InstanceStatusRunning),
		h.seed(tenantID, delegate, "x "+tag, domain.TaskStatusReady, domain.InstanceStatusRunning),
	}

	filtered, err := h.svc.DelegateImpact(context.Background(), port.DelegateImpactInput{TenantID: tenantID, DelegateUserID: delegate, DelegationID: &delegationID, Page: port.Page{Limit: 10}})
	require.NoError(t, err)
	assert.Equal(t, 1, filtered.ActiveAssignments)
	assert.Equal(t, 1, filtered.ActiveWorkflows)
	assert.Equal(t, []uuid.UUID{matching.instance.ID}, filtered.WorkflowIDs.Items)
	assert.Equal(t, []csaIDCall{{tenantID, matching.task.ID}}, h.tasks.reads, "only the matching assignment's task is read")
	for _, held := range excluded {
		assert.NotContains(t, filtered.WorkflowIDs.Items, held.instance.ID)
	}

	unfiltered, err := h.svc.DelegateImpact(context.Background(), port.DelegateImpactInput{TenantID: tenantID, DelegateUserID: delegate, Page: port.Page{Limit: 10}})
	require.NoError(t, err)
	assert.Equal(t, 5, unfiltered.ActiveAssignments, "without a delegation id every active assignment is counted")
	assert.Equal(t, 5, unfiltered.ActiveWorkflows)
}

func TestWorkflowClient_DelegateImpact_EachTenantSeesOnlyItsOwnAssignments(t *testing.T) {
	h := csaNewClientHarness()
	tenantA, tenantB, tenantC, delegate := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	var heldA, heldB []uuid.UUID
	for i := 0; i < 2; i++ {
		heldA = append(heldA, h.seed(tenantA, delegate, "", domain.TaskStatusReady, domain.InstanceStatusRunning).instance.ID)
	}
	for i := 0; i < 3; i++ {
		heldB = append(heldB, h.seed(tenantB, delegate, "", domain.TaskStatusReady, domain.InstanceStatusRunning).instance.ID)
	}

	for _, tc := range []struct {
		name     string
		tenantID uuid.UUID
		want     []uuid.UUID
	}{
		{"tenant A", tenantA, heldA},
		{"tenant B", tenantB, heldB},
		{"a tenant the user holds nothing in", tenantC, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h.tasks.reads, h.assignments.listed = nil, nil

			result, err := h.svc.DelegateImpact(context.Background(), port.DelegateImpactInput{TenantID: tc.tenantID, DelegateUserID: delegate, Page: port.Page{Limit: 10}})

			require.NoError(t, err)
			assert.Equal(t, len(tc.want), result.ActiveAssignments)
			assert.Equal(t, len(tc.want), result.ActiveWorkflows)
			assert.ElementsMatch(t, tc.want, result.WorkflowIDs.Items)
			assert.Equal(t, []csaUserCall{{tc.tenantID, delegate}}, h.assignments.listed)
			for _, read := range h.tasks.reads {
				assert.Equal(t, tc.tenantID, read.tenantID, "every task read is made in the caller's tenant")
			}
		})
	}
}

func TestWorkflowClient_DelegateImpact_CursorBeyondTheLastIDGivesAnEmptyPageWithFullTotals(t *testing.T) {
	ids := []uuid.UUID{
		uuid.MustParse("00000000-0000-0000-0000-000000000001"),
		uuid.MustParse("00000000-0000-0000-0000-000000000002"),
		uuid.MustParse("00000000-0000-0000-0000-000000000003"),
	}
	seed := func() (*csaClientHarness, uuid.UUID, uuid.UUID) {
		h := csaNewClientHarness()
		tenantID, delegate := uuid.New(), uuid.New()
		for i, id := range []uuid.UUID{ids[0], ids[1], ids[1], ids[2]} {
			held := h.seedOnInstance(tenantID, delegate, id, "", domain.TaskStatusReady, domain.InstanceStatusRunning)
			held.task.CreatedAt = time.Date(2026, 1, 1, 0, 0, i, 0, time.UTC)
		}
		return h, tenantID, delegate
	}

	for _, tc := range []struct {
		name string
		last uuid.UUID
	}{
		{"the maximum uuid", uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff")},
		{"the last id itself, which is not after it", ids[2]},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, tenantID, delegate := seed()

			result, err := h.svc.DelegateImpact(context.Background(), port.DelegateImpactInput{
				TenantID: tenantID, DelegateUserID: delegate,
				Page: port.Page{Limit: 2, Cursor: &port.CursorPosition{ID: tc.last, CreatedAt: time.Date(2026, 1, 1, 0, 0, 9, 0, time.UTC)}},
			})

			require.NoError(t, err)
			assert.Empty(t, result.WorkflowIDs.Items)
			assert.Empty(t, result.WorkflowIDs.NextCursor, "there is nothing after the cursor, so no next page")
			assert.Equal(t, 3, result.ActiveWorkflows, "the workflow total does not depend on the page")
			assert.Equal(t, 4, result.ActiveAssignments)
		})
	}

	t.Run("a cursor on the first id returns the ids after it", func(t *testing.T) {
		h, tenantID, delegate := seed()

		result, err := h.svc.DelegateImpact(context.Background(), port.DelegateImpactInput{
			TenantID: tenantID, DelegateUserID: delegate,
			Page: port.Page{Limit: 5, Cursor: &port.CursorPosition{ID: ids[0], CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}},
		})

		require.NoError(t, err)
		assert.Equal(t, []uuid.UUID{ids[1], ids[2]}, result.WorkflowIDs.Items)
		assert.Equal(t, 3, result.ActiveWorkflows)
	})
}

func TestWorkflowClient_DelegateWork_IgnoresTaskAndInstanceStatus(t *testing.T) {
	type spec struct {
		task     domain.TaskStatus
		instance domain.InstanceStatus
	}
	specs := []spec{
		{domain.TaskStatusReady, domain.InstanceStatusPaused},
		{domain.TaskStatusInProgress, domain.InstanceStatusDegraded},
		{domain.TaskStatusDeferred, domain.InstanceStatusRunning},
	}
	seedAll := func(h *csaClientHarness, tenantID, user uuid.UUID) []csaHeld {
		var out []csaHeld
		for _, s := range specs {
			out = append(out, h.seed(tenantID, user, "", s.task, s.instance))
		}
		return out
	}

	t.Run("impact counts the assignments of paused and other non-running work", func(t *testing.T) {
		h := csaNewClientHarness()
		tenantID, delegate := uuid.New(), uuid.New()
		held := seedAll(h, tenantID, delegate)

		result, err := h.svc.DelegateImpact(context.Background(), port.DelegateImpactInput{TenantID: tenantID, DelegateUserID: delegate, Page: port.Page{Limit: 10}})

		require.NoError(t, err)
		assert.Equal(t, 3, result.ActiveAssignments)
		assert.Equal(t, 3, result.ActiveWorkflows)
		for _, hd := range held {
			assert.Contains(t, result.WorkflowIDs.Items, hd.instance.ID)
		}
	})

	t.Run("cancel vacates every one of them", func(t *testing.T) {
		h := csaNewClientHarness()
		tenantID, delegate := uuid.New(), uuid.New()
		held := seedAll(h, tenantID, delegate)

		count, err := h.svc.CancelByDelegate(context.Background(), port.CancelByDelegateInput{TenantID: tenantID, DelegateUserID: delegate})

		require.NoError(t, err)
		assert.Equal(t, 3, count)
		for _, hd := range held {
			assert.False(t, hd.assignment.IsActive)
			assert.Contains(t, h.assignments.vacated, csaIDCall{tenantID, hd.assignment.ID})
		}
	})

	t.Run("reassign moves every one of them and signals each instance", func(t *testing.T) {
		h := csaNewClientHarness()
		tenantID, oldDelegate, newDelegate := uuid.New(), uuid.New(), uuid.New()
		held := seedAll(h, tenantID, oldDelegate)

		count, err := h.svc.ReassignDelegate(context.Background(), port.ReassignDelegateInput{TenantID: tenantID, OldDelegateID: oldDelegate, NewDelegateID: newDelegate})

		require.NoError(t, err)
		assert.Equal(t, 3, count)
		require.Len(t, h.temporal.signals, 3)
		signalled := map[uuid.UUID]csaReassignWire{}
		for _, s := range h.temporal.signals {
			assert.Equal(t, port.SignalInstanceReassign, s.SignalName)
			signalled[s.InstanceID] = csaDecodeReassign(t, s.Payload)
		}
		for _, hd := range held {
			assert.False(t, hd.assignment.IsActive)
			assert.Len(t, h.assignments.activeFor(hd.task.ID, newDelegate), 1, "the new delegate holds the task")
			assert.Equal(t, csaReassignWire{TaskID: hd.task.ID.String(), OldUserID: oldDelegate.String(), NewUserID: newDelegate.String()}, signalled[hd.instance.ID])
		}
	})
}
