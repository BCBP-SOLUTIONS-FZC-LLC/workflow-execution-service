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
)

var errW7aDatabase = errors.New("w7a: database unavailable")

// w7aTaskRepo answers for one task, in one tenant only, and records the tenant
// and task id of every read.
type w7aTaskRepo struct {
	port.TaskRepository
	task    *domain.Task
	err     error
	tenants []uuid.UUID
	ids     []uuid.UUID
}

func (r *w7aTaskRepo) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Task, error) {
	r.tenants = append(r.tenants, tenantID)
	r.ids = append(r.ids, id)
	if r.err != nil {
		return nil, r.err
	}
	if r.task == nil || r.task.ID != id || r.task.TenantID != tenantID {
		return nil, domain.ErrNotFound
	}
	return r.task, nil
}

// w7aInstanceRepo answers for one instance, in one tenant only, and records the
// tenant and instance id of every read.
type w7aInstanceRepo struct {
	port.InstanceRepository
	inst    *domain.Instance
	err     error
	tenants []uuid.UUID
	ids     []uuid.UUID
}

func (r *w7aInstanceRepo) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	r.tenants = append(r.tenants, tenantID)
	r.ids = append(r.ids, id)
	if r.err != nil {
		return nil, r.err
	}
	if r.inst == nil || r.inst.ID != id || r.inst.TenantID != tenantID {
		return nil, domain.ErrNotFound
	}
	return r.inst, nil
}

type w7aConnectorHarness struct {
	svc         *service.ConnectorTaskService
	tasks       *w7aTaskRepo
	instances   *w7aInstanceRepo
	assignments *fakeAssignmentRepo
	temporal    *fakeTemporalClient
	cache       *fakeCacheStore
	log         *fakeLogger
	task        *domain.Task
	inst        *domain.Instance
}

// w7aNewConnectorHarness stores one READY connector task and its RUNNING
// instance under the task's tenant.
func w7aNewConnectorHarness(extras string) *w7aConnectorHarness {
	task := connectorTask("storage", domain.TaskStatusReady, extras)
	inst := instanceWithStatus(task, domain.InstanceStatusRunning)
	inst.TenantID = task.TenantID
	h := &w7aConnectorHarness{
		tasks:       &w7aTaskRepo{task: task},
		instances:   &w7aInstanceRepo{inst: inst},
		assignments: newFakeAssignmentRepo(),
		temporal:    &fakeTemporalClient{},
		cache:       newFakeCacheStore(),
		log:         &fakeLogger{},
		task:        task,
		inst:        inst,
	}
	h.svc = &service.ConnectorTaskService{
		Instances:   h.instances,
		Tasks:       h.tasks,
		Assignments: h.assignments,
		Temporal:    h.temporal,
		Cache:       h.cache,
		Principals:  fakePrincipals{principal: testConnectorActorID},
		Log:         h.log,
	}
	return h
}

func (h *w7aConnectorHarness) complete(tenantID uuid.UUID) error {
	return h.svc.Complete(context.Background(), tenantID, h.task.ID, map[string]any{})
}

func (h *w7aConnectorHarness) fail(tenantID uuid.UUID) error {
	return h.svc.Fail(context.Background(), tenantID, h.task.ID, "upstream_error", nil)
}

func (h *w7aConnectorHarness) requireNothingWritten(t *testing.T) {
	t.Helper()
	assert.Empty(t, h.temporal.signals, "the workflow is not signalled")
	assert.Empty(t, h.assignments.byID, "no assignment is recorded for the automation principal")
	assert.Empty(t, h.cache.values, "no dedup reservation is left behind")
}

func w7aDecodeWire(t *testing.T, payload any) map[string]any {
	t.Helper()
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	var wire map[string]any
	require.NoError(t, json.Unmarshal(raw, &wire))
	return wire
}

func TestConnectorTaskService_Callback_LookupRejected_SignalsNothing(t *testing.T) {
	t.Parallel()

	otherTenant := uuid.New()
	cases := []struct {
		name         string
		arrange      func(h *w7aConnectorHarness)
		callerTenant func(h *w7aConnectorHarness) uuid.UUID
		wantIs       error
		wantNotIs    []error
		readsInst    bool
	}{
		{
			name:    "task does not exist",
			arrange: func(h *w7aConnectorHarness) { h.tasks.task = nil },
			wantIs:  port.ErrTaskNotFound,
		},
		{
			name:         "task belongs to another tenant",
			callerTenant: func(*w7aConnectorHarness) uuid.UUID { return otherTenant },
			wantIs:       port.ErrTaskNotFound,
		},
		{
			name:      "task read fails with a database error",
			arrange:   func(h *w7aConnectorHarness) { h.tasks.err = errW7aDatabase },
			wantIs:    errW7aDatabase,
			wantNotIs: []error{port.ErrTaskNotFound},
		},
		{
			name:    "task has no connector type",
			arrange: func(h *w7aConnectorHarness) { h.task.ConnectorType = nil },
			wantIs:  port.ErrTaskNotConnectorTyped,
		},
		{
			name: "task has an empty connector type",
			arrange: func(h *w7aConnectorHarness) {
				empty := ""
				h.task.ConnectorType = &empty
			},
			wantIs: port.ErrTaskNotConnectorTyped,
		},
		{
			name:      "instance does not exist",
			arrange:   func(h *w7aConnectorHarness) { h.instances.inst = nil },
			wantIs:    port.ErrInstanceNotFound,
			readsInst: true,
		},
		{
			name:      "instance read fails with a database error",
			arrange:   func(h *w7aConnectorHarness) { h.instances.err = errW7aDatabase },
			wantIs:    errW7aDatabase,
			wantNotIs: []error{port.ErrInstanceNotFound},
			readsInst: true,
		},
		{
			name:      "instance is pending",
			arrange:   func(h *w7aConnectorHarness) { h.inst.Status = domain.InstanceStatus("PENDING") },
			wantIs:    port.ErrInvalidInstanceState,
			readsInst: true,
		},
		{
			name:      "instance has a status the service does not know",
			arrange:   func(h *w7aConnectorHarness) { h.inst.Status = domain.InstanceStatus("ARCHIVED") },
			wantIs:    port.ErrInvalidInstanceState,
			readsInst: true,
		},
	}
	outcomes := []struct {
		name string
		call func(h *w7aConnectorHarness, tenantID uuid.UUID) error
	}{
		{"complete", (*w7aConnectorHarness).complete},
		{"fail", (*w7aConnectorHarness).fail},
	}

	for _, tc := range cases {
		for _, outcome := range outcomes {
			t.Run(outcome.name+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				h := w7aNewConnectorHarness(`{}`)
				if tc.arrange != nil {
					tc.arrange(h)
				}
				tenantID := h.task.TenantID
				if tc.callerTenant != nil {
					tenantID = tc.callerTenant(h)
				}

				err := outcome.call(h, tenantID)

				require.ErrorIs(t, err, tc.wantIs)
				for _, not := range tc.wantNotIs {
					assert.NotErrorIs(t, err, not)
				}
				assert.Equal(t, []uuid.UUID{tenantID}, h.tasks.tenants, "the task is read in the caller's tenant")
				assert.Equal(t, []uuid.UUID{h.task.ID}, h.tasks.ids)
				if tc.readsInst {
					assert.Equal(t, []uuid.UUID{tenantID}, h.instances.tenants, "the instance is read in the caller's tenant")
					assert.Equal(t, []uuid.UUID{h.task.WorkflowInstanceID}, h.instances.ids)
				} else {
					assert.Empty(t, h.instances.ids, "the instance is not read once the task is refused")
				}
				h.requireNothingWritten(t)
			})
		}
	}
}

func TestConnectorTaskService_Fail_NoPrincipalResolver_FailsClosed(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		principals port.AutomationPrincipalResolver
	}{
		{name: "resolver not wired", principals: nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := w7aNewConnectorHarness(`{}`)
			h.svc.Principals = tc.principals

			err := h.fail(h.task.TenantID)

			require.ErrorIs(t, err, port.ErrNoAutomationPrincipal)
			h.requireNothingWritten(t)
		})
	}
}

func TestConnectorTaskService_Fail_AutomationAssignmentAlreadyExists_StillSignals(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		arrange func(h *w7aConnectorHarness)
	}{
		{
			name:    "the repository reports the assignment already exists",
			arrange: func(h *w7aConnectorHarness) { h.assignments.createErr = domain.ErrAlreadyExists },
		},
		{
			name: "the principal already holds an active assignment on the task",
			arrange: func(h *w7aConnectorHarness) {
				h.assignments.add(&domain.TaskAssignment{
					ID: uuid.New(), TenantID: h.task.TenantID, TaskID: h.task.ID,
					UserID: testConnectorActorID, IsActive: true, Reason: "automation",
				})
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := w7aNewConnectorHarness(`{}`)
			tc.arrange(h)
			before := len(h.assignments.byID)

			require.NoError(t, h.fail(h.task.TenantID))

			assert.Len(t, h.assignments.byID, before, "the existing assignment is reused, none is added")
			require.Len(t, h.temporal.signals, 1, "the callback continues to the signal")
			sig := h.temporal.signals[0]
			assert.Equal(t, "stage-fail", sig.SignalName)
			assert.Equal(t, h.inst.TemporalWorkflowID, sig.TemporalWorkflowID)
			assert.Equal(t, h.inst.ID, sig.InstanceID)
			assert.Equal(t, "upstream_error", w7aDecodeWire(t, sig.Payload)["error_class"])
			assert.Equal(t, "delivered", h.cache.values["connector-signal:fail:"+h.task.ID.String()],
				"the fail outcome is recorded as delivered")
		})
	}
}

func TestConnectorTaskService_Fail_NoCache_SignalsEveryCallWithoutDedup(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		calls int
	}{
		{name: "one call", calls: 1},
		{name: "a repeated call", calls: 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := w7aNewConnectorHarness(`{}`)
			h.svc.Cache = nil

			for range tc.calls {
				require.NoError(t, h.fail(h.task.TenantID))
			}

			require.Len(t, h.temporal.signals, tc.calls, "with no cache every call signals")
			for _, sig := range h.temporal.signals {
				assert.Equal(t, "stage-fail", sig.SignalName)
				assert.Equal(t, h.inst.ID, sig.InstanceID)
			}
		})
	}
}

func TestConnectorTaskService_Complete_UnreadableOutputMapping_AssignmentFailureSignalsNothing(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		extras string
	}{
		{name: "stored mapping is not JSON", extras: `{not json`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := w7aNewConnectorHarness(tc.extras)
			h.assignments.createErr = errW7aDatabase

			err := h.complete(h.task.TenantID)

			require.ErrorIs(t, err, errW7aDatabase)
			assert.Empty(t, h.temporal.signals, "no stage-fail signal is sent for an unattributable failure")
			assert.Empty(t, h.cache.values, "the fail reservation is deleted so the worker's retry is not blocked")
			assert.Len(t, h.log.errorCalls, 1, "the unreadable mapping is logged once")

			h.assignments.createErr = nil
			require.NoError(t, h.complete(h.task.TenantID), "the worker's retry goes through")
			require.Len(t, h.temporal.signals, 1)
			sig := h.temporal.signals[0]
			assert.Equal(t, "stage-fail", sig.SignalName)
			assert.Equal(t, "output_mapping_invalid", w7aDecodeWire(t, sig.Payload)["error_class"])
			require.Len(t, h.assignments.byID, 1)
			for _, a := range h.assignments.byID {
				assert.Equal(t, testConnectorActorID, a.UserID)
				assert.Equal(t, h.task.ID, a.TaskID)
				assert.Equal(t, h.task.TenantID, a.TenantID)
			}
		})
	}
}
