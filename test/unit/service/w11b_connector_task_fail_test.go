package service_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

// w11bConnectorFakes wires a ConnectorTaskService whose repos record the tenant
// and ids every call received.
type w11bConnectorFakes struct {
	instances   *w11bRecordingInstances
	tasks       *w11bRecordingTasks
	assignments *w11bRecordingAssignments
	principals  *w11bRecordingPrincipals
	temporal    *fakeTemporalClient
	cache       *fakeCacheStore
}

type w11bIDCall struct {
	tenantID uuid.UUID
	id       uuid.UUID
}

type w11bRecordingInstances struct {
	*fakeInstanceRepo
	gets []w11bIDCall
}

func (r *w11bRecordingInstances) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	r.gets = append(r.gets, w11bIDCall{tenantID: tenantID, id: id})
	return r.fakeInstanceRepo.GetByID(ctx, tenantID, id)
}

type w11bRecordingTasks struct {
	*fakeTaskRepo
	gets []w11bIDCall
}

func (r *w11bRecordingTasks) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Task, error) {
	r.gets = append(r.gets, w11bIDCall{tenantID: tenantID, id: id})
	return r.fakeTaskRepo.GetByID(ctx, tenantID, id)
}

type w11bRecordingAssignments struct {
	*fakeAssignmentRepo
	created []*domain.TaskAssignment
}

func (r *w11bRecordingAssignments) Create(ctx context.Context, a *domain.TaskAssignment) error {
	r.created = append(r.created, a)
	return r.fakeAssignmentRepo.Create(ctx, a)
}

type w11bRecordingPrincipals struct {
	principal uuid.UUID
	tenants   []uuid.UUID
}

func (p *w11bRecordingPrincipals) AutomationPrincipal(_ context.Context, tenantID uuid.UUID) (port.AutomationPrincipal, error) {
	p.tenants = append(p.tenants, tenantID)
	return port.AutomationPrincipal{ID: p.principal, Subject: "sub-" + p.principal.String()}, nil
}

func newW11bConnectorFakes(task *domain.Task, inst *domain.Instance) (*service.ConnectorTaskService, *w11bConnectorFakes) {
	f := &w11bConnectorFakes{
		instances:   &w11bRecordingInstances{fakeInstanceRepo: newFakeInstanceRepo(inst)},
		tasks:       &w11bRecordingTasks{fakeTaskRepo: newFakeTaskRepo(task)},
		assignments: &w11bRecordingAssignments{fakeAssignmentRepo: newFakeAssignmentRepo()},
		principals:  &w11bRecordingPrincipals{principal: testConnectorActorID},
		temporal:    &fakeTemporalClient{},
		cache:       newFakeCacheStore(),
	}
	svc := &service.ConnectorTaskService{
		Instances:   f.instances,
		Tasks:       f.tasks,
		Assignments: f.assignments,
		Temporal:    f.temporal,
		Cache:       f.cache,
		Principals:  f.principals,
	}
	return svc, f
}

func TestConnectorTaskService_Fail_SignalCarriesTheTaskPosition(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		status domain.InstanceStatus
	}{
		{name: "running instance", status: domain.InstanceStatusRunning},
		{name: "degraded instance", status: domain.InstanceStatusDegraded},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			task := connectorTask("send-email", domain.TaskStatusReady, `{}`)
			task.NodeKey = "dept-9/nodeZ"
			task.RecordVersion = 7
			task.VisitCount = 4
			inst := instanceWithStatus(task, tc.status)
			svc, f := newW11bConnectorFakes(task, inst)

			require.NoError(t, svc.Fail(context.Background(), task.TenantID, task.ID, "upstream_error", nil))

			require.Len(t, f.temporal.signals, 1, "one stage-fail signal and nothing else")
			sig := f.temporal.signals[0]
			assert.Equal(t, "stage-fail", sig.SignalName)
			assert.Equal(t, inst.TemporalWorkflowID, sig.TemporalWorkflowID)
			assert.Equal(t, inst.ID, sig.InstanceID)
			assert.Empty(t, f.temporal.updates)

			raw, err := json.Marshal(sig.Payload)
			require.NoError(t, err)
			var wire map[string]any
			require.NoError(t, json.Unmarshal(raw, &wire))
			assert.Equal(t, map[string]any{
				"dept_id":        "dept-9",
				"node_id":        "nodeZ",
				"connector_type": "send-email",
				"error_class":    "upstream_error",
				"record_version": float64(7),
				"visit_count":    float64(4),
			}, wire, "the signal names the department and node apart, the task version and the visit")

			assert.Equal(t, []w11bIDCall{{tenantID: task.TenantID, id: task.ID}}, f.tasks.gets)
			assert.Equal(t, []w11bIDCall{{tenantID: task.TenantID, id: inst.ID}}, f.instances.gets)
			assert.Equal(t, []uuid.UUID{task.TenantID}, f.principals.tenants)
			require.Len(t, f.assignments.created, 1)
			assert.Equal(t, task.TenantID, f.assignments.created[0].TenantID)
			assert.Equal(t, task.ID, f.assignments.created[0].TaskID)
			assert.Equal(t, testConnectorActorID, f.assignments.created[0].UserID)
		})
	}
}

func TestConnectorTaskService_Fail_TerminalTask_ChangesNothing(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		status domain.TaskStatus
	}{
		{name: "completed task", status: domain.TaskStatusCompleted},
		{name: "superseded task", status: domain.TaskStatusSuperseded},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			task := connectorTask("send-email", tc.status, `{}`)
			inst := instanceWithStatus(task, domain.InstanceStatusRunning)
			svc, f := newW11bConnectorFakes(task, inst)

			require.NoError(t, svc.Fail(context.Background(), task.TenantID, task.ID, "timeout", nil))

			assert.Equal(t, []w11bIDCall{{tenantID: task.TenantID, id: task.ID}}, f.tasks.gets, "the task is read and nothing after it")
			assert.Empty(t, f.instances.gets, "an answered-before callback does not read the instance")
			assert.Empty(t, f.principals.tenants, "no principal lookup")
			assert.Empty(t, f.assignments.created, "no assignment")
			assert.Empty(t, f.temporal.signals, "no signal")
			assert.Empty(t, f.cache.values, "no dedup reservation")
		})
	}
}
