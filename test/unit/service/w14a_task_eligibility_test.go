package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

var errW14aPlanUnavailable = errors.New("definitions answered 503")

// w14aPlans records the tenant and version of every plan read and answers the plan it was given.
type w14aPlans struct {
	resp  *port.CompiledWorkflow
	err   error
	reads []w14aPlanRead
}

func (p *w14aPlans) GetCompiledWorkflow(_ context.Context, tenantID, versionID uuid.UUID) (*port.CompiledWorkflow, error) {
	p.reads = append(p.reads, w14aPlanRead{tenantID, versionID})
	return p.resp, p.err
}

type w14aEligibilityAsk struct {
	tenantID, userID, departmentID uuid.UUID
	level                          string
}

// w14aEligibility records every single check it is asked and answers eligible.
type w14aEligibility struct {
	asks     []w14aEligibilityAsk
	eligible bool
}

func (e *w14aEligibility) CheckEligibility(_ context.Context, tenantID, userID, departmentID uuid.UUID, level string) (bool, error) {
	e.asks = append(e.asks, w14aEligibilityAsk{tenantID, userID, departmentID, level})
	return e.eligible, nil
}

func (e *w14aEligibility) CheckEligibilityBatch(context.Context, uuid.UUID, []port.EligibilityCheckRequest) ([]port.EligibilityResult, error) {
	return nil, errors.New("the task service asks single checks only")
}

func w14aPlanWithDept(t *testing.T, dept dsl.DepartmentDef) string {
	t.Helper()
	collab := dsl.CompiledCollaboration{
		SchemaVersion: dsl.CurrentSchemaVersion, MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{Name: "main", TaskQueue: "wf-queue-default", Departments: []dsl.DepartmentDef{dept}}},
	}
	b, err := json.Marshal(collab)
	require.NoError(t, err)
	return string(b)
}

// w14aTaskLookups records the tenant and id of every task read by id.
type w14aTaskLookups struct {
	port.TaskRepository
	reads []w14aTaskRead
}

func (r *w14aTaskLookups) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Task, error) {
	r.reads = append(r.reads, w14aTaskRead{tenantID, id})
	return r.TaskRepository.GetByID(ctx, tenantID, id)
}

// w14aAssignmentLookups records the tenant and task of every read of a task's active assignments.
type w14aAssignmentLookups struct {
	port.TaskAssignmentRepository
	reads []w14aTaskRead
}

func (r *w14aAssignmentLookups) ListActiveByTask(ctx context.Context, tenantID, taskID uuid.UUID) ([]*domain.TaskAssignment, error) {
	r.reads = append(r.reads, w14aTaskRead{tenantID, taskID})
	return r.TaskAssignmentRepository.ListActiveByTask(ctx, tenantID, taskID)
}

// w14aTaskReads is what a task action read, by tenant and id.
type w14aTaskReads struct {
	instances   *w14aInstances
	tasks       *w14aTaskLookups
	assignments *w14aAssignmentLookups
}

func w14aRecordTaskReads(svc *service.TaskService, instances *fakeInstanceRepo, tasks *fakeTaskRepo, assignments *fakeAssignmentRepo) *w14aTaskReads {
	reads := &w14aTaskReads{
		instances:   &w14aInstances{InstanceRepository: instances},
		tasks:       &w14aTaskLookups{TaskRepository: tasks},
		assignments: &w14aAssignmentLookups{TaskAssignmentRepository: assignments},
	}
	svc.Instances, svc.Tasks, svc.Assignments = reads.instances, reads.tasks, reads.assignments
	return reads
}

// requireOnly checks that the action read the instance, the task and the task's active assignments, and nothing
// of another tenant, instance or task.
func (r *w14aTaskReads) requireOnly(t *testing.T, tenantID, instanceID, taskID uuid.UUID) {
	t.Helper()
	require.NotEmpty(t, r.instances.reads, "the instance is read")
	for _, read := range r.instances.reads {
		assert.Equal(t, w14aInstanceRead{tenantID, instanceID}, read)
	}
	require.NotEmpty(t, r.tasks.reads, "the task is read")
	for _, read := range r.tasks.reads {
		assert.Equal(t, w14aTaskRead{tenantID, taskID}, read)
	}
	require.NotEmpty(t, r.assignments.reads, "the task's active assignments are read")
	for _, read := range r.assignments.reads {
		assert.Equal(t, w14aTaskRead{tenantID, taskID}, read)
	}
}

type w14aTaskRig struct {
	svc         *service.TaskService
	temporal    *fakeTemporalClient
	reads       *w14aTaskReads
	inst        *domain.Instance
	task        *domain.Task
	assignments *fakeAssignmentRepo
	holder      uuid.UUID
	newUser     uuid.UUID
	actor       uuid.UUID
	departments uuid.UUID
}

// newW14aTaskRig is a running instance with a task of finance/review in status, held by one user.
func newW14aTaskRig(status domain.TaskStatus) *w14aTaskRig {
	svc, instances, tasks, assignments, _, temporal := newTaskServiceHarness()
	tenantID := uuid.New()
	inst := &domain.Instance{
		ID: uuid.New(), TenantID: tenantID, WorkflowVersionID: uuid.New(), Status: domain.InstanceStatusRunning, TemporalWorkflowID: "tenant:biz",
	}
	instances.byID[inst.ID] = inst
	task := &domain.Task{
		ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: inst.ID, DepartmentID: uuid.New(), NodeKey: "finance/review",
		Status: status, AssigneeMode: "single", RecordVersion: 4,
	}
	tasks.byID[task.ID] = task
	holder := uuid.New()
	assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: holder, IsActive: true}
	reads := w14aRecordTaskReads(svc, instances, tasks, assignments)
	return &w14aTaskRig{
		svc: svc, temporal: temporal, reads: reads, inst: inst, task: task, assignments: assignments,
		holder: holder, newUser: uuid.New(), actor: uuid.New(), departments: task.DepartmentID,
	}
}

func (r *w14aTaskRig) reassign() error {
	_, err := r.svc.Reassign(context.Background(), r.inst.TenantID, r.task.ID, r.actor, r.newUser, r.task.RecordVersion)
	return err
}

func (r *w14aTaskRig) requireReassignSignal(t *testing.T) {
	t.Helper()
	require.Len(t, r.temporal.signals, 1)
	sig := r.temporal.signals[0]
	assert.Equal(t, port.SignalInstanceReassign, sig.SignalName)
	assert.Equal(t, r.inst.ID, sig.InstanceID)
	assert.Equal(t, r.inst.TemporalWorkflowID, sig.TemporalWorkflowID)
	raw, err := json.Marshal(sig.Payload)
	require.NoError(t, err)
	var wire struct{ TaskID, OldUserID, NewUserID, AdminUserID string }
	require.NoError(t, json.Unmarshal(raw, &wire))
	assert.Equal(t, r.task.ID.String(), wire.TaskID)
	assert.Equal(t, r.holder.String(), wire.OldUserID)
	assert.Equal(t, r.newUser.String(), wire.NewUserID)
	assert.Equal(t, r.actor.String(), wire.AdminUserID)
	r.reads.requireOnly(t, r.inst.TenantID, r.inst.ID, r.task.ID)
}

func TestTaskService_Reassign_OnlyAReadyOrInProgressTaskIsActionable(t *testing.T) {
	cases := []struct {
		status  domain.TaskStatus
		allowed bool
	}{
		{domain.TaskStatusReady, true},
		{domain.TaskStatusInProgress, true},
		{domain.TaskStatusCompleted, false},
		{domain.TaskStatusDeferred, false},
		{domain.TaskStatusFailed, false},
		{domain.TaskStatusSuperseded, false},
	}
	for _, tc := range cases {
		t.Run(string(tc.status), func(t *testing.T) {
			rig := newW14aTaskRig(tc.status)
			fx9AllowEligibility(t, rig.svc, rig.departments)

			err := rig.reassign()

			if !tc.allowed {
				require.ErrorIs(t, err, port.ErrInvalidTaskState)
				assert.Empty(t, rig.temporal.signals, "a task that is not open is never reassigned")
				return
			}
			require.NoError(t, err)
			rig.requireReassignSignal(t)
		})
	}
}

func TestTaskService_Reassign_EligibilityCheckRefusesWhenNotFullyConfigured(t *testing.T) {
	cases := []struct {
		name        string
		definitions bool
		eligibility bool
	}{
		{"neither dependency is configured", false, false},
		{"only the plan source is configured", true, false},
		{"only the eligibility source is configured", false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := newW14aTaskRig(domain.TaskStatusReady)
			plans := &w14aPlans{err: errW14aPlanUnavailable}
			eligibility := &w14aEligibility{}
			rig.svc.Definitions, rig.svc.Eligibility = nil, nil
			if tc.definitions {
				rig.svc.Definitions = plans
			}
			if tc.eligibility {
				rig.svc.Eligibility = eligibility
			}

			require.Error(t, rig.reassign(), "an unconfigured check refuses the reassignment")

			assert.Empty(t, rig.temporal.signals, "nothing is signalled for an unchecked user")
			assert.Empty(t, plans.reads, "no plan is read when the check cannot run")
			assert.Empty(t, eligibility.asks, "nobody is asked about eligibility")
		})
	}

	t.Run("control: with both configured the plan is read and the new user is asked about the task's stage", func(t *testing.T) {
		rig := newW14aTaskRig(domain.TaskStatusReady)
		plans := &w14aPlans{resp: publishedCompiledWorkflow(uuid.New(), rig.inst.WorkflowVersionID, w14aPlanWithDept(t, dsl.DepartmentDef{
			ID: "finance", IAMDepartmentID: rig.departments.String(), Stages: []dsl.StageDef{{Type: "userTask", NodeID: "review", Role: "reviewer"}},
		}))}
		eligibility := &w14aEligibility{eligible: true}
		rig.svc.Definitions, rig.svc.Eligibility = plans, eligibility

		require.NoError(t, rig.reassign())

		rig.requireReassignSignal(t)
		assert.Equal(t, []w14aPlanRead{{rig.inst.TenantID, rig.inst.WorkflowVersionID}}, plans.reads)
		assert.Equal(t, []w14aEligibilityAsk{{rig.inst.TenantID, rig.newUser, rig.departments, "reviewer"}}, eligibility.asks)
	})
}

func TestTaskService_Reassign_EligibilityCheckRefusesWhatItCannotResolve(t *testing.T) {
	t.Run("a plan that cannot be loaded fails with the cause wrapped and nothing is signalled", func(t *testing.T) {
		rig := newW14aTaskRig(domain.TaskStatusReady)
		plans := &w14aPlans{err: errW14aPlanUnavailable}
		eligibility := &w14aEligibility{eligible: true}
		rig.svc.Definitions, rig.svc.Eligibility = plans, eligibility

		err := rig.reassign()

		require.ErrorIs(t, err, errW14aPlanUnavailable)
		assert.ErrorContains(t, err, "get compiled plan for eligibility check")
		assert.NotErrorIs(t, err, port.ErrAssigneeIneligible, "an unreadable plan is not a verdict on the user")
		assert.Equal(t, []w14aPlanRead{{rig.inst.TenantID, rig.inst.WorkflowVersionID}}, plans.reads)
		assert.Empty(t, eligibility.asks)
		assert.Empty(t, rig.temporal.signals)
	})

	cases := []struct {
		name string
		dept dsl.DepartmentDef
	}{
		{"a plan with no stage for the task's node", dsl.DepartmentDef{ID: "finance", Stages: []dsl.StageDef{{Type: "userTask", NodeID: "approve", Role: "approver"}}}},
		{"a department of the task's uuid whose node key differs", dsl.DepartmentDef{ID: "legal", Stages: []dsl.StageDef{{Type: "userTask", NodeID: "review", Role: "reviewer"}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name+" is an ineligible assignee, not asked about", func(t *testing.T) {
			rig := newW14aTaskRig(domain.TaskStatusReady)
			tc.dept.IAMDepartmentID = rig.departments.String()
			plans := &w14aPlans{resp: publishedCompiledWorkflow(uuid.New(), rig.inst.WorkflowVersionID, w14aPlanWithDept(t, tc.dept))}
			eligibility := &w14aEligibility{eligible: true}
			rig.svc.Definitions, rig.svc.Eligibility = plans, eligibility

			err := rig.reassign()

			require.ErrorIs(t, err, port.ErrAssigneeIneligible)
			assert.ErrorContains(t, err, rig.task.ID.String(), "the error names the task that has no stage")
			assert.Empty(t, eligibility.asks)
			assert.Empty(t, rig.temporal.signals)
		})
	}
}

func TestTaskService_Complete_SplitsTheNodeKeyAtTheFirstSlash(t *testing.T) {
	cases := []struct {
		name     string
		nodeKey  string
		wantDept string
		wantNode string
	}{
		{"department and node", "finance/review", "finance", "review"},
		{"a node with a slash keeps the rest", "finance/review/extra", "finance", "review/extra"},
		{"a call-scoped department", "CA_1::legal/review", "CA_1::legal", "review"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, instances, tasks, assignments, _, temporal := newTaskServiceHarness()
			tenantID, userID := uuid.New(), uuid.New()
			inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, TemporalWorkflowID: "tenant:biz", Status: domain.InstanceStatusRunning}
			instances.byID[inst.ID] = inst
			task := newReadyTask(tenantID, inst.ID)
			task.NodeKey = tc.nodeKey
			tasks.byID[task.ID] = task
			assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true}
			svc.Definitions = &fakeDefinitionClient{resp: publishedCompiledWorkflow(uuid.New(), uuid.New(), compiledPlanJSONWithDept(t, dsl.DepartmentDef{
				ID: tc.wantDept, IAMDepartmentID: task.DepartmentID.String(), Stages: []dsl.StageDef{{Type: "userTask", NodeID: tc.wantNode, Role: "approver"}},
			}))}
			reads := w14aRecordTaskReads(svc, instances, tasks, assignments)

			_, err := svc.Complete(context.Background(), tenantID, task.ID, userID, json.RawMessage(`{}`), task.RecordVersion)

			require.NoError(t, err)
			require.Len(t, temporal.signals, 1)
			assert.Equal(t, inst.ID, temporal.signals[0].InstanceID)
			assert.Equal(t, inst.TemporalWorkflowID, temporal.signals[0].TemporalWorkflowID)
			raw, err := json.Marshal(temporal.signals[0].Payload)
			require.NoError(t, err)
			var wire struct{ DeptID, NodeID, UserID string }
			require.NoError(t, json.Unmarshal(raw, &wire))
			assert.Equal(t, tc.wantDept, wire.DeptID)
			assert.Equal(t, tc.wantNode, wire.NodeID)
			assert.Equal(t, userID.String(), wire.UserID)
			reads.requireOnly(t, tenantID, inst.ID, task.ID)
		})
	}
}

func TestConnectorTaskService_Callbacks_SplitTheNodeKeyAtTheFirstSlash(t *testing.T) {
	keys := []struct {
		name     string
		nodeKey  string
		wantDept string
		wantNode string
	}{
		{"department and node", "dept-1/nodeA", "dept-1", "nodeA"},
		{"a key with no slash is all department", "dept-1", "dept-1", ""},
		{"a node with a slash keeps the rest", "dept-1/nodeA/extra", "dept-1", "nodeA/extra"},
	}
	for _, key := range keys {
		t.Run("complete, "+key.name, func(t *testing.T) {
			task := connectorTask("storage", domain.TaskStatusReady, `{"resolved_inputs":{},"output_mapping":[]}`)
			task.NodeKey = key.nodeKey
			inst := instanceWithStatus(task, domain.InstanceStatusRunning)
			temporal := &fakeTemporalClient{}
			svc := newConnectorTaskService(task, inst, temporal, newFakeCacheStore())

			require.NoError(t, svc.Complete(context.Background(), task.TenantID, task.ID, map[string]any{}))

			require.Len(t, temporal.signals, 1)
			assert.Equal(t, "stage-transition", temporal.signals[0].SignalName)
			assert.Equal(t, inst.ID, temporal.signals[0].InstanceID)
			raw, err := json.Marshal(temporal.signals[0].Payload)
			require.NoError(t, err)
			var wire struct{ DeptID, NodeID string }
			require.NoError(t, json.Unmarshal(raw, &wire))
			assert.Equal(t, key.wantDept, wire.DeptID)
			assert.Equal(t, key.wantNode, wire.NodeID)
		})
		t.Run("fail, "+key.name, func(t *testing.T) {
			task := connectorTask("storage", domain.TaskStatusReady, `{}`)
			task.NodeKey = key.nodeKey
			inst := instanceWithStatus(task, domain.InstanceStatusRunning)
			temporal := &fakeTemporalClient{}
			svc := newConnectorTaskService(task, inst, temporal, newFakeCacheStore())

			require.NoError(t, svc.Fail(context.Background(), task.TenantID, task.ID, "upstream_error", nil))

			require.Len(t, temporal.signals, 1)
			assert.Equal(t, "stage-fail", temporal.signals[0].SignalName)
			assert.Equal(t, inst.ID, temporal.signals[0].InstanceID)
			raw, err := json.Marshal(temporal.signals[0].Payload)
			require.NoError(t, err)
			var wire struct {
				DeptID string `json:"dept_id"`
				NodeID string `json:"node_id"`
			}
			require.NoError(t, json.Unmarshal(raw, &wire))
			assert.Equal(t, key.wantDept, wire.DeptID)
			assert.Equal(t, key.wantNode, wire.NodeID)
		})
	}
}
