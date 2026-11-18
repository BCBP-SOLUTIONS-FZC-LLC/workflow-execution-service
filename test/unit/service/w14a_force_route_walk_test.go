package service_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

func w14aDept(id string, nodeIDs ...string) dsl.DepartmentDef {
	stages := make([]dsl.StageDef, len(nodeIDs))
	for i, n := range nodeIDs {
		stages[i] = dsl.StageDef{Type: "userTask", NodeID: n}
	}
	return dsl.DepartmentDef{ID: id, Label: id, Stages: stages}
}

func w14aCollaborationJSON(t *testing.T, plans ...*dsl.CompiledPlan) string {
	t.Helper()
	collab := dsl.CompiledCollaboration{SchemaVersion: dsl.CurrentSchemaVersion, MainPlan: plans[0].Name, Plans: plans}
	b, err := json.Marshal(collab)
	require.NoError(t, err)
	return string(b)
}

type w14aTaskListing struct {
	tenantID, instanceID uuid.UUID
}

// w14aTasks records the tenant and instance of every task listing.
type w14aTasks struct {
	port.TaskRepository
	listings []w14aTaskListing
}

func (r *w14aTasks) ListByInstance(ctx context.Context, tenantID, instanceID uuid.UUID, page port.PageRequest) ([]*domain.Task, *port.Cursor, error) {
	r.listings = append(r.listings, w14aTaskListing{tenantID, instanceID})
	return r.TaskRepository.ListByInstance(ctx, tenantID, instanceID, page)
}

type w14aPlanRead struct {
	tenantID, versionID uuid.UUID
}

// w14aDefinitions records the tenant and version of every plan read.
type w14aDefinitions struct {
	port.DefinitionServiceClient
	reads []w14aPlanRead
}

func (d *w14aDefinitions) GetCompiledWorkflow(ctx context.Context, tenantID, versionID uuid.UUID) (*port.CompiledWorkflow, error) {
	d.reads = append(d.reads, w14aPlanRead{tenantID, versionID})
	return d.DefinitionServiceClient.GetCompiledWorkflow(ctx, tenantID, versionID)
}

type w14aForceRig struct {
	svc      *service.InstanceService
	temporal *fakeTemporalClient
	tasks    *w14aTasks
	plans    *w14aDefinitions
	tenantID uuid.UUID
	inst     *domain.Instance
	actor    uuid.UUID
}

// newW14aForceRig is a running instance whose plan is planJSON and whose open (ready) tasks sit at openNodeKeys.
func newW14aForceRig(t *testing.T, planJSON string, openNodeKeys ...string) *w14aForceRig {
	t.Helper()
	svc, instances, taskRepo, _, _, temporal, definitions, _ := newInstanceServiceHarness()
	tenantID, versionID := uuid.New(), uuid.New()
	inst := &domain.Instance{
		ID: uuid.New(), TenantID: tenantID, WorkflowVersionID: versionID, Status: domain.InstanceStatusRunning,
		RecordVersion: 1, TemporalWorkflowID: "tenant:biz",
	}
	instances.byID[inst.ID] = inst
	definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, planJSON)
	for _, key := range openNodeKeys {
		task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: inst.ID, NodeKey: key, Status: domain.TaskStatusReady}
		taskRepo.byID[task.ID] = task
	}
	rig := &w14aForceRig{
		svc: svc, temporal: temporal, tenantID: tenantID, inst: inst, actor: uuid.New(),
		tasks: &w14aTasks{TaskRepository: taskRepo}, plans: &w14aDefinitions{DefinitionServiceClient: definitions},
	}
	svc.Tasks, svc.Definitions = rig.tasks, rig.plans
	return rig
}

func (r *w14aForceRig) forward(target string) error {
	return r.svc.ForceForward(context.Background(), r.tenantID, r.inst.ID, r.actor, target, r.inst.RecordVersion)
}

func (r *w14aForceRig) requireForwarded(t *testing.T, target string) {
	t.Helper()
	require.Len(t, r.temporal.signals, 1)
	sig := r.temporal.signals[0]
	assert.Equal(t, port.SignalInstanceForceFwd, sig.SignalName)
	assert.Equal(t, r.inst.ID, sig.InstanceID)
	assert.Equal(t, r.inst.TemporalWorkflowID, sig.TemporalWorkflowID)
	raw, err := json.Marshal(sig.Payload)
	require.NoError(t, err)
	var wire struct {
		AdminUserID   string
		TargetNodeKey string
	}
	require.NoError(t, json.Unmarshal(raw, &wire))
	assert.Equal(t, target, wire.TargetNodeKey)
	assert.Equal(t, r.actor.String(), wire.AdminUserID)
	r.requireReadForTenant(t)
}

func (r *w14aForceRig) requireReadForTenant(t *testing.T) {
	t.Helper()
	require.NotEmpty(t, r.plans.reads, "the plan is read to check the target")
	for _, read := range r.plans.reads {
		assert.Equal(t, w14aPlanRead{r.tenantID, r.inst.WorkflowVersionID}, read)
	}
	require.NotEmpty(t, r.tasks.listings, "the open tasks are read to find the open parallel step")
	for _, listing := range r.tasks.listings {
		assert.Equal(t, w14aTaskListing{r.tenantID, r.inst.ID}, listing)
	}
}

func (r *w14aForceRig) requireRefused(t *testing.T, err error) {
	t.Helper()
	require.ErrorIs(t, err, port.ErrTargetNodeNotFound)
	assert.Empty(t, r.temporal.signals, "a refused target is never signalled")
	r.requireReadForTenant(t)
}

// w14aBoundaryTargetPlan runs a parallel step of the legal and finance branches, then archive. The legal branch
// carries route; every department a boundary of route leads to is a department of the plan but of no branch.
func w14aBoundaryTargetPlan(route dsl.ExecutionStep) (*dsl.CompiledPlan, *dsl.CompiledPlan) {
	main := &dsl.CompiledPlan{
		Name: "main", TaskQueue: "wf-queue-default",
		Departments: []dsl.DepartmentDef{
			w14aDept("legal", "review"), w14aDept("finance", "check"), w14aDept("archive", "file"),
			w14aDept("msg", "do"), w14aDept("suberr", "do"), w14aDept("subtimer", "do"), w14aDept("submsg", "do"),
			w14aDept("callerr", "do"), w14aDept("calltimer", "do"), w14aDept("callmsg", "do"),
		},
		Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{
			{Parallel: []dsl.ParallelBranch{
				{DeptID: "legal", Steps: []dsl.ExecutionStep{{Sequential: []string{"legal"}}, route}},
				{DeptID: "finance", Steps: []dsl.ExecutionStep{{Sequential: []string{"finance"}}}},
			}},
			{Sequential: []string{"archive"}},
		}},
	}
	sub := &dsl.CompiledPlan{
		Name: "sub", Departments: []dsl.DepartmentDef{w14aDept("inner", "do")},
		Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"inner"}}}},
	}
	return main, sub
}

func TestForceForward_InsideAParallelStep_AcceptsTheTargetOfAnyBoundaryPathOfTheStepsInIt(t *testing.T) {
	inner := dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"finance"}}}}
	cases := []struct {
		name   string
		route  dsl.ExecutionStep
		target string
	}{
		{"a message path on the step", dsl.ExecutionStep{
			Sequential:   []string{"finance"},
			MessagePaths: []dsl.MessagePath{{MessageName: "m", TargetDept: "msg"}},
		}, "msg/do"},
		{"an error path of a sub-workflow", dsl.ExecutionStep{SubWorkflow: &dsl.SubWorkflowStep{
			Name: "s", Plan: inner, ErrorPaths: []dsl.ErrorPath{{ErrorCode: "E", TargetDept: "suberr"}},
		}}, "suberr/do"},
		{"a timer path of a sub-workflow", dsl.ExecutionStep{SubWorkflow: &dsl.SubWorkflowStep{
			Name: "s", Plan: inner, TimerPaths: []dsl.TimerPath{{Duration: "PT1H", TargetDept: "subtimer"}},
		}}, "subtimer/do"},
		{"a message path of a sub-workflow", dsl.ExecutionStep{SubWorkflow: &dsl.SubWorkflowStep{
			Name: "s", Plan: inner, MessagePaths: []dsl.MessagePath{{MessageName: "m", TargetDept: "submsg"}},
		}}, "submsg/do"},
		{"an error path of a call", dsl.ExecutionStep{CallPlan: &dsl.CallPlanStep{
			NodeID: "CA_1", Name: "CA_1", Plan: "sub", ErrorPaths: []dsl.ErrorPath{{ErrorCode: "E", TargetDept: "callerr"}},
		}}, "callerr/do"},
		{"a timer path of a call", dsl.ExecutionStep{CallPlan: &dsl.CallPlanStep{
			NodeID: "CA_1", Name: "CA_1", Plan: "sub", TimerPaths: []dsl.TimerPath{{Duration: "PT1H", TargetDept: "calltimer"}},
		}}, "calltimer/do"},
		{"a message path of a call", dsl.ExecutionStep{CallPlan: &dsl.CallPlanStep{
			NodeID: "CA_1", Name: "CA_1", Plan: "sub", MessagePaths: []dsl.MessagePath{{MessageName: "m", TargetDept: "callmsg"}},
		}}, "callmsg/do"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			main, sub := w14aBoundaryTargetPlan(tc.route)
			plan := w14aCollaborationJSON(t, main, sub)

			accepting := newW14aForceRig(t, plan, "legal/review")
			require.NoError(t, accepting.forward(tc.target))
			accepting.requireForwarded(t, tc.target)

			refusing := newW14aForceRig(t, plan, "legal/review")
			refusing.requireRefused(t, refusing.forward("archive/file"))
		})
	}

	t.Run("a department no boundary leads to is refused", func(t *testing.T) {
		main, sub := w14aBoundaryTargetPlan(dsl.ExecutionStep{Sequential: []string{"finance"}})
		rig := newW14aForceRig(t, w14aCollaborationJSON(t, main, sub), "legal/review")

		rig.requireRefused(t, rig.forward("msg/do"))
	})
}

// w14aNestedStepsPlan runs host, then archive; the departments x1 and x2 are the branches of the parallel step a
// test nests behind a gateway or a boundary path.
func w14aNestedStepsPlan() (*dsl.CompiledPlan, *dsl.CompiledPlan) {
	main := &dsl.CompiledPlan{
		Name: "main", TaskQueue: "wf-queue-default",
		Departments: []dsl.DepartmentDef{
			w14aDept("host", "work"), w14aDept("x1", "a"), w14aDept("x2", "b"), w14aDept("esc", "look"), w14aDept("archive", "file"),
		},
		Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"host"}}, {Sequential: []string{"archive"}}}},
	}
	sub := &dsl.CompiledPlan{
		Name: "sub", Departments: []dsl.DepartmentDef{w14aDept("inner", "do")},
		Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"inner"}}}},
	}
	return main, sub
}

func w14aNestedParallel() []dsl.ExecutionStep {
	return []dsl.ExecutionStep{{Parallel: []dsl.ParallelBranch{
		{DeptID: "x1", Steps: []dsl.ExecutionStep{{Sequential: []string{"x1"}}}},
		{DeptID: "x2", Steps: []dsl.ExecutionStep{{Sequential: []string{"x2"}}}},
	}}}
}

func TestForceForward_ParallelStepNestedBehindAGatewayOrBoundaryPath_IsAnOpenStep(t *testing.T) {
	cases := []struct {
		name     string
		attach   func(main *dsl.CompiledPlan, nested []dsl.ExecutionStep) []dsl.ExecutionStep
		seesStep bool
	}{
		{"a plan with no parallel step", func(*dsl.CompiledPlan, []dsl.ExecutionStep) []dsl.ExecutionStep { return nil }, false},
		{"an exclusive branch", func(_ *dsl.CompiledPlan, nested []dsl.ExecutionStep) []dsl.ExecutionStep {
			return []dsl.ExecutionStep{{Exclusive: []dsl.ExclusiveBranch{{Target: "esc", ConditionExpression: "true", Steps: nested}}}}
		}, true},
		{"a timer boundary of a stage", func(main *dsl.CompiledPlan, nested []dsl.ExecutionStep) []dsl.ExecutionStep {
			main.Departments[0].Stages[0].BoundaryTimer = &dsl.BoundaryTimer{Duration: "PT1H", Terminates: true, Steps: nested}
			return nil
		}, true},
		{"a message boundary of a stage", func(main *dsl.CompiledPlan, nested []dsl.ExecutionStep) []dsl.ExecutionStep {
			main.Departments[0].Stages[0].BoundaryMessage = &dsl.MessagePath{MessageName: "m", Terminates: true, Steps: nested}
			return nil
		}, true},
		{"a message path on a step", func(_ *dsl.CompiledPlan, nested []dsl.ExecutionStep) []dsl.ExecutionStep {
			return []dsl.ExecutionStep{{Sequential: []string{"host"}, MessagePaths: []dsl.MessagePath{{MessageName: "m", Terminates: true, Steps: nested}}}}
		}, true},
		{"the plan of a sub-workflow", func(_ *dsl.CompiledPlan, nested []dsl.ExecutionStep) []dsl.ExecutionStep {
			return []dsl.ExecutionStep{{SubWorkflow: &dsl.SubWorkflowStep{Name: "s", Plan: dsl.ExecutionPlan{Steps: nested}}}}
		}, true},
		{"an error path of a sub-workflow", func(_ *dsl.CompiledPlan, nested []dsl.ExecutionStep) []dsl.ExecutionStep {
			return []dsl.ExecutionStep{{SubWorkflow: &dsl.SubWorkflowStep{Name: "s", ErrorPaths: []dsl.ErrorPath{{Terminates: true, Steps: nested}}}}}
		}, true},
		{"a timer path of a sub-workflow", func(_ *dsl.CompiledPlan, nested []dsl.ExecutionStep) []dsl.ExecutionStep {
			return []dsl.ExecutionStep{{SubWorkflow: &dsl.SubWorkflowStep{Name: "s", TimerPaths: []dsl.TimerPath{{Duration: "PT1H", Terminates: true, Steps: nested}}}}}
		}, true},
		{"a message path of a sub-workflow", func(_ *dsl.CompiledPlan, nested []dsl.ExecutionStep) []dsl.ExecutionStep {
			return []dsl.ExecutionStep{{SubWorkflow: &dsl.SubWorkflowStep{Name: "s", MessagePaths: []dsl.MessagePath{{MessageName: "m", Terminates: true, Steps: nested}}}}}
		}, true},
		{"an error path of a call", func(_ *dsl.CompiledPlan, nested []dsl.ExecutionStep) []dsl.ExecutionStep {
			return []dsl.ExecutionStep{{CallPlan: &dsl.CallPlanStep{NodeID: "CA_1", Plan: "sub", ErrorPaths: []dsl.ErrorPath{{Terminates: true, Steps: nested}}}}}
		}, true},
		{"a timer path of a call", func(_ *dsl.CompiledPlan, nested []dsl.ExecutionStep) []dsl.ExecutionStep {
			return []dsl.ExecutionStep{{CallPlan: &dsl.CallPlanStep{NodeID: "CA_1", Plan: "sub", TimerPaths: []dsl.TimerPath{{Duration: "PT1H", Terminates: true, Steps: nested}}}}}
		}, true},
		{"a message path of a call", func(_ *dsl.CompiledPlan, nested []dsl.ExecutionStep) []dsl.ExecutionStep {
			return []dsl.ExecutionStep{{CallPlan: &dsl.CallPlanStep{NodeID: "CA_1", Plan: "sub", MessagePaths: []dsl.MessagePath{{MessageName: "m", Terminates: true, Steps: nested}}}}}
		}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			main, sub := w14aNestedStepsPlan()
			main.Execution.Steps = append(
				append([]dsl.ExecutionStep{{Sequential: []string{"host"}}}, tc.attach(main, w14aNestedParallel())...),
				dsl.ExecutionStep{Sequential: []string{"archive"}},
			)
			plan := w14aCollaborationJSON(t, main, sub)

			sibling := newW14aForceRig(t, plan, "x1/a")
			require.NoError(t, sibling.forward("x2/b"), "a sibling branch is always a branch of the open step")
			sibling.requireForwarded(t, "x2/b")

			after := newW14aForceRig(t, plan, "x1/a")
			err := after.forward("archive/file")
			if !tc.seesStep {
				require.NoError(t, err, "control: with no parallel step open, any node of the plan is a top-level jump")
				after.requireForwarded(t, "archive/file")
				return
			}
			after.requireRefused(t, err)
		})
	}
}
