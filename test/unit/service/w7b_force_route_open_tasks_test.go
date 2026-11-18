package service_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

// w7bTaskPages records each page request of an instance's tasks and fails
// every page from the failAt-th on (0 never fails).
type w7bTaskPages struct {
	*fakeTaskRepo
	pages     []port.PageRequest
	tenants   []uuid.UUID
	instances []uuid.UUID
	failAt    int
	err       error
}

func (r *w7bTaskPages) ListByInstance(ctx context.Context, tenantID, instanceID uuid.UUID, page port.PageRequest) ([]*domain.Task, *port.Cursor, error) {
	r.pages = append(r.pages, page)
	r.tenants = append(r.tenants, tenantID)
	r.instances = append(r.instances, instanceID)
	if r.failAt > 0 && len(r.pages) >= r.failAt {
		return nil, nil, r.err
	}
	return r.fakeTaskRepo.ListByInstance(ctx, tenantID, instanceID, page)
}

type w7bForceRig struct {
	svc       *service.InstanceService
	tasks     *w7bTaskPages
	tenantID  uuid.UUID
	instance  *domain.Instance
	temporal  *fakeTemporalClient
	signalled func() []signalCall
}

func w7bNewForceRig(t *testing.T, planJSON string) *w7bForceRig {
	t.Helper()
	svc, instances, taskRepo, _, _, temporal, definitions, _ := newInstanceServiceHarness()
	tenantID := uuid.New()
	inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning, RecordVersion: 1, TemporalWorkflowID: "tenant:biz"}
	instances.byID[inst.ID] = inst
	definitions.resp = publishedCompiledWorkflow(uuid.New(), uuid.New(), planJSON)
	pages := &w7bTaskPages{fakeTaskRepo: taskRepo}
	svc.Tasks = pages
	return &w7bForceRig{svc: svc, tasks: pages, tenantID: tenantID, instance: inst, temporal: temporal, signalled: func() []signalCall { return temporal.signals }}
}

// addTasks gives the instance count tasks of the node, oldest first from start.
func (r *w7bForceRig) addTasks(count int, nodeKey string, status domain.TaskStatus, start time.Time) {
	for i := 0; i < count; i++ {
		id := uuid.New()
		r.tasks.byID[id] = &domain.Task{ID: id, TenantID: r.tenantID, WorkflowInstanceID: r.instance.ID, NodeKey: nodeKey, Status: status, CreatedAt: start.Add(time.Duration(i) * time.Second)}
	}
}

// requireListedForRig asserts every page request named the rig's tenant and instance.
func (r *w7bForceRig) requireListedForRig(t *testing.T) {
	t.Helper()
	require.NotEmpty(t, r.tasks.pages)
	for i := range r.tasks.pages {
		assert.Equal(t, r.tenantID, r.tasks.tenants[i], "the task list is read for the caller's tenant")
		assert.Equal(t, r.instance.ID, r.tasks.instances[i], "and for the instance being routed")
	}
}

func (r *w7bForceRig) forward(target string) error {
	return r.svc.ForceForward(context.Background(), r.tenantID, r.instance.ID, uuid.New(), target, 1)
}

func TestForceForward_OpenTaskOnALaterPageOfTheTaskList_StillCountsAsOpen(t *testing.T) {
	rig := w7bNewForceRig(t, frfixPlanJSON(t, dsl.CurrentSchemaVersion, frfixParallelMain()))
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// The open task is the oldest, so it is on the last of two pages (50 and 10).
	rig.addTasks(1, "legal/review", domain.TaskStatusReady, start)
	rig.addTasks(59, "intake/open", domain.TaskStatusCompleted, start.Add(time.Hour))

	err := rig.forward("archive/file")

	require.ErrorIs(t, err, port.ErrTargetNodeNotFound, "the open parallel step is only seen when the second page is read")
	assert.Empty(t, rig.signalled())
	require.Len(t, rig.tasks.pages, 2)
	assert.Equal(t, 50, rig.tasks.pages[0].Limit)
	assert.Nil(t, rig.tasks.pages[0].After)
	assert.Equal(t, 50, rig.tasks.pages[1].Limit)
	require.NotNil(t, rig.tasks.pages[1].After, "the second request carries the cursor of the first page")
	rig.requireListedForRig(t)

	rig.tasks.pages = nil
	require.NoError(t, rig.forward("finance/check"), "control: a branch of the open step is accepted")
	assert.Len(t, rig.signalled(), 1)
	assert.Equal(t, "finance/check", w7bSignalTarget(t, rig.signalled()[0]))
}

func w7bSignalTarget(t *testing.T, call signalCall) string {
	t.Helper()
	raw, err := json.Marshal(call.Payload)
	require.NoError(t, err)
	var wire struct{ TargetNodeKey string }
	require.NoError(t, json.Unmarshal(raw, &wire))
	return wire.TargetNodeKey
}

func TestForceForward_ListingTheInstancesTasksFails_ReturnsTheWrappedErrorAndSendsNoSignal(t *testing.T) {
	cases := []struct {
		name   string
		failAt int
	}{
		{name: "the first page fails", failAt: 1},
		{name: "the second page fails", failAt: 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w7bNewForceRig(t, frfixPlanJSON(t, dsl.CurrentSchemaVersion, frfixParallelMain()))
			rig.addTasks(60, "legal/review", domain.TaskStatusReady, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
			rig.tasks.failAt, rig.tasks.err = tc.failAt, assert.AnError

			err := rig.forward("legal/sign")

			require.ErrorIs(t, err, assert.AnError)
			assert.ErrorContains(t, err, "list tasks by instance: ")
			assert.NotErrorIs(t, err, port.ErrTargetNodeNotFound, "it is a read failure, not a refused target")
			assert.Empty(t, rig.signalled())
			assert.Len(t, rig.tasks.pages, tc.failAt)
			rig.requireListedForRig(t)
		})
	}
}

func TestForceForward_MainPlanNamesNoPlanOfTheCollaboration_RefusesTheTargetBeforeTheSignal(t *testing.T) {
	other := &dsl.CompiledPlan{Name: "other", Departments: []dsl.DepartmentDef{frfixDept("legal", "review")}}
	collab := dsl.CompiledCollaboration{SchemaVersion: dsl.CurrentSchemaVersion, MainPlan: "ghost", Plans: []*dsl.CompiledPlan{other}}
	raw, err := json.Marshal(collab)
	require.NoError(t, err)
	rig := w7bNewForceRig(t, string(raw))
	rig.addTasks(1, "legal/review", domain.TaskStatusReady, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	err = rig.forward("legal/review")

	require.ErrorIs(t, err, port.ErrTargetNodeNotFound, "a node of another plan is not a node of the main plan")
	assert.Empty(t, rig.signalled())
	assert.Empty(t, rig.tasks.pages, "the open tasks are not read once the plan has no main plan")
}
