package temporal_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"

	outboundtemporal "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// wkctTasks adds task creation to the world's task fake. Like the real
// repository, a second insert of the same id is refused with ErrAlreadyExists.
type wkctTasks struct {
	wkactTasks
}

func (r wkctTasks) Create(_ context.Context, task *domain.Task) error {
	r.w.record("Tasks.Create", task.TenantID, task.ID)
	if _, exists := r.w.tasks[task.ID]; exists {
		return domain.ErrAlreadyExists
	}
	task.RecordVersion = 1
	r.w.tasks[task.ID] = task
	return nil
}

// wkctAssignments refuses a second insert of one assignment id, as the real
// repository does.
type wkctAssignments struct {
	wkactAssignments
}

func (r wkctAssignments) Create(ctx context.Context, a *domain.TaskAssignment) error {
	if _, exists := r.w.assignments[a.ID]; exists {
		r.w.record("Assignments.Create", a.TenantID, a.TaskID)
		return domain.ErrAlreadyExists
	}
	return r.wkactAssignments.Create(ctx, a)
}

func wkctNewDeps(t *testing.T, w *wkactWorld) *outboundtemporal.Deps {
	t.Helper()
	deps, _ := wkactNewDeps(t, w)
	deps.Tasks = wkctTasks{wkactTasks{w: w}}
	deps.Assignments = wkctAssignments{wkactAssignments{w: w}}
	return deps
}

func wkctCreateInput(t *testing.T, inst *domain.Instance, tenant uuid.UUID, stage dsl.StageDef) port.CreateTaskInput {
	t.Helper()
	compiled, err := json.Marshal(stage)
	require.NoError(t, err)
	return port.CreateTaskInput{
		InstanceID: inst.ID.String(), TenantID: tenant.String(), NodeKey: "sales/review", CompiledNode: compiled,
		VisitCount: 1, IAMDepartmentID: uuid.NewString(),
	}
}

func requireValidationError(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	var appErr *temporal.ApplicationError
	require.ErrorAs(t, err, &appErr)
	assert.Equal(t, "ValidationError", appErr.Type())
	assert.True(t, appErr.NonRetryable())
}

func TestCreateTask_ReplayAfterCommit_InsertsAndAnnouncesNothingMore(t *testing.T) {
	t.Parallel()
	world := wkactNewWorld()
	deps := wkctNewDeps(t, world)
	tenant := uuid.New()
	inst := world.addInstance(tenant, domain.InstanceStatusRunning)
	assignee := uuid.NewString()
	in := wkctCreateInput(t, inst, tenant, dsl.StageDef{Type: "review", DefaultAssignees: []string{assignee}})

	first, err := deps.CreateTask(context.Background(), in)
	require.NoError(t, err)
	second, err := deps.CreateTask(context.Background(), in)
	require.NoError(t, err)

	assert.Equal(t, first.TaskID, second.TaskID, "the replay answers with the task the first attempt made")
	assert.Len(t, world.callsTo("Tasks.Create"), 1, "the replay never inserts the same id again, because that aborts the transaction")
	assert.Len(t, world.callsTo("Assignments.Create"), 1)
	assert.Len(t, world.eventsOfType(domain.EventWorkflowTaskCreated), 1)
	world.requireAllCallsForTenant(t, tenant)
}

func TestCreateTask_TheSameAssigneeListedTwice_GetsOneAssignment(t *testing.T) {
	t.Parallel()
	world := wkactNewWorld()
	deps := wkctNewDeps(t, world)
	tenant := uuid.New()
	inst := world.addInstance(tenant, domain.InstanceStatusRunning)
	alice, bob := uuid.New(), uuid.New()
	in := wkctCreateInput(t, inst, tenant, dsl.StageDef{
		Type: "review", DefaultAssignees: []string{alice.String(), bob.String(), alice.String()},
	})

	out, err := deps.CreateTask(context.Background(), in)

	require.NoError(t, err)
	var users []uuid.UUID
	for _, a := range world.assignments {
		assert.Equal(t, uuid.MustParse(out.TaskID), a.TaskID)
		users = append(users, a.UserID)
	}
	assert.ElementsMatch(t, []uuid.UUID{alice, bob}, users)
	assert.Len(t, world.callsTo("Assignments.Create"), 2, "one insert per distinct person")
	created := world.eventsOfType(domain.EventWorkflowTaskCreated)
	require.Len(t, created, 1)
	var payload struct {
		AssigneeUserIDs []uuid.UUID `json:"assignee_user_ids"`
	}
	require.NoError(t, json.Unmarshal(created[0].Payload, &payload))
	assert.Equal(t, []uuid.UUID{alice, bob}, payload.AssigneeUserIDs, "the event lists each person once, in the order given")
	assert.Equal(t, "all", world.tasks[uuid.MustParse(out.TaskID)].AssigneeMode)
}

func TestCreateTask_TheSameAssigneeTwiceOnly_IsASingleAssigneeTask(t *testing.T) {
	t.Parallel()
	world := wkactNewWorld()
	deps := wkctNewDeps(t, world)
	tenant := uuid.New()
	inst := world.addInstance(tenant, domain.InstanceStatusRunning)
	alice := uuid.NewString()
	in := wkctCreateInput(t, inst, tenant, dsl.StageDef{Type: "review", DefaultAssignees: []string{alice, alice}})

	out, err := deps.CreateTask(context.Background(), in)

	require.NoError(t, err)
	assert.Equal(t, "single", world.tasks[uuid.MustParse(out.TaskID)].AssigneeMode)
	assert.Len(t, world.callsTo("Assignments.Create"), 1)
}

func TestCreateTask_UnreadableDatesOrDepartment_AreNonRetryableValidationErrors(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		stage  dsl.StageDef
		mutate func(in *port.CreateTaskInput)
	}{
		{name: "due date", stage: dsl.StageDef{Type: "review", DueDate: "next tuesday"}},
		{name: "follow-up date", stage: dsl.StageDef{Type: "review", FollowUpDate: "2026-13-45T00:00:00Z"}},
		{name: "department id", stage: dsl.StageDef{Type: "review"}, mutate: func(in *port.CreateTaskInput) { in.IAMDepartmentID = "not-a-uuid" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			world := wkactNewWorld()
			deps := wkctNewDeps(t, world)
			tenant := uuid.New()
			inst := world.addInstance(tenant, domain.InstanceStatusRunning)
			in := wkctCreateInput(t, inst, tenant, tc.stage)
			if tc.mutate != nil {
				tc.mutate(&in)
			}

			_, err := deps.CreateTask(context.Background(), in)

			requireValidationError(t, err)
			assert.Empty(t, world.tasks, "no task is created without its SLA dates or department")
			assert.Empty(t, world.outbox)
		})
	}
}

func TestCreateTask_ReadableDates_BecomeTheTaskSLA(t *testing.T) {
	t.Parallel()
	world := wkactNewWorld()
	deps := wkctNewDeps(t, world)
	tenant := uuid.New()
	inst := world.addInstance(tenant, domain.InstanceStatusRunning)
	in := wkctCreateInput(t, inst, tenant, dsl.StageDef{
		Type: "review", DueDate: "2026-01-02T03:04:05Z", FollowUpDate: "2026-01-01T00:00:00Z",
	})

	out, err := deps.CreateTask(context.Background(), in)

	require.NoError(t, err)
	task := world.tasks[uuid.MustParse(out.TaskID)]
	require.NotNil(t, task.DueAt)
	require.NotNil(t, task.FollowUpAt)
	assert.Equal(t, "2026-01-02T03:04:05Z", task.DueAt.UTC().Format("2006-01-02T15:04:05Z07:00"))
	assert.Equal(t, "2026-01-01T00:00:00Z", task.FollowUpAt.UTC().Format("2006-01-02T15:04:05Z07:00"))
}

func TestCreateTask_CreatedEventSubjectNamesTheInstanceAndTheTask(t *testing.T) {
	t.Parallel()
	world := wkactNewWorld()
	deps := wkctNewDeps(t, world)
	tenant := uuid.New()
	inst := world.addInstance(tenant, domain.InstanceStatusRunning)
	in := wkctCreateInput(t, inst, tenant, dsl.StageDef{Type: "review", DefaultAssignees: []string{uuid.NewString()}})

	out, err := deps.CreateTask(context.Background(), in)

	require.NoError(t, err)
	created := world.eventsOfType(domain.EventWorkflowTaskCreated)
	require.Len(t, created, 1)
	assert.Equal(t, "instances/"+inst.ID.String()+"/tasks/"+out.TaskID, created[0].Subject)
}
