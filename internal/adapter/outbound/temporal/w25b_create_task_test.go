package temporal_test

import (
	"context"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

const w25bNodeKey = "sales/review"

func w25bCreateInput(t *testing.T, inst *domain.Instance, tenant uuid.UUID, stage dsl.StageDef) port.CreateTaskInput {
	t.Helper()
	compiled, err := json.Marshal(stage)
	require.NoError(t, err)
	return port.CreateTaskInput{
		InstanceID: inst.ID.String(), TenantID: tenant.String(), NodeKey: w25bNodeKey, CompiledNode: compiled,
		VisitCount: 1, IAMDepartmentID: uuid.NewString(),
	}
}

// w25bRequireNothingTried checks that the activity refused its input before it
// opened a transaction or called any repository.
func w25bRequireNothingTried(t *testing.T, rig *w25bRig) {
	t.Helper()
	assert.Empty(t, rig.txOpened, "no transaction is opened for an input that is refused up front")
	assert.Empty(t, rig.calls, "no repository is called for an input that is refused up front")
	assert.Empty(t, rig.tasks)
	assert.Empty(t, rig.assignments)
	assert.Empty(t, rig.outbox)
}

// w25bRawEventField returns one top-level field of an enqueued payload as raw
// JSON, so a test can tell [] from null.
func w25bRawEventField(t *testing.T, rig *w25bRig, eventType, field string) json.RawMessage {
	t.Helper()
	created := rig.eventsOfType(eventType)
	require.Len(t, created, 1)
	fields := map[string]json.RawMessage{}
	require.NoError(t, json.Unmarshal(created[0].Payload, &fields))
	return fields[field]
}

func w25bAssignmentsOf(rig *w25bRig, taskID uuid.UUID) []*domain.TaskAssignment {
	var out []*domain.TaskAssignment
	for _, id := range rig.assignmentOrder {
		if a := rig.assignments[id]; a.TaskID == taskID {
			out = append(out, a)
		}
	}
	return out
}

func w25bUserIDsOf(assignments []*domain.TaskAssignment) []uuid.UUID {
	out := make([]uuid.UUID, 0, len(assignments))
	for _, a := range assignments {
		out = append(out, a.UserID)
	}
	return out
}

func TestCreateTask_UnusableInstanceOrCompiledNode_IsRefusedBeforeAnyTransaction(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		mutate      func(in *port.CreateTaskInput)
		messagePart string
	}{
		{name: "instance id is not a uuid", mutate: func(in *port.CreateTaskInput) { in.InstanceID = "not-a-uuid" }, messagePart: "parse instance_id"},
		{name: "compiled node is empty", mutate: func(in *port.CreateTaskInput) { in.CompiledNode = nil }, messagePart: "unmarshal compiled node"},
		{name: "compiled node is not json", mutate: func(in *port.CreateTaskInput) { in.CompiledNode = json.RawMessage(`not-json`) }, messagePart: "unmarshal compiled node"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rig := w25bNewRig()
			deps := w25bNewDeps(t, rig)
			tenant := uuid.New()
			inst := rig.addInstance(tenant, domain.InstanceStatusRunning)
			in := w25bCreateInput(t, inst, tenant, dsl.StageDef{Type: "review"})
			tc.mutate(&in)

			_, err := deps.CreateTask(context.Background(), in)

			_ = w25bRequireNonRetryable(t, err, "ValidationError", tc.messagePart)
			w25bRequireNothingTried(t, rig)
		})
	}
}

func TestCreateTask_HumanStageWithOneAssignee_CreatesAReadySingleTaskWithItsAssignmentAndEvent(t *testing.T) {
	t.Parallel()
	rig := w25bNewRig()
	deps := w25bNewDeps(t, rig)
	tenant := uuid.New()
	inst := rig.addInstance(tenant, domain.InstanceStatusRunning)
	user := uuid.New()
	in := w25bCreateInput(t, inst, tenant, dsl.StageDef{
		Type: "review", DefaultAssignees: []string{user.String()},
		DueDate: "2026-12-01T10:00:00Z", FollowUpDate: "2026-11-30T09:00:00Z",
	})
	in.VisitCount = 4

	out, err := deps.CreateTask(context.Background(), in)

	require.NoError(t, err)
	taskID := uuid.MustParse(out.TaskID)
	task := rig.tasks[taskID]
	require.NotNil(t, task)
	assert.Equal(t, domain.TaskStatusReady, task.Status)
	assert.Equal(t, "single", task.AssigneeMode)
	assert.Equal(t, int64(4), task.VisitCount)
	assert.Equal(t, w25bNodeKey, task.NodeKey)
	assert.Equal(t, inst.ID, task.WorkflowInstanceID)
	assert.Equal(t, tenant, task.TenantID)
	assert.Equal(t, in.IAMDepartmentID, task.DepartmentID.String())
	require.NotNil(t, task.DueAt)
	require.NotNil(t, task.FollowUpAt)
	assert.True(t, task.DueAt.Equal(time.Date(2026, 12, 1, 10, 0, 0, 0, time.UTC)))
	assert.True(t, task.FollowUpAt.Equal(time.Date(2026, 11, 30, 9, 0, 0, 0, time.UTC)))

	held := w25bAssignmentsOf(rig, taskID)
	require.Len(t, held, 1)
	assert.Equal(t, user, held[0].UserID)
	assert.Equal(t, tenant, held[0].TenantID)
	assert.True(t, held[0].IsActive)
	assert.NotEqual(t, uuid.Nil, held[0].ID)
	assert.NotEqual(t, taskID, held[0].ID)

	created := rig.eventsOfType(domain.EventWorkflowTaskCreated)
	require.Len(t, created, 1)
	assert.Equal(t, tenant.String(), created[0].TenantID)
	payload := w25bDecode[domain.WorkflowTaskCreatedPayload](t, created[0])
	assert.Equal(t, taskID, payload.TaskID)
	assert.Equal(t, w25bNodeKey, payload.NodeKey)
	assert.Equal(t, []uuid.UUID{user}, payload.AssigneeUserIDs)
	assert.Equal(t, "review", payload.StageType)
	require.NotNil(t, payload.DueAt)
	require.NotNil(t, payload.FollowUpAt)
	assert.True(t, payload.DueAt.Equal(*task.DueAt))
	assert.True(t, payload.FollowUpAt.Equal(*task.FollowUpAt))
	rig.requireEveryCallScopedTo(t, tenant)

	// A retry of the same call names the same task and writes nothing more.
	before := rig.snapshot()
	again, err := deps.CreateTask(context.Background(), in)
	require.NoError(t, err)
	assert.Equal(t, out.TaskID, again.TaskID)
	w25bRequireUntouched(t, rig, before)
	assert.Len(t, rig.callsTo("Tasks.Create"), 1)
	assert.Len(t, rig.callsTo("Assignments.Create"), 1)
	assert.Len(t, rig.callsTo("Outbox.Enqueue"), 1)
}

func TestCreateTask_TheSameInstanceNodeAndVisit_NameTheSameTaskAndAssignmentsWhateverElseDiffers(t *testing.T) {
	t.Parallel()
	instanceID, userOne, userTwo := uuid.New(), uuid.New(), uuid.New()

	run := func(t *testing.T, tenant uuid.UUID, stage dsl.StageDef, contextJSON string) (uuid.UUID, map[uuid.UUID]uuid.UUID) {
		t.Helper()
		rig := w25bNewRig()
		rig.instances[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenant, Status: domain.InstanceStatusRunning, RecordVersion: 1}
		deps := w25bNewDeps(t, rig)
		in := w25bCreateInput(t, rig.instances[instanceID], tenant, stage)
		in.ContextJSON = contextJSON
		out, err := deps.CreateTask(context.Background(), in)
		require.NoError(t, err)
		taskID := uuid.MustParse(out.TaskID)
		byUser := map[uuid.UUID]uuid.UUID{}
		for _, a := range w25bAssignmentsOf(rig, taskID) {
			byUser[a.UserID] = a.ID
		}
		return taskID, byUser
	}

	firstTask, firstAssignments := run(t, uuid.New(),
		dsl.StageDef{Type: "review", DefaultAssignees: []string{userOne.String(), userTwo.String()}, DueDate: "2026-12-01T10:00:00Z"}, `{"a":1}`)
	secondTask, secondAssignments := run(t, uuid.New(),
		dsl.StageDef{Type: "approve", DefaultAssignees: []string{userTwo.String(), userOne.String()}, DueDate: "2027-01-01T00:00:00Z"}, `{"b":2}`)

	require.Len(t, firstAssignments, 2)
	assert.Equal(t, firstTask, secondTask, "another tenant, stage type, due date, context and assignee order name the same task")
	assert.Equal(t, firstAssignments, secondAssignments, "each user's assignment id stays with the task and the user")
}

func TestCreateTask_TwoAssignees_MakeAnAllTaskWithOneDistinctAssignmentEach(t *testing.T) {
	t.Parallel()
	rig := w25bNewRig()
	deps := w25bNewDeps(t, rig)
	tenant := uuid.New()
	inst := rig.addInstance(tenant, domain.InstanceStatusRunning)
	users := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	// Descending order, so an event that lists them sorted would show.
	slices.SortFunc(users, func(a, b uuid.UUID) int { return -compareStrings(a.String(), b.String()) })
	in := w25bCreateInput(t, inst, tenant, dsl.StageDef{
		Type: "review", DefaultAssignees: []string{users[0].String(), users[1].String(), users[2].String()},
	})

	out, err := deps.CreateTask(context.Background(), in)

	require.NoError(t, err)
	taskID := uuid.MustParse(out.TaskID)
	assert.Equal(t, "all", rig.tasks[taskID].AssigneeMode)
	held := w25bAssignmentsOf(rig, taskID)
	require.Len(t, held, 3)
	assert.Equal(t, users, w25bUserIDsOf(held))
	ids := map[uuid.UUID]bool{}
	for _, a := range held {
		ids[a.ID] = true
		assert.Equal(t, taskID, a.TaskID)
		assert.True(t, a.IsActive)
	}
	assert.Len(t, ids, 3, "each assignee gets an assignment of their own")
	created := rig.eventsOfType(domain.EventWorkflowTaskCreated)
	require.Len(t, created, 1)
	assert.Equal(t, users, w25bDecode[domain.WorkflowTaskCreatedPayload](t, created[0]).AssigneeUserIDs)

	before := rig.snapshot()
	_, err = deps.CreateTask(context.Background(), in)
	require.NoError(t, err)
	w25bRequireUntouched(t, rig, before)
	assert.Len(t, rig.callsTo("Assignments.Create"), 3, "the retry creates no assignment")
}

func TestCreateTask_OverrideForTheNodeKey_ReplacesTheDefaultAssignees(t *testing.T) {
	t.Parallel()
	defaultOne, defaultTwo, overrideUser := uuid.New(), uuid.New(), uuid.New()
	defaults := []uuid.UUID{defaultOne, defaultTwo}

	cases := []struct {
		name         string
		overrideMap  map[string]string
		wantUsers    []uuid.UUID
		wantAssignee string
	}{
		{name: "override for this node replaces both defaults", overrideMap: map[string]string{w25bNodeKey: overrideUser.String()}, wantUsers: []uuid.UUID{overrideUser}, wantAssignee: "single"},
		{name: "empty override keeps the defaults", overrideMap: map[string]string{w25bNodeKey: ""}, wantUsers: defaults, wantAssignee: "all"},
		{name: "override for another node keeps the defaults", overrideMap: map[string]string{"sales/approve": overrideUser.String()}, wantUsers: defaults, wantAssignee: "all"},
		{name: "no override map keeps the defaults", overrideMap: nil, wantUsers: defaults, wantAssignee: "all"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rig := w25bNewRig()
			deps := w25bNewDeps(t, rig)
			tenant := uuid.New()
			inst := rig.addInstance(tenant, domain.InstanceStatusRunning)
			in := w25bCreateInput(t, inst, tenant, dsl.StageDef{
				Type: "review", DefaultAssignees: []string{defaultOne.String(), defaultTwo.String()},
			})
			in.OverrideMap = tc.overrideMap

			out, err := deps.CreateTask(context.Background(), in)

			require.NoError(t, err)
			taskID := uuid.MustParse(out.TaskID)
			assert.Equal(t, tc.wantAssignee, rig.tasks[taskID].AssigneeMode)
			assert.Equal(t, tc.wantUsers, w25bUserIDsOf(w25bAssignmentsOf(rig, taskID)))
			created := rig.eventsOfType(domain.EventWorkflowTaskCreated)
			require.Len(t, created, 1)
			assert.Equal(t, tc.wantUsers, w25bDecode[domain.WorkflowTaskCreatedPayload](t, created[0]).AssigneeUserIDs)

			before := rig.snapshot()
			_, err = deps.CreateTask(context.Background(), in)
			require.NoError(t, err)
			w25bRequireUntouched(t, rig, before)
		})
	}
}

func TestCreateTask_AssigneeThatIsNotAUUID_IsRefusedBeforeTheTransaction(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		defaults    []string
		overrideMap map[string]string
		badValue    string
	}{
		{name: "default assignee", defaults: []string{uuid.NewString(), "ann@example.com"}, badValue: "ann@example.com"},
		{name: "override value", defaults: []string{uuid.NewString()}, overrideMap: map[string]string{w25bNodeKey: "not-a-user"}, badValue: "not-a-user"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rig := w25bNewRig()
			deps := w25bNewDeps(t, rig)
			tenant := uuid.New()
			inst := rig.addInstance(tenant, domain.InstanceStatusRunning)
			in := w25bCreateInput(t, inst, tenant, dsl.StageDef{Type: "review", DefaultAssignees: tc.defaults})
			in.OverrideMap = tc.overrideMap

			_, err := deps.CreateTask(context.Background(), in)

			_ = w25bRequireNonRetryable(t, err, "ValidationError", "parse assignee user_id")
			_ = w25bRequireNonRetryable(t, err, "ValidationError", tc.badValue)
			w25bRequireNothingTried(t, rig)
		})
	}
}

func TestCreateTask_ConnectorStageInputs_AreAnObjectEvenWhenThereIsNothingToMap(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		mapping     *dsl.IOMapping
		contextJSON string
		wantInputs  string
		wantOutputs string
	}{
		{name: "no io mapping", mapping: nil, contextJSON: `{"a":"x"}`, wantInputs: `{}`, wantOutputs: `null`},
		{
			name:        "empty context reads variables as unset",
			mapping:     &dsl.IOMapping{Inputs: []dsl.IOVar{{Source: "=who", Target: "to"}, {Source: "hello", Target: "subject"}}},
			contextJSON: "", wantInputs: `{"subject":"hello","to":null}`, wantOutputs: `null`,
		},
		{
			name:        "null context reads variables as unset",
			mapping:     &dsl.IOMapping{Inputs: []dsl.IOVar{{Source: "=who", Target: "to"}}},
			contextJSON: "null", wantInputs: `{"to":null}`, wantOutputs: `null`,
		},
		{
			name:        "the last input for one target wins",
			mapping:     &dsl.IOMapping{Inputs: []dsl.IOVar{{Source: "=first", Target: "to"}, {Source: "=second", Target: "to"}}},
			contextJSON: `{"first":"a@example.com","second":"b@example.com"}`, wantInputs: `{"to":"b@example.com"}`, wantOutputs: `null`,
		},
		{
			name:        "outputs are carried as the output mapping",
			mapping:     &dsl.IOMapping{Outputs: []dsl.IOVar{{Source: "=messageId", Target: "mail_id"}}},
			contextJSON: `{}`, wantInputs: `{}`, wantOutputs: `[{"source":"=messageId","target":"mail_id"}]`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rig := w25bNewRig()
			deps := w25bNewDeps(t, rig)
			tenant := uuid.New()
			inst := rig.addInstance(tenant, domain.InstanceStatusRunning)
			in := w25bCreateInput(t, inst, tenant, dsl.StageDef{Type: "connector", ConnectorType: "send-email", IOMapping: tc.mapping})
			in.ContextJSON = tc.contextJSON

			out, err := deps.CreateTask(context.Background(), in)

			require.NoError(t, err)
			task := rig.tasks[uuid.MustParse(out.TaskID)]
			require.NotNil(t, task)
			assert.Equal(t, domain.TaskStatusReady, task.Status)
			require.NotNil(t, task.ConnectorType)
			assert.Equal(t, "send-email", *task.ConnectorType)
			extras := map[string]json.RawMessage{}
			require.NoError(t, json.Unmarshal(task.ExtrasJSON, &extras))
			assert.JSONEq(t, tc.wantInputs, string(extras["resolved_inputs"]))
			assert.Equal(t, tc.wantOutputs, string(extras["output_mapping"]))
			assert.Empty(t, rig.assignments)
			assert.Len(t, rig.eventsOfType(domain.EventWorkflowTaskCreated), 1)
			rig.requireEveryCallScopedTo(t, tenant)

			before := rig.snapshot()
			_, err = deps.CreateTask(context.Background(), in)
			require.NoError(t, err)
			w25bRequireUntouched(t, rig, before)
		})
	}
}

func TestCreateTask_ConnectorStageContextThatIsNotAnObject_IsRefusedBeforeTheTransaction(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		contextJSON string
	}{
		{name: "not json", contextJSON: "not-json"},
		{name: "an array", contextJSON: "[1,2]"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rig := w25bNewRig()
			deps := w25bNewDeps(t, rig)
			tenant := uuid.New()
			inst := rig.addInstance(tenant, domain.InstanceStatusRunning)
			in := w25bCreateInput(t, inst, tenant, dsl.StageDef{
				Type: "connector", ConnectorType: "send-email",
				IOMapping: &dsl.IOMapping{Inputs: []dsl.IOVar{{Source: "=who", Target: "to"}}},
			})
			in.ContextJSON = tc.contextJSON

			_, err := deps.CreateTask(context.Background(), in)

			_ = w25bRequireNonRetryable(t, err, "ValidationError", "parsing context_json for connector io_mapping")
			w25bRequireNothingTried(t, rig)
		})
	}
}

func TestCreateTask_ConnectorStageWithAssignees_IgnoresThemWithoutValidatingThem(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		defaults    []string
		overrideMap map[string]string
	}{
		{name: "valid default assignees", defaults: []string{uuid.NewString(), uuid.NewString()}},
		{name: "override entry", overrideMap: map[string]string{w25bNodeKey: uuid.NewString()}},
		{name: "default assignee that is not a uuid", defaults: []string{"not-a-user"}},
		{name: "override value that is not a uuid", overrideMap: map[string]string{w25bNodeKey: "not-a-user"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rig := w25bNewRig()
			deps := w25bNewDeps(t, rig)
			tenant := uuid.New()
			inst := rig.addInstance(tenant, domain.InstanceStatusRunning)
			in := w25bCreateInput(t, inst, tenant, dsl.StageDef{Type: "connector", ConnectorType: "send-email", DefaultAssignees: tc.defaults})
			in.OverrideMap = tc.overrideMap

			out, err := deps.CreateTask(context.Background(), in)

			require.NoError(t, err)
			assert.Equal(t, "single", rig.tasks[uuid.MustParse(out.TaskID)].AssigneeMode)
			assert.Empty(t, rig.assignments, "a connector task has no assignment rows")
			assert.Empty(t, rig.callsTo("Assignments.Create"))
			assert.Equal(t, "[]", string(w25bRawEventField(t, rig, domain.EventWorkflowTaskCreated, "assignee_user_ids")))
			rig.requireEveryCallScopedTo(t, tenant)

			before := rig.snapshot()
			_, err = deps.CreateTask(context.Background(), in)
			require.NoError(t, err)
			w25bRequireUntouched(t, rig, before)
		})
	}
}

func TestCreateTask_TasksCreateFails_IsAPlainWrappedErrorThatRollsBackAndTheRetryCreatesTheTask(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		cause error
	}{
		{name: "storage failure", cause: errW25bStorage},
		{name: "primary key conflict after the look-up found nothing", cause: domain.ErrAlreadyExists},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rig := w25bNewRig()
			deps := w25bNewDeps(t, rig)
			tenant := uuid.New()
			inst := rig.addInstance(tenant, domain.InstanceStatusRunning)
			in := w25bCreateInput(t, inst, tenant, dsl.StageDef{Type: "review", DefaultAssignees: []string{uuid.NewString()}})
			rig.failOn("Tasks.Create", tc.cause, 1)

			_, err := deps.CreateTask(context.Background(), in)

			w25bRequireRetryable(t, err)
			require.ErrorIs(t, err, tc.cause)
			assert.Contains(t, err.Error(), "create task: create workflow_task: ")
			assert.Equal(t, 1, rig.rolledBack)
			assert.Empty(t, rig.tasks)
			assert.Empty(t, rig.callsTo("Assignments.Create"), "no assignment is attempted after the task insert failed")
			assert.Empty(t, rig.callsTo("Outbox.Enqueue"))
			assert.Empty(t, rig.outbox)

			out, err := deps.CreateTask(context.Background(), in)

			require.NoError(t, err)
			assert.Len(t, rig.tasks, 1)
			assert.Contains(t, rig.tasks, uuid.MustParse(out.TaskID))
			assert.Len(t, rig.assignments, 1)
			assert.Len(t, rig.eventsOfType(domain.EventWorkflowTaskCreated), 1)
		})
	}
}

func TestCreateTask_AFailureAfterTheTaskInsert_RollsBackTheWholeTransaction(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		breakIt     func(rig *w25bRig)
		messagePart string
	}{
		{name: "assignment insert fails", breakIt: func(rig *w25bRig) { rig.failOn("Assignments.Create", errW25bStorage, 0) }, messagePart: "create workflow_task_assignment"},
		{name: "outbox enqueue fails", breakIt: func(rig *w25bRig) { rig.failOn("Outbox.Enqueue", errW25bStorage, 0) }, messagePart: "create task: w25b storage failure"},
		{name: "event is refused by the schema check", breakIt: nil, messagePart: "build workflow.task.created envelope"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rig := w25bNewRig()
			deps := w25bNewDeps(t, rig)
			tenant := uuid.New()
			inst := rig.addInstance(tenant, domain.InstanceStatusRunning)
			in := w25bCreateInput(t, inst, tenant, dsl.StageDef{Type: "review", DefaultAssignees: []string{uuid.NewString()}})
			if tc.breakIt != nil {
				tc.breakIt(rig)
			} else {
				w25bRejectEvents(deps, errW25bStorage)
			}

			_, err := deps.CreateTask(context.Background(), in)

			if tc.breakIt != nil {
				w25bRequireRetryable(t, err)
			} else {
				_ = w25bRequireNonRetryable(t, err, "ValidationError", tc.messagePart)
			}
			require.ErrorIs(t, err, errW25bStorage)
			assert.Contains(t, err.Error(), tc.messagePart)
			assert.Equal(t, 1, rig.rolledBack)
			assert.Empty(t, rig.tasks, "the task row goes with the failed transaction")
			assert.Empty(t, rig.assignments)
			assert.Empty(t, rig.outbox)

			delete(rig.failures, "Assignments.Create")
			delete(rig.failures, "Outbox.Enqueue")
			w25bRejectEvents(deps, nil)
			out, err := deps.CreateTask(context.Background(), in)
			require.NoError(t, err)
			assert.Contains(t, rig.tasks, uuid.MustParse(out.TaskID))
			assert.Len(t, rig.assignments, 1)
			assert.Len(t, rig.eventsOfType(domain.EventWorkflowTaskCreated), 1)
		})
	}
}

func TestCreateTask_DueAndFollowUpDates_AreRFC3339OrEmptyAndNothingElse(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		value    string
		wantNil  bool
		wantAt   time.Time
		wantZone int
		refused  bool
	}{
		{name: "zulu time", value: "2026-12-01T10:00:00Z", wantAt: time.Date(2026, 12, 1, 10, 0, 0, 0, time.UTC), wantZone: 0},
		{name: "offset is kept", value: "2026-12-01T10:00:00+02:00", wantAt: time.Date(2026, 12, 1, 8, 0, 0, 0, time.UTC), wantZone: 7200},
		{name: "empty means none", value: "", wantNil: true},
		{name: "a date alone is refused", value: "2026-12-01", refused: true},
		{name: "free text is refused", value: "tomorrow", refused: true},
	}
	fields := []struct {
		name  string
		stage func(v string) dsl.StageDef
		read  func(task *domain.Task) *time.Time
		label string
	}{
		{name: "due date", label: `parse due date "`, stage: func(v string) dsl.StageDef { return dsl.StageDef{Type: "review", DueDate: v} }, read: func(task *domain.Task) *time.Time { return task.DueAt }},
		{name: "follow-up date", label: `parse follow-up date "`, stage: func(v string) dsl.StageDef { return dsl.StageDef{Type: "review", FollowUpDate: v} }, read: func(task *domain.Task) *time.Time { return task.FollowUpAt }},
	}
	for _, field := range fields {
		for _, tc := range cases {
			t.Run(field.name+": "+tc.name, func(t *testing.T) {
				t.Parallel()
				rig := w25bNewRig()
				deps := w25bNewDeps(t, rig)
				tenant := uuid.New()
				inst := rig.addInstance(tenant, domain.InstanceStatusRunning)
				in := w25bCreateInput(t, inst, tenant, field.stage(tc.value))

				out, err := deps.CreateTask(context.Background(), in)

				if tc.refused {
					_ = w25bRequireNonRetryable(t, err, "ValidationError", field.label+tc.value+`"`)
					w25bRequireNothingTried(t, rig)
					return
				}
				require.NoError(t, err)
				got := field.read(rig.tasks[uuid.MustParse(out.TaskID)])
				if tc.wantNil {
					assert.Nil(t, got)
					return
				}
				require.NotNil(t, got)
				assert.True(t, got.Equal(tc.wantAt), "got %s", got)
				_, offset := got.Zone()
				assert.Equal(t, tc.wantZone, offset)
			})
		}
	}
}

func TestCreateTask_TaskAndAssignmentIds_DependOnTheirInputsAndNothingElse(t *testing.T) {
	t.Parallel()
	tenant, instanceID, otherInstanceID := uuid.New(), uuid.New(), uuid.New()
	userOne, userTwo := uuid.New(), uuid.New()

	type created struct {
		task        uuid.UUID
		assignments map[uuid.UUID]uuid.UUID
	}
	run := func(t *testing.T, instance uuid.UUID, nodeKey string, visit int64, users ...uuid.UUID) created {
		t.Helper()
		rig := w25bNewRig()
		rig.instances[instance] = &domain.Instance{ID: instance, TenantID: tenant, Status: domain.InstanceStatusRunning, RecordVersion: 1}
		deps := w25bNewDeps(t, rig)
		assignees := make([]string, len(users))
		for i, u := range users {
			assignees[i] = u.String()
		}
		in := w25bCreateInput(t, rig.instances[instance], tenant, dsl.StageDef{Type: "review", DefaultAssignees: assignees})
		in.NodeKey = domain.NodeKey(nodeKey)
		in.VisitCount = visit
		out, err := deps.CreateTask(context.Background(), in)
		require.NoError(t, err)
		taskID := uuid.MustParse(out.TaskID)
		byUser := map[uuid.UUID]uuid.UUID{}
		for _, a := range w25bAssignmentsOf(rig, taskID) {
			byUser[a.UserID] = a.ID
		}
		return created{task: taskID, assignments: byUser}
	}

	base := run(t, instanceID, "sales/review", 1, userOne, userTwo)

	same := run(t, instanceID, "sales/review", 1, userOne, userTwo)
	assert.Equal(t, base.task, same.task, "the same inputs name the same task")
	assert.Equal(t, base.assignments, same.assignments, "the same task and user name the same assignment")

	assert.NotEqual(t, base.task, run(t, instanceID, "sales/approve", 1, userOne).task, "another node key is another task")
	assert.NotEqual(t, base.task, run(t, otherInstanceID, "sales/review", 1, userOne).task, "another instance is another task")
	assert.NotEqual(t, base.task, run(t, instanceID, "sales/review", 2, userOne).task, "another visit is another task")

	assert.NotEqual(t, base.assignments[userOne], base.assignments[userTwo], "another user on the same task is another assignment")
	assert.Equal(t, base.assignments[userOne], run(t, instanceID, "sales/review", 1, userOne).assignments[userOne], "who else holds the task does not change an assignment's id")
	assert.NotEqual(t, base.assignments[userOne], run(t, instanceID, "sales/review", 2, userOne).assignments[userOne], "the same user on another task is another assignment")
}
