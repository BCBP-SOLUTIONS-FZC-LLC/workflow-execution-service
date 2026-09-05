package temporal_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	outboundtemporal "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

// permissiveInstanceRepo auto-vivifies a zero-value Instance for any
// unregistered ID, so CreateTask's own GetByID call (needed for the outbound
// event's BusinessKey/WorkflowVersionID) doesn't require every test above to
// pre-register an instance it otherwise doesn't care about. Tests that do
// care register a real one via Create/byID first.
type permissiveInstanceRepo struct {
	*fakeInstanceRepo
}

func newPermissiveInstanceRepo() *permissiveInstanceRepo {
	return &permissiveInstanceRepo{fakeInstanceRepo: newFakeInstanceRepo()}
}

func (r *permissiveInstanceRepo) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	if inst, ok := r.byID[id]; ok {
		return inst, nil
	}
	return &domain.Instance{ID: id}, nil
}

func newTestDeps() (*outboundtemporal.Deps, *fakeTaskRepo, *fakeAssignmentRepo, *fakeOutbox) {
	tasks := newFakeTaskRepo()
	assignments := newFakeAssignmentRepo()
	outbox := &fakeOutbox{}
	deps := &outboundtemporal.Deps{
		Tasks:       tasks,
		Assignments: assignments,
		Outbox:      outbox,
		Instances:   newPermissiveInstanceRepo(),
		Transactor:  fakeTransactor{},
		Validator:   noopValidator{},
	}
	return deps, tasks, assignments, outbox
}

func TestCreateTask_ConnectorStage_ResolvesInputsNoAssignments(t *testing.T) {
	deps, tasks, assignments, outbox := newTestDeps()

	stage := dsl.StageDef{
		Type: "connector", ConnectorType: "send-email",
		IOMapping: &dsl.IOMapping{Inputs: []dsl.IOVar{{Source: "applicantEmail", Target: "to"}}},
	}
	compiled, err := json.Marshal(stage)
	require.NoError(t, err)

	out, err := deps.CreateTask(context.Background(), port.CreateTaskInput{
		InstanceID: uuid.New().String(), TenantID: uuid.New().String(), NodeKey: "sales/connector",
		CompiledNode: compiled, ContextJSON: `{"applicantEmail":"a@example.com"}`,
	})
	require.NoError(t, err)

	taskID, err := uuid.Parse(out.TaskID)
	require.NoError(t, err)
	task := tasks.byID[taskID]
	require.NotNil(t, task)
	require.NotNil(t, task.ConnectorType)
	assert.Equal(t, "send-email", *task.ConnectorType)
	assert.Empty(t, assignments.byID, "connector tasks get zero assignment rows")

	var extras struct {
		ResolvedInputs map[string]any `json:"resolved_inputs"`
	}
	require.NoError(t, json.Unmarshal(task.ExtrasJSON, &extras))
	assert.Equal(t, "a@example.com", extras.ResolvedInputs["to"])

	require.Len(t, outbox.enqueued, 1)
	assert.Equal(t, domain.EventWorkflowTaskCreated, outbox.enqueued[0].Type)
}

func TestCreateTask_NonConnectorStage_CreatesAssignmentsFromDefaultAssignees(t *testing.T) {
	deps, _, assignments, _ := newTestDeps()
	user1, user2 := uuid.New(), uuid.New()

	stage := dsl.StageDef{Type: "review", DefaultAssignees: []string{user1.String(), user2.String()}}
	compiled, err := json.Marshal(stage)
	require.NoError(t, err)

	out, err := deps.CreateTask(context.Background(), port.CreateTaskInput{
		InstanceID: uuid.New().String(), TenantID: uuid.New().String(), NodeKey: "sales/review", CompiledNode: compiled,
	})
	require.NoError(t, err)

	taskID, err := uuid.Parse(out.TaskID)
	require.NoError(t, err)
	var gotUserIDs []uuid.UUID
	for _, a := range assignments.byID {
		if a.TaskID == taskID {
			gotUserIDs = append(gotUserIDs, a.UserID)
		}
	}
	assert.ElementsMatch(t, []uuid.UUID{user1, user2}, gotUserIDs)
}

func TestCreateTask_OverrideMapReplacesDefaultAssignee(t *testing.T) {
	deps, _, assignments, _ := newTestDeps()
	defaultUser, overrideUser := uuid.New(), uuid.New()

	stage := dsl.StageDef{Type: "review", DefaultAssignees: []string{defaultUser.String()}}
	compiled, err := json.Marshal(stage)
	require.NoError(t, err)

	nodeKey := domain.NodeKey("sales/review")
	out, err := deps.CreateTask(context.Background(), port.CreateTaskInput{
		InstanceID: uuid.New().String(), TenantID: uuid.New().String(), NodeKey: nodeKey, CompiledNode: compiled,
		OverrideMap: map[string]string{string(nodeKey): overrideUser.String()},
	})
	require.NoError(t, err)

	taskID, err := uuid.Parse(out.TaskID)
	require.NoError(t, err)
	var gotUserIDs []uuid.UUID
	for _, a := range assignments.byID {
		if a.TaskID == taskID {
			gotUserIDs = append(gotUserIDs, a.UserID)
		}
	}
	assert.Equal(t, []uuid.UUID{overrideUser}, gotUserIDs)
}

// TestCreateTask_RetriedCall_IsIdempotent is the regression test for the
// missing-idempotency-key finding: task.ID is now deterministic
// (instanceID+NodeKey), so an at-least-once Temporal retry of the exact same
// input — simulating a lost ack after the first attempt's commit succeeded —
// must resolve to the same task, not a second real row and a duplicate
// workflow.task.created event.
func TestCreateTask_RetriedCall_IsIdempotent(t *testing.T) {
	deps, tasks, assignments, outbox := newTestDeps()
	stage := dsl.StageDef{Type: "review", DefaultAssignees: []string{uuid.New().String()}}
	compiled, err := json.Marshal(stage)
	require.NoError(t, err)

	in := port.CreateTaskInput{
		InstanceID: uuid.New().String(), TenantID: uuid.New().String(), NodeKey: "sales/review", CompiledNode: compiled,
	}

	out1, err := deps.CreateTask(context.Background(), in)
	require.NoError(t, err)

	out2, err := deps.CreateTask(context.Background(), in)
	require.NoError(t, err, "a retried CreateTask must succeed idempotently, not error")

	assert.Equal(t, out1.TaskID, out2.TaskID, "retry must resolve to the same deterministic task ID")
	assert.Len(t, tasks.byID, 1, "retry must not create a second workflow_task row")
	assert.Len(t, assignments.byID, 1, "retry must not create a second assignment row")
	assert.Len(t, outbox.enqueued, 1, "retry must not re-enqueue workflow.task.created")
}

// TestCreateTask_DifferentVisitCount_CreatesDistinctTask is the regression
// test for a real bug found reviewing the retry-idempotency fix above: a
// legitimate revisit of the same NodeKey (an exclusive-gateway back-edge or
// admin force-back, internal/workflow's own VisitCount counter incrementing)
// must NOT be treated as a retry of the same call — it needs its own real
// task, not a silent no-op against the first visit's row.
func TestCreateTask_DifferentVisitCount_CreatesDistinctTask(t *testing.T) {
	deps, tasks, _, outbox := newTestDeps()
	stage := dsl.StageDef{Type: "review", DefaultAssignees: []string{uuid.New().String()}}
	compiled, err := json.Marshal(stage)
	require.NoError(t, err)

	instanceID := uuid.New().String()
	firstVisit := port.CreateTaskInput{
		InstanceID: instanceID, TenantID: uuid.New().String(), NodeKey: "sales/review", CompiledNode: compiled, VisitCount: 1,
	}
	secondVisit := firstVisit
	secondVisit.VisitCount = 2

	out1, err := deps.CreateTask(context.Background(), firstVisit)
	require.NoError(t, err)
	out2, err := deps.CreateTask(context.Background(), secondVisit)
	require.NoError(t, err, "a genuine revisit (different VisitCount) must succeed, not be swallowed as an idempotent retry")

	assert.NotEqual(t, out1.TaskID, out2.TaskID, "a revisit must get its own distinct task ID, not collide with the first visit's")
	assert.Len(t, tasks.byID, 2, "a revisit must create a second real workflow_task row")
	assert.Len(t, outbox.enqueued, 2, "a revisit must enqueue its own workflow.task.created, not be silently dropped")
}

// TestCreateTask_ConnectorStage_PublishesSchemaValidEvent is the regression
// test for a real bug: assigneeUserIDs was left nil (not an empty slice) for
// connector-typed stages, which Go marshals as JSON null. The real schema
// (unlike noopValidator, used everywhere else in this file) requires
// assignee_user_ids as a strict array with no null alternative, so a
// connector task's workflow.task.created event failed validation and never
// published — the Temporal activity errored and retried forever.
func TestCreateTask_ConnectorStage_PublishesSchemaValidEvent(t *testing.T) {
	deps, _, _, outbox := newTestDeps()
	validator, err := eventbus.NewSchemaValidator()
	require.NoError(t, err)
	deps.Validator = validator

	stage := dsl.StageDef{
		Type: "connector", ConnectorType: "send-email",
		IOMapping: &dsl.IOMapping{Inputs: []dsl.IOVar{{Source: "applicantEmail", Target: "to"}}},
	}
	compiled, err := json.Marshal(stage)
	require.NoError(t, err)

	_, err = deps.CreateTask(context.Background(), port.CreateTaskInput{
		InstanceID: uuid.New().String(), TenantID: uuid.New().String(), NodeKey: "sales/connector",
		CompiledNode: compiled, ContextJSON: `{"applicantEmail":"a@example.com"}`,
	})
	require.NoError(t, err, "a connector task's workflow.task.created must pass real schema validation")
	require.Len(t, outbox.enqueued, 1)

	var payload struct {
		AssigneeUserIDs []uuid.UUID `json:"assignee_user_ids"`
	}
	require.NoError(t, json.Unmarshal(outbox.enqueued[0].Payload, &payload))
	assert.NotNil(t, payload.AssigneeUserIDs, "assignee_user_ids must marshal as [], not null")
	assert.Empty(t, payload.AssigneeUserIDs)
}

// TestCreateTask_UsesInstanceBusinessKeyAndVersion is the regression test for
// workflow.task.created always shipping an empty business_key and all-zero
// workflow_version_id: CreateTaskInput carried neither, even though the
// running instance already has both.
func TestCreateTask_UsesInstanceBusinessKeyAndVersion(t *testing.T) {
	deps, _, _, outbox := newTestDeps()
	instanceID := uuid.New()
	wantVersion := uuid.New()
	deps.Instances.(*permissiveInstanceRepo).byID[instanceID] = &domain.Instance{
		ID: instanceID, BusinessKey: "TND-042", WorkflowVersionID: wantVersion,
	}

	compiled, err := json.Marshal(dsl.StageDef{Type: "review", DefaultAssignees: []string{uuid.New().String()}})
	require.NoError(t, err)

	_, err = deps.CreateTask(context.Background(), port.CreateTaskInput{
		InstanceID: instanceID.String(), TenantID: uuid.New().String(), NodeKey: "sales/review", CompiledNode: compiled,
	})
	require.NoError(t, err)
	require.Len(t, outbox.enqueued, 1)

	var payload struct {
		BusinessKey       string    `json:"business_key"`
		WorkflowVersionID uuid.UUID `json:"workflow_version_id"`
	}
	require.NoError(t, json.Unmarshal(outbox.enqueued[0].Payload, &payload))
	assert.Equal(t, "TND-042", payload.BusinessKey)
	assert.Equal(t, wantVersion, payload.WorkflowVersionID)
}

func TestCreateTask_IAMDepartmentID_UsedAsDepartmentID(t *testing.T) {
	deps, tasks, _, _ := newTestDeps()
	compiled, err := json.Marshal(dsl.StageDef{Type: "review"})
	require.NoError(t, err)
	iamDeptID := uuid.New()

	out, err := deps.CreateTask(context.Background(), port.CreateTaskInput{
		InstanceID: uuid.New().String(), TenantID: uuid.New().String(), NodeKey: "sales/review",
		CompiledNode: compiled, IAMDepartmentID: iamDeptID.String(),
	})
	require.NoError(t, err)

	taskID, err := uuid.Parse(out.TaskID)
	require.NoError(t, err)
	assert.Equal(t, iamDeptID, tasks.byID[taskID].DepartmentID)
}

func TestCreateTask_NoIAMDepartmentID_IsNilDepartmentID(t *testing.T) {
	deps, tasks, _, _ := newTestDeps()
	compiled, err := json.Marshal(dsl.StageDef{Type: "review"})
	require.NoError(t, err)

	out, err := deps.CreateTask(context.Background(), port.CreateTaskInput{
		InstanceID: uuid.New().String(), TenantID: uuid.New().String(), NodeKey: "sales/review", CompiledNode: compiled,
	})
	require.NoError(t, err)

	taskID, err := uuid.Parse(out.TaskID)
	require.NoError(t, err)
	assert.Equal(t, uuid.Nil, tasks.byID[taskID].DepartmentID)
}

func TestCreateTask_InvalidTenantID_IsNonRetryable(t *testing.T) {
	deps, _, _, _ := newTestDeps()
	compiled, err := json.Marshal(dsl.StageDef{Type: "review"})
	require.NoError(t, err)

	_, err = deps.CreateTask(context.Background(), port.CreateTaskInput{
		InstanceID: uuid.New().String(), TenantID: "not-a-uuid", NodeKey: "sales/review", CompiledNode: compiled,
	})
	require.Error(t, err)
	var appErr *temporal.ApplicationError
	require.True(t, errors.As(err, &appErr))
	assert.Equal(t, "ValidationError", appErr.Type())
}

func TestUpdateInstanceNodes_UpdatesCurrentNodeKeys(t *testing.T) {
	tenantID, instanceID := uuid.New(), uuid.New()
	inst := &domain.Instance{ID: instanceID, TenantID: tenantID, RecordVersion: 1}
	instances := newFakeInstanceRepo(inst)
	deps := &outboundtemporal.Deps{Instances: instances}

	err := deps.UpdateInstanceNodes(context.Background(), port.UpdateInstanceNodesInput{
		InstanceID: instanceID.String(), TenantID: tenantID.String(), NodeKeys: []domain.NodeKey{"sales/approve"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"sales/approve"}, inst.CurrentNodeKeys)
	assert.Equal(t, int64(2), inst.RecordVersion)
}

// TestUpdateInstanceNodes_UpdateCurrentNodeKeysError forces
// Instances.UpdateCurrentNodeKeys to fail.
func TestUpdateInstanceNodes_UpdateCurrentNodeKeysError(t *testing.T) {
	tenantID, instanceID := uuid.New(), uuid.New()
	inst := &domain.Instance{ID: instanceID, TenantID: tenantID, RecordVersion: 1}
	instances := newFakeInstanceRepo(inst)
	instances.updateCurrentNodeKeysErr = errBoom
	deps := &outboundtemporal.Deps{Instances: instances}

	err := deps.UpdateInstanceNodes(context.Background(), port.UpdateInstanceNodesInput{
		InstanceID: instanceID.String(), TenantID: tenantID.String(), NodeKeys: []domain.NodeKey{"sales/approve"},
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "update current_node_keys")
}
