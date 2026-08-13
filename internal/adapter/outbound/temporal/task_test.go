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

	outboundtemporal "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

func newTestDeps() (*outboundtemporal.Deps, *fakeTaskRepo, *fakeAssignmentRepo, *fakeOutbox) {
	tasks := newFakeTaskRepo()
	assignments := newFakeAssignmentRepo()
	outbox := &fakeOutbox{}
	deps := &outboundtemporal.Deps{
		Tasks:       tasks,
		Assignments: assignments,
		Outbox:      outbox,
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
