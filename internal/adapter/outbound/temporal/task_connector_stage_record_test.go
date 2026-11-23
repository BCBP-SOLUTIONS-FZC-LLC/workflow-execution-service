package temporal_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

func TestCreateTask_ConnectorStage_IsSingleModeWithNoAssigneeInTheEvent(t *testing.T) {
	deps, tasks, _, outbox := newTestDeps()
	compiled, err := json.Marshal(dsl.StageDef{Type: "connector", ConnectorType: "send-email"})
	require.NoError(t, err)

	out, err := deps.CreateTask(context.Background(), port.CreateTaskInput{
		InstanceID: uuid.NewString(), TenantID: uuid.NewString(), NodeKey: "sales/connector", CompiledNode: compiled,
	})

	require.NoError(t, err)
	assert.Equal(t, "single", tasks.byID[uuid.MustParse(out.TaskID)].AssigneeMode)
	require.Len(t, outbox.enqueued, 1)
	assert.JSONEq(t, `[]`, string(wkactPayloadFields(t, outbox.enqueued[0])["assignee_user_ids"]), "a connector task has no assignee")
}

func TestCreateTask_HumanStageWithNoAssigneesCreatesAReadyTaskWithNoAssignmentRows(t *testing.T) {
	deps, tasks, assignments, outbox := newTestDeps()
	compiled, err := json.Marshal(dsl.StageDef{Type: "prep"})
	require.NoError(t, err)

	out, err := deps.CreateTask(context.Background(), port.CreateTaskInput{
		InstanceID: uuid.NewString(), TenantID: uuid.NewString(), NodeKey: "ops/Step", CompiledNode: compiled,
	})

	require.NoError(t, err)
	task := tasks.byID[uuid.MustParse(out.TaskID)]
	require.NotNil(t, task)
	assert.Equal(t, domain.TaskStatusReady, task.Status)
	assert.Empty(t, assignments.byID, "no default assignee and no override give no assignment row")
	require.Len(t, outbox.enqueued, 1)
	assert.JSONEq(t, `[]`, string(wkactPayloadFields(t, outbox.enqueued[0])["assignee_user_ids"]))
}
