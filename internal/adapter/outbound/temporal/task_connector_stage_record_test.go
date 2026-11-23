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

func TestCreateTask_ConnectorStage_RecordsTheOutputMappingAndTheSingleAssigneeMode(t *testing.T) {
	deps, tasks, assignments, outbox := newTestDeps()
	stage := dsl.StageDef{
		Type: "connector", ConnectorType: "send-email",
		IOMapping: &dsl.IOMapping{
			Inputs:  []dsl.IOVar{{Source: "=applicantEmail", Target: "to"}},
			Outputs: []dsl.IOVar{{Source: "messageId", Target: "mailId"}},
		},
	}
	compiled, err := json.Marshal(stage)
	require.NoError(t, err)

	out, err := deps.CreateTask(context.Background(), port.CreateTaskInput{
		InstanceID: uuid.NewString(), TenantID: uuid.NewString(), NodeKey: "sales/connector",
		CompiledNode: compiled, ContextJSON: `{"applicantEmail":"a@example.com"}`,
	})

	require.NoError(t, err)
	task := tasks.byID[uuid.MustParse(out.TaskID)]
	require.NotNil(t, task)
	assert.Equal(t, "single", task.AssigneeMode)
	var extras struct {
		ResolvedInputs map[string]any   `json:"resolved_inputs"`
		OutputMapping  []map[string]any `json:"output_mapping"`
	}
	require.NoError(t, json.Unmarshal(task.ExtrasJSON, &extras))
	assert.Equal(t, []map[string]any{{"source": "messageId", "target": "mailId"}}, extras.OutputMapping)
	assert.Empty(t, assignments.byID)
	require.Len(t, outbox.enqueued, 1)
	fields := wkactPayloadFields(t, outbox.enqueued[0])
	assert.JSONEq(t, `"send-email"`, string(fields["connector_type"]))
	assert.JSONEq(t, `{"to":"a@example.com"}`, string(fields["resolved_inputs"]))
	assert.JSONEq(t, `[{"source":"messageId","target":"mailId"}]`, string(fields["output_mapping"]))
	assert.JSONEq(t, `[]`, string(fields["assignee_user_ids"]), "a connector task has no assignee")
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
