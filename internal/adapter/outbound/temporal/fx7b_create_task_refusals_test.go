package temporal_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

func TestCreateTask_AnEventPayloadTheSchemaRefuses_IsNonRetryableAndRollsBack(t *testing.T) {
	t.Parallel()
	rig := w25bNewRig()
	deps := w25bNewDeps(t, rig)
	tenant := uuid.New()
	inst := rig.addInstance(tenant, domain.InstanceStatusRunning)
	in := w25bCreateInput(t, inst, tenant, dsl.StageDef{Type: "review", DefaultAssignees: []string{uuid.NewString()}})
	w25bRejectEvents(deps, errW25bStorage)

	_, err := deps.CreateTask(context.Background(), in)

	appErr := w25bRequireNonRetryable(t, err, "ValidationError", "build workflow.task.created envelope")
	require.ErrorIs(t, appErr, errW25bStorage)
	assert.Equal(t, 1, rig.rolledBack)
	assert.Empty(t, rig.tasks, "the task row goes with the failed transaction")
	assert.Empty(t, rig.outbox)
}

func TestEnqueueInstanceEvent_APayloadTheSchemaRefuses_IsNonRetryable(t *testing.T) {
	t.Parallel()
	s := w25bNewScene(t, true)
	w25bRejectEvents(s.deps, errW25bStorage)

	err := s.pauseInstance()

	appErr := w25bRequireNonRetryable(t, err, "ValidationError", "build "+domain.EventWorkflowInstancePaused+" envelope")
	require.ErrorIs(t, appErr, errW25bStorage)
	assert.Equal(t, domain.InstanceStatusRunning, s.rig.instances[s.inst.ID].Status, "the status change goes with the refused event")
}

func TestCreateTask_ACompiledNodeThatIsJSONNull_IsRefusedWithoutRetryOrAnyWrite(t *testing.T) {
	t.Parallel()
	for _, node := range []string{`null`, " \n null\t"} {
		t.Run(node, func(t *testing.T) {
			t.Parallel()
			rig := w25bNewRig()
			deps := w25bNewDeps(t, rig)
			tenant := uuid.New()
			inst := rig.addInstance(tenant, domain.InstanceStatusRunning)
			in := w25bCreateInput(t, inst, tenant, dsl.StageDef{Type: "review"})
			in.CompiledNode = json.RawMessage(node)

			_, err := deps.CreateTask(context.Background(), in)

			_ = w25bRequireNonRetryable(t, err, "ValidationError", "compiled node")
			w25bRequireNothingTried(t, rig)
		})
	}
}
