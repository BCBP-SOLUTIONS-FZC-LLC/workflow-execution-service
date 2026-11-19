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

func TestCreateTask_ConnectorInputsKeepIntegersAbove2Pow53Verbatim(t *testing.T) {
	rig := w25bNewRig()
	deps := w25bNewDeps(t, rig)
	tenant := uuid.New()
	inst := rig.addInstance(tenant, domain.InstanceStatusRunning)
	mapping := &dsl.IOMapping{Inputs: []dsl.IOVar{{Source: "=amount", Target: "total"}}}
	in := w25bCreateInput(t, inst, tenant, dsl.StageDef{Type: "connector", ConnectorType: "send-email", IOMapping: mapping})
	in.ContextJSON = `{"amount":9007199254740993}`

	out, err := deps.CreateTask(context.Background(), in)

	require.NoError(t, err)
	extras := map[string]json.RawMessage{}
	require.NoError(t, json.Unmarshal(rig.tasks[uuid.MustParse(out.TaskID)].ExtrasJSON, &extras))
	assert.Equal(t, `{"total":9007199254740993}`, string(extras["resolved_inputs"]))
}
