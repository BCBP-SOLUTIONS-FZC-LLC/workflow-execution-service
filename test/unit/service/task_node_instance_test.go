package service_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

func TestTaskService_GetByNode_AnInstanceThatCannotBeLoadedIsNotFound(t *testing.T) {
	w := cseNewTaskWorld()
	_, defs := w.withEligibility(t, true, dsl.StageDef{NodeID: "review", Role: "reviewer"})
	w.instances.rows = nil

	got, err := w.svc.GetByNode(context.Background(), w.tenant, w.instance.ID, w.task.NodeKey)

	require.ErrorIs(t, err, port.ErrInstanceNotFound)
	assert.Nil(t, got, "no task is returned without its required level")
	assert.Equal(t, []cseLookup{{Tenant: w.tenant, ID: w.instance.ID}}, w.tasks.nodeCalls)
	assert.Equal(t, []string{w.task.NodeKey}, w.tasks.nodeKeyArg)
	assert.Equal(t, []cseLookup{{Tenant: w.tenant, ID: w.instance.ID}}, w.instances.calls, "the instance is read in the task's tenant")
	assert.Empty(t, defs.calls, "no plan is read for an instance that is not there")
}
