package service_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestForceBack_DegradedWithAKeyNamingNoNode_IsRefusedBeforeTheSignal(t *testing.T) {
	plan := frfixPlanJSON(t, dsl.CurrentSchemaVersion, frfixParallelMain())
	failed := frfixTask("legal/review", domain.TaskStatusFailed)

	t.Run("a stage the department does not have", func(t *testing.T) {
		rig := newFrfixRig(t, domain.InstanceStatusDegraded, plan, failed)
		require.ErrorIs(t, rig.back("finance/typo"), port.ErrTargetNodeNotFound)
		assert.Zero(t, rig.signals())
	})
	t.Run("a department the plan does not have", func(t *testing.T) {
		rig := newFrfixRig(t, domain.InstanceStatusDegraded, plan, failed)
		require.ErrorIs(t, rig.back("nowhere/check"), port.ErrTargetNodeNotFound)
		assert.Zero(t, rig.signals())
	})
	t.Run("control: a node of the plan is signalled", func(t *testing.T) {
		rig := newFrfixRig(t, domain.InstanceStatusDegraded, plan, failed)
		require.NoError(t, rig.back("finance/check"))
		assert.Equal(t, 1, rig.signals())
	})
}
