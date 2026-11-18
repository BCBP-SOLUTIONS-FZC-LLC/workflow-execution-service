package temporal_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// An expansion failure means the stored plan is unusable, so the activity
// reports it as an invalid version, the same answer the service's own reader gives.
func TestGetCompiledPlan_ExpansionFailure_IsVersionInvalid(t *testing.T) {
	missing := `{"main_plan":"main","schema_version":1,"plans":[{"name":"main","departments":[],"execution":{"steps":[{"call_plan":{"node_id":"CA_1","plan":"Nowhere"}}]}}]}`
	_, err := readPlan(planDeps(t, missing))
	requireNonRetryable(t, err)
	require.ErrorIs(t, err, port.ErrVersionInvalid)
	assert.Contains(t, err.Error(), "expand compiled plan")
}
