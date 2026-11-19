package workflow

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// A redirect into a parallel branch chosen by its department returns the
// branch's own steps followed by the rest of the plan. Two redirects through
// the same branch must each keep their own continuation: the second must not
// write into the spare room of the branch's steps the first one returned.
func TestFindRedirectTarget_TwoRedirectsThroughOneParallelBranch_KeepTheirOwnContinuations(t *testing.T) {
	t.Parallel()
	gateway := dsl.ExecutionStep{Exclusive: []dsl.ExclusiveBranch{
		{ConditionExpression: `x == "y"`, Steps: []dsl.ExecutionStep{{Sequential: []string{"Fast"}}}},
		{Steps: []dsl.ExecutionStep{{Sequential: []string{"Slow"}}}},
	}}
	branchSteps := make([]dsl.ExecutionStep, 1, 4)
	branchSteps[0] = gateway
	parallel := dsl.ExecutionStep{Parallel: []dsl.ParallelBranch{{DeptID: "Ops", Steps: branchSteps}}}

	withClose := []dsl.ExecutionStep{parallel, {Sequential: []string{"Close"}}}
	withAudit := []dsl.ExecutionStep{parallel, {Sequential: []string{"Audit"}}}

	first, ok := findRedirectTarget(withClose, "Ops")
	require.True(t, ok)
	second, ok := findRedirectTarget(withAudit, "Ops")
	require.True(t, ok)

	assert.Equal(t, []dsl.ExecutionStep{gateway, {Sequential: []string{"Close"}}}, first,
		"the first continuation is not overwritten by the second redirect")
	assert.Equal(t, []dsl.ExecutionStep{gateway, {Sequential: []string{"Audit"}}}, second)
}
