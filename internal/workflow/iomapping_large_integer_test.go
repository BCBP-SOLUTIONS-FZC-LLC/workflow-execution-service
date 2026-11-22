package workflow

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

func TestApplyIOMapping_KeepsIntegersAbove2Pow53Verbatim(t *testing.T) {
	mapping := &dsl.IOMapping{Inputs: []dsl.IOVar{{Source: "=amount", Target: "copy"}}}

	got, err := applyIOMapping(`{"amount":9007199254740993,"nested":{"id":9007199254740995}}`, mapping)

	require.NoError(t, err)
	assert.Equal(t, `{"amount":9007199254740993,"copy":9007199254740993,"nested":{"id":9007199254740995}}`, got)
}
