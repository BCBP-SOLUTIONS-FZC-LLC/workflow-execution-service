package service_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestAnswerOutsideWait_VariablesThatCannotMergeIntoTheContextAreRefusedBeforeTheSignal(t *testing.T) {
	cases := []struct {
		name      string
		context   string
		variables map[string]any
		refused   bool
	}{
		{"an object context takes variables", `{"a":1}`, map[string]any{"bid": "1"}, false},
		{"an empty context takes variables", "", map[string]any{"bid": "1"}, false},
		{"a null context takes variables", "null", map[string]any{"bid": "1"}, false},
		{"an array context refuses variables", `[1,2]`, map[string]any{"bid": "1"}, true},
		{"a string context refuses variables", `"text"`, map[string]any{"bid": "1"}, true},
		{"a number context refuses variables", `42`, map[string]any{"bid": "1"}, true},
		{"an array context takes an answer with no variables", `[1,2]`, nil, false},
		{"an array context takes an answer with empty variables", `[1,2]`, map[string]any{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, inst, temporal := outsideAnswerHarness(t, domain.InstanceStatusRunning, outsideKey)
			inst.ContextJSON = []byte(tc.context)
			asked := w28bCountQueries(t, temporal, inst.TemporalWorkflowID, func() []string { return []string{outsideKey} })

			err := svc.AnswerOutsideWait(context.Background(), inst.TenantID, inst.ID, adminScope(), outsideKey, tc.variables)

			if tc.refused {
				require.ErrorIs(t, err, port.ErrAnswerVariablesInvalid)
				assert.Empty(t, temporal.signals)
				assert.Zero(t, *asked, "the workflow is not asked")
				return
			}
			require.NoError(t, err)
			assert.Len(t, temporal.signals, 1)
		})
	}
}
