package temporal_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestGetCompiledPlan_PlanTextThatCannotBeRead_IsANonRetryableUnmarshalError(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		planJSON   string
		wantSyntax bool
	}{
		{name: "empty string", planJSON: "", wantSyntax: true},
		{name: "white space only", planJSON: " \n\t", wantSyntax: true},
		{name: "truncated object", planJSON: `{"main_plan":"main","schema_ver`, wantSyntax: true},
		{name: "not json at all", planJSON: `compiled plan`, wantSyntax: true},
		{name: "a number where the plan name is a string", planJSON: `{"main_plan":5,"schema_version":1}`},
		{name: "a string where the schema version is a number", planJSON: `{"main_plan":"main","schema_version":"one"}`},
		{name: "an array where the plan is an object", planJSON: `[]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tenant, version := uuid.New(), uuid.New()
			defs := &w25bDefinitions{answer: &port.CompiledWorkflow{Status: "PUBLISHED", IsValid: true, CompiledPlanJSON: tc.planJSON}}

			out, err := w25bPlanDeps(defs).GetCompiledPlan(context.Background(), port.GetCompiledPlanInput{TenantID: tenant.String(), VersionID: version.String()})

			appErr := w25bRequireNonRetryable(t, err, "DefinitionServiceClientError", "unmarshal compiled plan")
			assert.Empty(t, out.Collaboration.MainPlan, "no plan comes back with the error")
			var syntaxErr *json.SyntaxError
			var typeErr *json.UnmarshalTypeError
			if tc.wantSyntax {
				require.ErrorAs(t, err, &syntaxErr)
				assert.False(t, errors.As(err, &typeErr))
			} else {
				require.ErrorAs(t, err, &typeErr)
				assert.False(t, errors.As(err, &syntaxErr))
			}
			require.Error(t, appErr.Unwrap(), "the decoder's error travels as the cause")
			assert.Equal(t, []w25bAsk{{tenant: tenant, version: version}}, defs.asked, "the plan was read once, for exactly that version")
		})
	}
}
