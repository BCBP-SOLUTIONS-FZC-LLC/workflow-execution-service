package contract_test

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDelegationStartedInbound_SchemaFileRequiresAScopeIdForAScopeThatNamesAnObject(t *testing.T) {
	raw, err := os.ReadFile("../../internal/eventschema/delegation_started_inbound.json")
	require.NoError(t, err)
	var schema struct {
		If struct {
			Required   []string `json:"required"`
			Properties struct {
				Scope struct {
					Enum []string `json:"enum"`
				} `json:"scope"`
			} `json:"properties"`
		} `json:"if"`
		Then struct {
			Required   []string `json:"required"`
			Properties struct {
				ScopeID struct {
					MinLength int `json:"minLength"`
				} `json:"scope_id"`
			} `json:"properties"`
		} `json:"then"`
	}
	require.NoError(t, json.Unmarshal(raw, &schema))

	assert.ElementsMatch(t, []string{"department", "tender"}, schema.If.Properties.Scope.Enum, "the rule applies to the scopes that name an object")
	assert.Contains(t, schema.If.Required, "scope")
	assert.Equal(t, []string{"scope_id"}, schema.Then.Required)
	assert.Equal(t, 1, schema.Then.Properties.ScopeID.MinLength, "an empty scope_id does not satisfy the rule")
}
