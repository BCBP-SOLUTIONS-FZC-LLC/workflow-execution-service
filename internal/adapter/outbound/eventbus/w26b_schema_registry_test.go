package eventbus

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
)

func TestNewSchemaValidator_RegistersExactlyTheOutboundEventTypes(t *testing.T) {
	validator, err := NewSchemaValidator()
	require.NoError(t, err)

	registered := make([]string, 0, len(validator.schemas))
	for name, sch := range validator.schemas {
		assert.NotNil(t, sch, "every registered name holds a compiled schema: %s", name)
		registered = append(registered, name)
	}

	assert.Len(t, registered, 18)
	assert.ElementsMatch(t, domain.OutboundEventTypes(), registered,
		"the registered names are the outbound event types, no more and no fewer")

	for _, name := range domain.OutboundEventTypes() {
		err := validator.Validate(context.Background(), name, json.RawMessage(`{}`))
		require.Error(t, err, name)
		assert.NotContains(t, err.Error(), "no schema registered", "an empty body fails the schema of %s, not the lookup", name)
	}
}

func TestNewSchemaValidatorFromEntries_ValidJSONThatIsNotASchema_FailsToCompileNamingTheEntry(t *testing.T) {
	cases := []struct {
		name string
		src  string
	}{
		{name: "type is a number", src: `{"type": 5}`},
		{name: "type is an unknown word", src: `{"type": "widget"}`},
		{name: "required is not a list", src: `{"type": "object", "required": "a"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			entries := []schemaEntry{
				{name: "workflow.fine", src: []byte(`{"type":"object"}`)},
				{name: "workflow.broken", src: []byte(tc.src)},
			}

			validator, err := newSchemaValidatorFromEntries(entries)

			require.Error(t, err)
			assert.Nil(t, validator, "no validator is returned when an entry does not compile")
			assert.Contains(t, err.Error(), `compile event schema "workflow.broken": `)
			assert.NotContains(t, err.Error(), "workflow.fine", "the entry that compiled is not blamed")
			assert.NotContains(t, err.Error(), "load event schema", "the JSON itself was fine")
		})
	}
}
