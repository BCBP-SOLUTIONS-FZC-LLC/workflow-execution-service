package eventbus

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewSchemaValidatorFromEntries_BadJSON(t *testing.T) {
	entries := []schemaEntry{
		{name: "workflow.test", src: []byte(`{not valid json`)},
	}
	_, err := newSchemaValidatorFromEntries(entries)
	require.Error(t, err, "malformed JSON schema bytes must return error")
	assert.Contains(t, err.Error(), "workflow.test", "error must name the failing schema")
}

func TestNewSchemaValidatorFromEntries_DuplicateURL(t *testing.T) {
	validSchema := []byte(`{"type":"object"}`)
	entries := []schemaEntry{
		{name: "workflow.test", src: validSchema},
		{name: "workflow.test", src: validSchema}, // duplicate name -> same URL -> AddResource fails
	}
	_, err := newSchemaValidatorFromEntries(entries)
	require.Error(t, err, "duplicate schema name must return error from AddResource")
	assert.Contains(t, err.Error(), "workflow.test")
}

func TestNewSchemaValidatorFromEntries_InvalidSchema(t *testing.T) {
	invalidSchema := []byte(`{"$ref": "execution-event-schema:nonexistent"}`)
	entries := []schemaEntry{
		{name: "workflow.test", src: invalidSchema},
	}
	_, err := newSchemaValidatorFromEntries(entries)
	require.Error(t, err, "schema with unresolvable $ref must fail at Compile")
	assert.Contains(t, err.Error(), "workflow.test")
}

func TestNewSchemaValidatorFromEntries_Success(t *testing.T) {
	entries := []schemaEntry{
		{name: "workflow.test", src: []byte(`{"type":"object","required":["a"],"additionalProperties":true}`)},
	}
	validator, err := newSchemaValidatorFromEntries(entries)
	require.NoError(t, err)
	require.NotNil(t, validator)
	assert.Len(t, validator.schemas, 1)
}
