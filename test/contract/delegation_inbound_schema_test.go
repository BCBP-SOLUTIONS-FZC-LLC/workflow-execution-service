package contract_test

import (
	"encoding/json"
	"os"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

var delegationEndedReasons = []string{
	"cancelled", "delegate_disabled", "delegate_removed", "expired", "reassigned", "review_expired",
}

type schemaProperty struct {
	Type any      `json:"type" yaml:"type"`
	Enum []string `json:"enum" yaml:"enum"`
}

type inboundSchema struct {
	Properties map[string]schemaProperty `json:"properties" yaml:"properties"`
}

func loadAsyncAPISchema(t *testing.T, name string) inboundSchema {
	t.Helper()
	raw, err := os.ReadFile("../../api/asyncapi.yaml")
	require.NoError(t, err)
	var doc struct {
		Components struct {
			Schemas map[string]inboundSchema `yaml:"schemas"`
		} `yaml:"components"`
	}
	require.NoError(t, yaml.Unmarshal(raw, &doc))
	schema, ok := doc.Components.Schemas[name]
	require.True(t, ok, "AsyncAPI schema %s is missing", name)
	return schema
}

func loadEventSchemaFile(t *testing.T, file string) inboundSchema {
	t.Helper()
	raw, err := os.ReadFile("../../internal/eventschema/" + file)
	require.NoError(t, err)
	var schema inboundSchema
	require.NoError(t, json.Unmarshal(raw, &schema))
	return schema
}

func sorted(values []string) []string {
	out := append([]string(nil), values...)
	sort.Strings(out)
	return out
}

func TestDelegationEndedInbound_EndedReasonCoversEveryProducerValue(t *testing.T) {
	fromAsyncAPI := loadAsyncAPISchema(t, "DelegationEndedInbound").Properties["ended_reason"].Enum
	fromSchemaFile := loadEventSchemaFile(t, "delegation_ended_inbound.json").Properties["ended_reason"].Enum

	assert.Equal(t, delegationEndedReasons, sorted(fromAsyncAPI), "AsyncAPI ended_reason")
	assert.Equal(t, delegationEndedReasons, sorted(fromSchemaFile), "inbound schema file ended_reason")
}

func TestDelegationStartedInbound_OptionalFieldsAreAbsentNotNull(t *testing.T) {
	for source, schema := range map[string]inboundSchema{
		"AsyncAPI":    loadAsyncAPISchema(t, "DelegationStartedInbound"),
		"schema file": loadEventSchemaFile(t, "delegation_started_inbound.json"),
	} {
		for _, field := range []string{"scope_id", "ends_at"} {
			assert.Equal(t, "string", schema.Properties[field].Type,
				"%s: %s is omitted by the producer when not applicable, never null", source, field)
		}
	}
}
