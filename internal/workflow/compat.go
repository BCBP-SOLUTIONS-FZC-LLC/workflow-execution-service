package workflow

import "github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"

// DSL schema compatibility: see "execution LLD" §2.5.

// schemaStrategy normalizes one SchemaVersion major's CompiledCollaboration
// shape into whatever runSteps expects today.
type schemaStrategy interface {
	normalize(collab *dsl.CompiledCollaboration) *dsl.CompiledCollaboration
}

type passthroughStrategy struct{}

func (passthroughStrategy) normalize(c *dsl.CompiledCollaboration) *dsl.CompiledCollaboration {
	return c
}

var schemaStrategies = map[int]schemaStrategy{
	dsl.CurrentSchemaVersion: passthroughStrategy{},
}

func resolveSchemaStrategy(version int) (schemaStrategy, bool) {
	s, ok := schemaStrategies[version]
	return s, ok
}
