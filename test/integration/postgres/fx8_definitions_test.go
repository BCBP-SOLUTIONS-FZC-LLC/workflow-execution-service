//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// fx8Definitions answers every version with one plan holding the given node.
type fx8Definitions struct {
	planJSON string
}

func newFx8Definitions(t *testing.T, deptID, nodeID string) *fx8Definitions {
	t.Helper()
	plan := &dsl.CompiledPlan{Name: "main", Departments: []dsl.DepartmentDef{
		{ID: deptID, Stages: []dsl.StageDef{{Type: "userTask", NodeID: nodeID}}},
	}}
	raw, err := json.Marshal(dsl.CompiledCollaboration{SchemaVersion: dsl.CurrentSchemaVersion, MainPlan: plan.Name, Plans: []*dsl.CompiledPlan{plan}})
	require.NoError(t, err)
	return &fx8Definitions{planJSON: string(raw)}
}

func (d *fx8Definitions) GetCompiledWorkflow(_ context.Context, _, versionID uuid.UUID) (*port.CompiledWorkflow, error) {
	return &port.CompiledWorkflow{VersionID: versionID, VersionNumber: 1, Status: "PUBLISHED", IsValid: true, CompiledPlanJSON: d.planJSON}, nil
}
