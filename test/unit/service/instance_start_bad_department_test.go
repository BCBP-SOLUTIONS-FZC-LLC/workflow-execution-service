package service_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

func TestInstanceStart_ADepartmentIDThatIsNotAUUIDRefusesTheVersionBeforeAnyEligibilityCall(t *testing.T) {
	svc, _, _, _, _, _, definitions, eligibility := newInstanceServiceHarness()
	raw, err := json.Marshal(dsl.CompiledCollaboration{
		SchemaVersion: dsl.CurrentSchemaVersion, MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main", TaskQueue: "wf-queue-default",
			Departments: []dsl.DepartmentDef{{
				ID: "finance", IAMDepartmentID: "not-a-uuid",
				Stages: []dsl.StageDef{{
					Type: "userTask", NodeID: "review", Role: "reviewer",
					DefaultAssignees: []string{uuid.NewString()},
				}},
			}},
		}},
	})
	require.NoError(t, err)
	definitions.resp = publishedCompiledWorkflow(uuid.New(), uuid.New(), string(raw))
	eligibilityCalls := 0
	eligibility.batchCheck = func(_ context.Context, _ uuid.UUID, reqs []port.EligibilityCheckRequest) ([]port.EligibilityResult, error) {
		eligibilityCalls++
		return make([]port.EligibilityResult, len(reqs)), nil
	}

	_, err = svc.Start(context.Background(), port.StartInstanceInput{
		TenantID: uuid.New(), WorkflowVersionID: uuid.New(), SubjectID: "SUBJ-1",
		BusinessKey: "TND-201", StartedByUserID: uuid.New(),
	})

	require.ErrorIs(t, err, port.ErrVersionInvalid)
	assert.ErrorContains(t, err, "not-a-uuid")
	assert.Zero(t, eligibilityCalls)
}
