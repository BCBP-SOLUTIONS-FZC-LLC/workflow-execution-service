package service_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// w8aCallingPlan is a main plan with one human stage and a call to a plan that does not exist.
func w8aCallingPlan(schemaVersion int, assignee uuid.UUID) string {
	return fmt.Sprintf(`{"main_plan":"main","schema_version":%d,"plans":[{"name":"main","task_queue":"wf-queue-default",`+
		`"departments":[{"id":"finance","label":"Finance","stages":[{"type":"userTask","node_id":"review","role":"approver","default_assignees":[%q]}]}],`+
		`"execution":{"steps":[{"call_plan":{"node_id":"CA_1","plan":"Nowhere"}}]}}]}`, schemaVersion, assignee.String())
}

func TestInstanceStart_CurrentSchemaPlanCallingAMissingPlan_ReturnsVersionInvalidAndStartsNothing(t *testing.T) {
	cases := []struct {
		name    string
		wantErr error
	}{
		{"the current schema is expanded and the missing plan refused", port.ErrVersionInvalid},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, instances, _, _, _, temporal, _, eligibility := newInstanceServiceHarness()
			tenantID, versionID, starter, assignee := uuid.New(), uuid.New(), uuid.New(), uuid.New()
			definitions := &csdDefinitions{resp: publishedCompiledWorkflow(uuid.New(), versionID, w8aCallingPlan(dsl.CurrentSchemaVersion, assignee))}
			svc.Definitions = definitions
			eligibilityAsked := 0
			eligibility.batchCheck = func(_ context.Context, _ uuid.UUID, reqs []port.EligibilityCheckRequest) ([]port.EligibilityResult, error) {
				eligibilityAsked++
				return make([]port.EligibilityResult, len(reqs)), nil
			}
			started := 0
			temporal.startFunc = func(_ context.Context, in port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
				started++
				return port.StartWorkflowOutput{TemporalWorkflowID: in.TemporalWorkflowID}, nil
			}

			got, err := svc.Start(context.Background(), port.StartInstanceInput{
				TenantID: tenantID, WorkflowVersionID: versionID, SubjectID: "SUBJ-1", BusinessKey: "BK-1", StartedByUserID: starter,
			})

			require.ErrorIs(t, err, tc.wantErr)
			assert.Nil(t, got)
			assert.Equal(t, []csdIDCall{{TenantID: tenantID, ID: versionID}}, definitions.calls, "the plan is read for the caller's tenant and the requested version")
			assert.Empty(t, instances.byID, "no instance is created for a plan that does not expand")
			assert.Zero(t, started)
			assert.Zero(t, eligibilityAsked)
		})
	}
}
