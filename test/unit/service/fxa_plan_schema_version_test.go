package service_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func fxaOtherSchemaVersions() []int {
	return []int{0, dsl.CurrentSchemaVersion + 1}
}

func TestInstanceStart_APlanOfAnotherSchemaVersionIsVersionInvalidAndStartsNothing(t *testing.T) {
	for _, schema := range fxaOtherSchemaVersions() {
		t.Run(fmt.Sprintf("schema version %d", schema), func(t *testing.T) {
			svc, instances, _, _, outbox, temporal, _, eligibility := newInstanceServiceHarness()
			tenantID, versionID, assignee := uuid.New(), uuid.New(), uuid.New()
			definitions := &csdDefinitions{resp: publishedCompiledWorkflow(uuid.New(), versionID, w8aCallingPlan(schema, assignee))}
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
				TenantID: tenantID, WorkflowVersionID: versionID, SubjectID: "SUBJ-1", BusinessKey: "BK-1", StartedByUserID: uuid.New(),
			})

			require.ErrorIs(t, err, port.ErrVersionInvalid)
			assert.Nil(t, got)
			assert.Equal(t, []csdIDCall{{TenantID: tenantID, ID: versionID}}, definitions.calls)
			assert.Empty(t, instances.byID)
			assert.Empty(t, outbox.enqueued)
			assert.Zero(t, started)
			assert.Zero(t, eligibilityAsked)
		})
	}
}

func TestAnswerOutsideWait_APlanOfAnotherSchemaVersionIsVersionInvalidAndTheWorkflowIsNotAsked(t *testing.T) {
	for _, schema := range fxaOtherSchemaVersions() {
		t.Run(fmt.Sprintf("schema version %d", schema), func(t *testing.T) {
			svc, inst, temporal, queries := csbOutsideSetup(t, csbCalledPlanJSON(t, schema), domain.InstanceStatusRunning, outsideKey)

			err := svc.AnswerOutsideWait(context.Background(), inst.TenantID, inst.ID, adminScope(), outsideKey, nil)

			require.ErrorIs(t, err, port.ErrVersionInvalid)
			assert.Empty(t, temporal.signals)
			assert.Zero(t, *queries)
		})
	}
}

func TestForceForward_APlanOfAnotherSchemaVersionIsVersionInvalidAndNothingIsSignalled(t *testing.T) {
	for _, schema := range fxaOtherSchemaVersions() {
		t.Run(fmt.Sprintf("schema version %d", schema), func(t *testing.T) {
			main := &dsl.CompiledPlan{Name: "main", Departments: []dsl.DepartmentDef{frfixDept("finance", "settlement")}}
			rig := newFrfixRig(t, domain.InstanceStatusRunning, frfixPlanJSON(t, schema, main))

			err := rig.forward("finance/settlement")

			require.ErrorIs(t, err, port.ErrVersionInvalid)
			assert.Zero(t, rig.signals())
		})
	}
}
