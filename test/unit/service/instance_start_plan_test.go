package service_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestInstanceService_Start_UnreadableStoredPlanIsAnInternalFailure(t *testing.T) {
	cases := []struct {
		name    string
		plan    string
		wantMsg string
	}{
		{"an empty plan", "", "unmarshal compiled plan"},
		{"a plan that is not JSON", "{not json", "unmarshal compiled plan"},
		{"a plan that is JSON null", "null", `main plan "" not found`},
		{
			"a main plan name no plan carries",
			`{"main_plan":"ghost","plans":[{"name":"main","task_queue":"wf-queue-default","departments":[]}]}`,
			`main plan "ghost" not found`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, instances, _, _, outbox, temporal, definitions, _ := newInstanceServiceHarness()
			tenantID, versionID := uuid.New(), uuid.New()
			definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, tc.plan)
			started := 0
			temporal.startFunc = func(context.Context, port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
				started++
				return port.StartWorkflowOutput{}, nil
			}

			got, err := svc.Start(context.Background(), port.StartInstanceInput{
				TenantID: tenantID, WorkflowVersionID: versionID, SubjectID: "SUBJ-1", BusinessKey: "TND-001", StartedByUserID: uuid.New(),
			})

			require.Error(t, err)
			assert.Nil(t, got)
			assert.ErrorContains(t, err, tc.wantMsg)
			for _, domainErr := range []error{port.ErrVersionInvalid, port.ErrVersionNotPublished, port.ErrDuplicateBusinessKey} {
				assert.NotErrorIs(t, err, domainErr, "a corrupt stored plan is an internal failure, not a refusal the caller can fix")
			}
			assert.Empty(t, instances.byID, "no instance is written")
			assert.Empty(t, outbox.enqueued, "no started event is written")
			assert.Zero(t, started, "no workflow is started")
		})
	}
}
