package service_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestInstanceService_Start_WorkflowIDIsUniquePerInstance(t *testing.T) {
	tenantID, versionID, caller := uuid.New(), uuid.New(), uuid.New()

	setup := func(t *testing.T) (start func(subject, key string) *port.Instance, instances *fakeInstanceRepo, sent *[]string) {
		t.Helper()
		svc, repo, _, _, _, temporal, definitions, _ := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))
		var ids []string
		temporal.startFunc = func(_ context.Context, in port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
			ids = append(ids, in.TemporalWorkflowID)
			return port.StartWorkflowOutput{TemporalWorkflowID: in.TemporalWorkflowID, TemporalRunID: "run-" + in.InstanceID.String()}, nil
		}
		return func(subject, key string) *port.Instance {
			in := startfixInput(tenantID, versionID, caller)
			in.SubjectID, in.BusinessKey = subject, key
			got, err := svc.Start(context.Background(), in)
			require.NoError(t, err)
			return got
		}, repo, &ids
	}

	t.Run("the id names the tenant and the instance and is the one stored and sent", func(t *testing.T) {
		start, _, sent := setup(t)

		got := start("SUBJ-1", "TND-001")

		want := "tenant:" + tenantID.String() + ":instance:" + got.ID.String()
		assert.Equal(t, want, got.TemporalWorkflowID)
		assert.Equal(t, []string{want}, *sent)
	})

	t.Run("a business key reused after the first instance finished gets its own id", func(t *testing.T) {
		start, instances, sent := setup(t)
		first := start("SUBJ-1", "TND-001")
		instances.byID[first.ID].Status = domain.InstanceStatusCompleted

		second := start("SUBJ-1", "TND-001")

		require.NotEqual(t, first.ID, second.ID)
		assert.NotEqual(t, first.TemporalWorkflowID, second.TemporalWorkflowID)
		assert.Equal(t, []string{first.TemporalWorkflowID, second.TemporalWorkflowID}, *sent)
	})

	t.Run("two subject and key pairs that join to the same text do not share an id", func(t *testing.T) {
		start, _, _ := setup(t)

		a := start("a:b", "c")
		b := start("a", "b:c")

		assert.NotEqual(t, a.TemporalWorkflowID, b.TemporalWorkflowID)
	})
}
