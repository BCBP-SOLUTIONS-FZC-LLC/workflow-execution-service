package service_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestInstanceService_Start_StoredContextThatCannotBeReadFailsTheStartVisibly(t *testing.T) {
	for name, stored := range map[string]string{"truncated json": `{`, "not json": `<html>`} {
		t.Run(name, func(t *testing.T) {
			tenantID, versionID, caller := uuid.New(), uuid.New(), uuid.New()
			svc, instances, _, _, _, temporal, definitions, _ := newInstanceServiceHarness()
			logger := &startfixLogger{}
			svc.Log = logger
			definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))
			starts := 0
			temporal.startFunc = func(_ context.Context, in port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
				starts++
				return port.StartWorkflowOutput{TemporalWorkflowID: in.TemporalWorkflowID, TemporalRunID: "run-1"}, nil
			}
			first, err := svc.Start(context.Background(), startfixInput(tenantID, versionID, caller))
			require.NoError(t, err)
			instances.byID[first.ID].ContextJSON = []byte(stored)

			_, err = svc.Start(context.Background(), startfixInput(tenantID, versionID, caller))

			require.Error(t, err)
			assert.NotErrorIs(t, err, port.ErrDuplicateBusinessKey, "an unreadable row is a fault, not a duplicate")
			assert.Equal(t, 1, starts, "no second workflow start is issued")
			var named bool
			for _, f := range logger.errorFields {
				if f["instance_id"] == first.ID {
					named = true
				}
			}
			assert.True(t, named, "the Error log names the instance")
			assert.Len(t, instances.byID, 1)
		})
	}
}

func TestInstanceService_Start_DifferentRequestStillAnswersDuplicateWhenTheStoredContextReads(t *testing.T) {
	tenantID, versionID, caller := uuid.New(), uuid.New(), uuid.New()
	svc, _, _, _, _, _, definitions, _ := newInstanceServiceHarness()
	definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))
	_, err := svc.Start(context.Background(), startfixInput(tenantID, versionID, caller))
	require.NoError(t, err)

	other := startfixInput(tenantID, versionID, caller)
	other.ContextJSON = []byte(`{"changed":true}`)
	_, err = svc.Start(context.Background(), other)

	require.ErrorIs(t, err, port.ErrDuplicateBusinessKey)
}
