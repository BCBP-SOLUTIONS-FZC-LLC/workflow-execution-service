package service_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type startfixLogger struct{ errorFields []map[string]any }

func (l *startfixLogger) Debug(string, map[string]any) {}
func (l *startfixLogger) Info(string, map[string]any)  {}
func (l *startfixLogger) Warn(string, map[string]any)  {}
func (l *startfixLogger) Error(_ string, fields map[string]any) {
	l.errorFields = append(l.errorFields, fields)
}

func startfixInput(tenantID, versionID, caller uuid.UUID) port.StartInstanceInput {
	return port.StartInstanceInput{
		TenantID: tenantID, WorkflowVersionID: versionID, SubjectID: "SUBJ-1", BusinessKey: "TND-001", StartedByUserID: caller,
	}
}

func TestInstanceService_Start_AVersionDefinitionDoesNotKnowIsNotFound(t *testing.T) {
	tenantID, versionID, caller := uuid.New(), uuid.New(), uuid.New()
	notFound := fmt.Errorf("%w: %w: compiled plan returned 404", port.ErrDefinitionRejected, port.ErrDefinitionNotFound)

	t.Run("a 404 from Definition is VERSION_NOT_FOUND and writes nothing", func(t *testing.T) {
		svc, instances, _, _, outbox, temporal, definitions, _ := newInstanceServiceHarness()
		definitions.err = notFound

		_, err := svc.Start(context.Background(), startfixInput(tenantID, versionID, caller))

		require.ErrorIs(t, err, port.ErrVersionNotFound)
		assert.NotErrorIs(t, err, port.ErrDefinitionUnavailable)
		assert.NotErrorIs(t, err, port.ErrDefinitionRejected, "the caller sees one answer, not an upstream failure")
		assert.Empty(t, instances.byID)
		assert.Empty(t, outbox.enqueued)
		assert.Empty(t, temporal.signals)
	})

	t.Run("an unreachable Definition Service stays an upstream failure", func(t *testing.T) {
		svc, _, _, _, _, _, definitions, _ := newInstanceServiceHarness()
		definitions.err = fmt.Errorf("%w: compiled plan returned 503", port.ErrDefinitionUnavailable)

		_, err := svc.Start(context.Background(), startfixInput(tenantID, versionID, caller))

		require.ErrorIs(t, err, port.ErrDefinitionUnavailable)
		assert.NotErrorIs(t, err, port.ErrVersionNotFound)
	})

	t.Run("another refusal from Definition stays an upstream failure", func(t *testing.T) {
		svc, _, _, _, _, _, definitions, _ := newInstanceServiceHarness()
		definitions.err = fmt.Errorf("%w: compiled plan returned 401", port.ErrDefinitionRejected)

		_, err := svc.Start(context.Background(), startfixInput(tenantID, versionID, caller))

		require.ErrorIs(t, err, port.ErrDefinitionRejected)
		assert.NotErrorIs(t, err, port.ErrVersionNotFound)
	})
}

func TestInstanceService_Start_StoredOverrideMapThatCannotBeReadFailsTheStartVisibly(t *testing.T) {
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
	require.Equal(t, 1, starts)
	instances.byID[first.ID].OverrideMap = []byte(`[`)
	instances.byID[first.ID].TemporalRunID = ""

	_, err = svc.Start(context.Background(), startfixInput(tenantID, versionID, caller))

	require.Error(t, err)
	assert.NotErrorIs(t, err, port.ErrDuplicateBusinessKey, "an unreadable row is a fault, not a duplicate")
	assert.NotErrorIs(t, err, port.ErrDefinitionUnavailable)
	assert.Equal(t, 1, starts, "no second workflow start is issued")
	require.NotEmpty(t, logger.errorFields, "the fault is logged at Error")
	var named bool
	for _, f := range logger.errorFields {
		if f["instance_id"] == first.ID {
			named = true
		}
	}
	assert.True(t, named, "the log names the instance")
	assert.Len(t, instances.byID, 1)
}
