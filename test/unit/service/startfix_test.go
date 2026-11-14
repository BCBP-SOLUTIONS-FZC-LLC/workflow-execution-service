package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
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

func TestInstanceService_Start_AnExecutionHeldByAnotherInstanceIsRefused(t *testing.T) {
	tenantID, versionID, caller := uuid.New(), uuid.New(), uuid.New()
	held := fmt.Errorf("start workflow: %w", port.ErrExecutionHeld)

	setup := func(t *testing.T) (*fakeInstanceRepo, *fakeTemporalClient, func() (*port.Instance, error), *int) {
		t.Helper()
		svc, instances, _, _, _, temporal, definitions, _ := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))
		attempts := 0
		temporal.startFunc = func(context.Context, port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
			attempts++
			return port.StartWorkflowOutput{}, held
		}
		return instances, temporal, func() (*port.Instance, error) {
			return svc.Start(context.Background(), startfixInput(tenantID, versionID, caller))
		}, &attempts
	}

	t.Run("a fresh start is a duplicate, is not retried and leaves no live row", func(t *testing.T) {
		instances, _, start, attempts := setup(t)

		got, err := start()

		require.ErrorIs(t, err, port.ErrDuplicateBusinessKey)
		assert.Nil(t, got)
		assert.Equal(t, 1, *attempts, "a held execution does not heal by retrying")
		require.Len(t, instances.byID, 1)
		for _, inst := range instances.byID {
			assert.Equal(t, domain.InstanceStatusFailed, inst.Status, "the row of the refused start is closed so it does not hold the business key")
			assert.Empty(t, inst.TemporalRunID, "the refused start never joined the other execution")
		}
	})

	t.Run("a start retried after the execution is free creates its own execution", func(t *testing.T) {
		svc, instances, _, _, _, temporal, definitions, _ := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))
		isHeld := true
		temporal.startFunc = func(_ context.Context, in port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
			if isHeld {
				return port.StartWorkflowOutput{}, held
			}
			return port.StartWorkflowOutput{TemporalWorkflowID: in.TemporalWorkflowID, TemporalRunID: "run-own"}, nil
		}
		_, err := svc.Start(context.Background(), startfixInput(tenantID, versionID, caller))
		require.ErrorIs(t, err, port.ErrDuplicateBusinessKey)

		isHeld = false
		got, err := svc.Start(context.Background(), startfixInput(tenantID, versionID, caller))

		require.NoError(t, err)
		assert.Equal(t, "run-own", got.TemporalRunID)
		assert.Len(t, instances.byID, 2)
	})

	t.Run("a pending row whose execution is held is refused when its start is resumed", func(t *testing.T) {
		svc, instances, _, _, _, temporal, definitions, _ := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))
		failing := true
		temporal.startFunc = func(_ context.Context, in port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
			if failing {
				return port.StartWorkflowOutput{}, errors.New("temporal frontend unreachable")
			}
			return port.StartWorkflowOutput{}, held
		}
		_, err := svc.Start(context.Background(), startfixInput(tenantID, versionID, caller))
		require.Error(t, err)
		failing = false

		_, err = svc.Start(context.Background(), startfixInput(tenantID, versionID, caller))

		require.ErrorIs(t, err, port.ErrDuplicateBusinessKey)
		for _, inst := range instances.byID {
			assert.Equal(t, domain.InstanceStatusFailed, inst.Status)
		}
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

func TestInstanceService_Start_ARefusedHeldStartClosesItsRowWithOneFailedEvent(t *testing.T) {
	tenantID, versionID, caller := uuid.New(), uuid.New(), uuid.New()
	held := fmt.Errorf("start workflow: %w", port.ErrExecutionHeld)

	setup := func(t *testing.T) (*service.InstanceService, *fakeInstanceRepo, *fakeOutbox, *fakeTemporalClient) {
		t.Helper()
		svc, instances, _, _, outbox, temporal, definitions, _ := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))
		temporal.startFunc = func(context.Context, port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
			return port.StartWorkflowOutput{}, held
		}
		return svc, instances, outbox, temporal
	}

	t.Run("the refusal leaves exactly one Failed event for the instance, after its Started event", func(t *testing.T) {
		svc, instances, outbox, _ := setup(t)

		_, err := svc.Start(context.Background(), startfixInput(tenantID, versionID, caller))

		require.ErrorIs(t, err, port.ErrDuplicateBusinessKey)
		require.Len(t, instances.byID, 1)
		var id uuid.UUID
		for k, inst := range instances.byID {
			id = k
			assert.Equal(t, domain.InstanceStatusFailed, inst.Status)
		}
		var types []string
		for _, env := range outbox.enqueued {
			types = append(types, env.Type)
			assert.Equal(t, "instances/"+id.String(), env.Subject)
		}
		assert.Equal(t, []string{domain.EventWorkflowInstanceStarted, domain.EventWorkflowInstanceFailed}, types)
		var payload map[string]any
		require.NoError(t, json.Unmarshal(outbox.enqueued[1].Payload, &payload))
		assert.Equal(t, "execution_id_held", payload["error_class"])
		assert.Equal(t, id.String(), payload["workflow_instance_id"])
		assert.Equal(t, caller.String(), payload["started_by_user_id"])
	})

	t.Run("the close and its event are written in one transaction", func(t *testing.T) {
		svc, _, outbox, _ := setup(t)
		tx := &startfixTransactor{}
		svc.Transactor = tx

		_, err := svc.Start(context.Background(), startfixInput(tenantID, versionID, caller))

		require.ErrorIs(t, err, port.ErrDuplicateBusinessKey)
		assert.Equal(t, 2, tx.runs, "one transaction for the start row, one for the refusal")
		assert.Equal(t, []string{domain.EventWorkflowInstanceStarted, domain.EventWorkflowInstanceFailed},
			[]string{outbox.enqueued[0].Type, outbox.enqueued[1].Type})
	})

	t.Run("a close that fails is logged and the answer stays a duplicate", func(t *testing.T) {
		svc, _, outbox, temporal := setup(t)
		logger := &startfixLogger{}
		svc.Log = logger
		temporal.startFunc = func(context.Context, port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
			outbox.enqueueErr = errors.New("outbox unavailable")
			return port.StartWorkflowOutput{}, held
		}

		_, err := svc.Start(context.Background(), startfixInput(tenantID, versionID, caller))

		require.ErrorIs(t, err, port.ErrDuplicateBusinessKey)
		require.NotEmpty(t, logger.errorFields)
		assert.Contains(t, logger.errorFields[0]["error"], "outbox unavailable")
	})
}

// startfixTransactor counts the transactions it runs.
type startfixTransactor struct{ runs int }

func (t *startfixTransactor) RunInTx(ctx context.Context, fn func(context.Context) error) error {
	t.runs++
	return fn(ctx)
}

func (t *startfixTransactor) RunInTxWithRetry(ctx context.Context, fn func(context.Context) error) error {
	return t.RunInTx(ctx, fn)
}

func TestInstanceService_Start_ARefusedHeldStartClosesOnlyAnInstanceThatNeverStarted(t *testing.T) {
	tenantID, versionID, caller := uuid.New(), uuid.New(), uuid.New()
	held := fmt.Errorf("start workflow: %w", port.ErrExecutionHeld)

	failedEvents := func(outbox *fakeOutbox) int {
		n := 0
		for _, env := range outbox.enqueued {
			if env.Type == domain.EventWorkflowInstanceFailed {
				n++
			}
		}
		return n
	}
	setup := func(t *testing.T, onStart func(svc *service.InstanceService, instances *fakeInstanceRepo, in port.StartWorkflowInput)) (*service.InstanceService, *fakeInstanceRepo, *fakeOutbox) {
		t.Helper()
		svc, instances, _, _, outbox, temporal, definitions, _ := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))
		temporal.startFunc = func(_ context.Context, in port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
			onStart(svc, instances, in)
			return port.StartWorkflowOutput{}, held
		}
		return svc, instances, outbox
	}

	t.Run("a second refusal of the same instance emits no second event", func(t *testing.T) {
		nested := false
		svc, instances, outbox := setup(t, func(svc *service.InstanceService, _ *fakeInstanceRepo, _ port.StartWorkflowInput) {
			if nested {
				return
			}
			nested = true
			_, err := svc.Start(context.Background(), startfixInput(tenantID, versionID, caller))
			require.ErrorIs(t, err, port.ErrDuplicateBusinessKey)
		})

		_, err := svc.Start(context.Background(), startfixInput(tenantID, versionID, caller))

		require.ErrorIs(t, err, port.ErrDuplicateBusinessKey)
		assert.Equal(t, 1, failedEvents(outbox), "two refused requests for one instance emit one Failed event")
		require.Len(t, instances.byID, 1)
	})

	t.Run("a row terminated meanwhile is left alone", func(t *testing.T) {
		svc, instances, outbox := setup(t, func(_ *service.InstanceService, instances *fakeInstanceRepo, in port.StartWorkflowInput) {
			instances.byID[in.InstanceID].Status = domain.InstanceStatusTerminated
		})

		_, err := svc.Start(context.Background(), startfixInput(tenantID, versionID, caller))

		require.ErrorIs(t, err, port.ErrDuplicateBusinessKey)
		for _, inst := range instances.byID {
			assert.Equal(t, domain.InstanceStatusTerminated, inst.Status)
		}
		assert.Zero(t, failedEvents(outbox))
	})

	t.Run("a row whose workflow a retry started is not overwritten", func(t *testing.T) {
		svc, instances, outbox := setup(t, func(_ *service.InstanceService, instances *fakeInstanceRepo, in port.StartWorkflowInput) {
			instances.byID[in.InstanceID].TemporalRunID = "run-by-the-retry"
		})

		_, err := svc.Start(context.Background(), startfixInput(tenantID, versionID, caller))

		require.ErrorIs(t, err, port.ErrDuplicateBusinessKey)
		for _, inst := range instances.byID {
			assert.Equal(t, domain.InstanceStatusRunning, inst.Status)
			assert.Equal(t, "run-by-the-retry", inst.TemporalRunID)
		}
		assert.Zero(t, failedEvents(outbox))
	})
}
