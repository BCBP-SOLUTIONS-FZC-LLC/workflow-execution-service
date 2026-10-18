package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestInstanceService_StartRetry(t *testing.T) {
	tenantID, versionID, caller := uuid.New(), uuid.New(), uuid.New()
	input := func(by uuid.UUID, version uuid.UUID) port.StartInstanceInput {
		return port.StartInstanceInput{
			TenantID: tenantID, WorkflowVersionID: version, SubjectID: "SUBJ-1", BusinessKey: "TND-001", StartedByUserID: by,
		}
	}

	t.Run("the run id is recorded once the workflow starts", func(t *testing.T) {
		svc, instances, _, _, _, _, definitions, _ := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))

		got, err := svc.Start(context.Background(), input(caller, versionID))
		require.NoError(t, err)
		assert.Equal(t, "run-1", got.TemporalRunID)
		assert.Equal(t, "run-1", instances.byID[got.ID].TemporalRunID)
	})

	t.Run("a failed run id write does not fail a start that succeeded", func(t *testing.T) {
		svc, instances, _, _, _, _, definitions, _ := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))
		instances.runIDErr = errors.New("database unavailable")

		got, err := svc.Start(context.Background(), input(caller, versionID))
		require.NoError(t, err)
		assert.Equal(t, port.InstanceStatusRunning, got.Status)
	})

	t.Run("the same request sent again completes a start that failed after the row was written", func(t *testing.T) {
		svc, instances, _, _, outbox, temporal, definitions, _ := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))
		starts := 0
		temporal.startFunc = func(_ context.Context, in port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
			starts++
			if starts <= 3 {
				return port.StartWorkflowOutput{}, errors.New("temporal frontend unreachable")
			}
			return port.StartWorkflowOutput{TemporalWorkflowID: in.TemporalWorkflowID, TemporalRunID: "run-9"}, nil
		}

		_, err := svc.Start(context.Background(), input(caller, versionID))
		require.Error(t, err)
		require.Len(t, instances.byID, 1)

		got, err := svc.Start(context.Background(), input(caller, versionID))
		require.NoError(t, err)
		assert.Len(t, instances.byID, 1, "the retry must not write a second row")
		assert.Len(t, outbox.enqueued, 1, "the retry must not write a second started event")
		assert.Equal(t, "run-9", got.TemporalRunID)
		assert.Equal(t, port.InstanceStatusRunning, got.Status)
		assert.Equal(t, 4, starts)
	})

	t.Run("a retry that fails again leaves the one row and reports the failure", func(t *testing.T) {
		svc, instances, _, _, _, temporal, definitions, _ := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))
		temporal.startFunc = func(context.Context, port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
			return port.StartWorkflowOutput{}, errors.New("temporal frontend unreachable")
		}

		_, err := svc.Start(context.Background(), input(caller, versionID))
		require.Error(t, err)
		_, err = svc.Start(context.Background(), input(caller, versionID))
		require.Error(t, err)
		assert.NotErrorIs(t, err, port.ErrDuplicateBusinessKey)
		assert.Len(t, instances.byID, 1)
	})

	t.Run("a start whose run id is recorded is returned unchanged and issues nothing", func(t *testing.T) {
		svc, instances, _, _, outbox, temporal, definitions, _ := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))
		starts := 0
		temporal.startFunc = func(_ context.Context, in port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
			starts++
			return port.StartWorkflowOutput{TemporalWorkflowID: in.TemporalWorkflowID, TemporalRunID: "run-1"}, nil
		}

		first, err := svc.Start(context.Background(), input(caller, versionID))
		require.NoError(t, err)
		again, err := svc.Start(context.Background(), input(caller, versionID))
		require.NoError(t, err)
		assert.Equal(t, first.ID, again.ID)
		assert.Equal(t, 1, starts)
		assert.Len(t, instances.byID, 1)
		assert.Len(t, outbox.enqueued, 1)
	})

	failedStart := func(t *testing.T) (*fakeInstanceRepo, func(port.StartInstanceInput) error) {
		svc, instances, _, _, _, temporal, definitions, _ := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))
		failing := true
		temporal.startFunc = func(_ context.Context, in port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
			if failing {
				return port.StartWorkflowOutput{}, errors.New("temporal frontend unreachable")
			}
			return port.StartWorkflowOutput{TemporalWorkflowID: in.TemporalWorkflowID, TemporalRunID: "run-1"}, nil
		}
		_, err := svc.Start(context.Background(), input(caller, versionID))
		require.Error(t, err)
		failing = false
		return instances, func(in port.StartInstanceInput) error {
			_, err := svc.Start(context.Background(), in)
			return err
		}
	}

	t.Run("a start by another caller is a duplicate", func(t *testing.T) {
		instances, start := failedStart(t)
		err := start(input(uuid.New(), versionID))
		assert.ErrorIs(t, err, port.ErrDuplicateBusinessKey)
		assert.Len(t, instances.byID, 1)
	})

	t.Run("a start of another version is a duplicate", func(t *testing.T) {
		instances, start := failedStart(t)
		err := start(input(caller, uuid.New()))
		assert.ErrorIs(t, err, port.ErrDuplicateBusinessKey)
		assert.Len(t, instances.byID, 1)
	})

	t.Run("a live instance that is not RUNNING is a duplicate", func(t *testing.T) {
		for _, status := range []domain.InstanceStatus{domain.InstanceStatusPaused, domain.InstanceStatusDegraded} {
			instances, start := failedStart(t)
			for _, inst := range instances.byID {
				inst.Status = status
			}
			assert.ErrorIs(t, start(input(caller, versionID)), port.ErrDuplicateBusinessKey, status)
		}
	})

	t.Run("a start carrying another context or override map is a duplicate", func(t *testing.T) {
		for name, mutate := range map[string]func(*port.StartInstanceInput){
			"context":      func(in *port.StartInstanceInput) { in.ContextJSON = json.RawMessage(`{"amount":900}`) },
			"override map": func(in *port.StartInstanceInput) { in.OverrideMap = map[string]uuid.UUID{"finance/review": uuid.New()} },
		} {
			svc, instances, _, _, _, temporal, definitions, _ := newInstanceServiceHarness()
			definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))
			original := input(caller, versionID)
			original.ContextJSON = json.RawMessage(`{"amount":100}`)
			_, err := svc.Start(context.Background(), original)
			require.NoError(t, err, name)

			changed := original
			mutate(&changed)
			_, err = svc.Start(context.Background(), changed)
			assert.ErrorIs(t, err, port.ErrDuplicateBusinessKey, name)
			assert.Len(t, instances.byID, 1, name)
			assert.Len(t, temporal.terminateCalls(), 0, name)
		}
	})

	t.Run("a context written with other spacing or key order is the same start", func(t *testing.T) {
		svc, instances, _, _, _, _, definitions, _ := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))
		first := input(caller, versionID)
		first.ContextJSON = json.RawMessage(`{"a":1,"b":[1,2]}`)
		got, err := svc.Start(context.Background(), first)
		require.NoError(t, err)

		again := first
		again.ContextJSON = json.RawMessage(`{ "b": [1, 2], "a": 1 }`)
		same, err := svc.Start(context.Background(), again)
		require.NoError(t, err)
		assert.Equal(t, got.ID, same.ID)
		assert.Len(t, instances.byID, 1)
	})

	t.Run("a retry completes the start although the version was archived meanwhile", func(t *testing.T) {
		svc, instances, _, _, _, temporal, definitions, _ := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))
		failing := true
		temporal.startFunc = func(_ context.Context, in port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
			if failing {
				return port.StartWorkflowOutput{}, errors.New("temporal frontend unreachable")
			}
			return port.StartWorkflowOutput{TemporalWorkflowID: in.TemporalWorkflowID, TemporalRunID: "run-4"}, nil
		}
		_, err := svc.Start(context.Background(), input(caller, versionID))
		require.Error(t, err)

		failing = false
		definitions.resp = &port.CompiledWorkflow{Status: "ARCHIVED", IsValid: true}
		got, err := svc.Start(context.Background(), input(caller, versionID))
		require.NoError(t, err)
		assert.Equal(t, "run-4", got.TemporalRunID)
		assert.Len(t, instances.byID, 1)
	})

	t.Run("a retry completes the start although the plan read fails", func(t *testing.T) {
		svc, _, _, _, _, temporal, definitions, _ := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))
		failing := true
		temporal.startFunc = func(_ context.Context, in port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
			if failing {
				return port.StartWorkflowOutput{}, errors.New("temporal frontend unreachable")
			}
			return port.StartWorkflowOutput{TemporalWorkflowID: in.TemporalWorkflowID, TemporalRunID: "run-4"}, nil
		}
		_, err := svc.Start(context.Background(), input(caller, versionID))
		require.Error(t, err)

		failing = false
		definitions.resp, definitions.err = nil, errors.New("definition service unavailable")
		got, err := svc.Start(context.Background(), input(caller, versionID))
		require.NoError(t, err)
		assert.Equal(t, "run-4", got.TemporalRunID)
	})

	t.Run("the first start sends the workflow the request's context, override map and the resolved queue", func(t *testing.T) {
		svc, _, _, _, _, temporal, definitions, _ := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))
		var sent port.StartWorkflowInput
		temporal.startFunc = func(_ context.Context, in port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
			sent = in
			return port.StartWorkflowOutput{TemporalWorkflowID: in.TemporalWorkflowID, TemporalRunID: "run-1"}, nil
		}
		assignee := uuid.New()
		in := input(caller, versionID)
		in.ContextJSON = json.RawMessage(`{"amount":100}`)
		in.OverrideMap = map[string]uuid.UUID{"finance/review": assignee}

		_, err := svc.Start(context.Background(), in)
		require.NoError(t, err)
		assert.JSONEq(t, `{"amount":100}`, sent.ContextJSON)
		assert.Equal(t, map[string]string{"finance/review": assignee.String()}, sent.OverrideMap)
		assert.Equal(t, "wf-queue-default", sent.TaskQueue)
		assert.Equal(t, tenantID.String()+":SUBJ-1:TND-001", sent.TemporalWorkflowID)
	})

	t.Run("a row whose run id was never recorded is closed by the current execution", func(t *testing.T) {
		svc, instances, _, _, _, temporal, definitions, _ := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))
		instances.runIDErr = errors.New("database unavailable")
		got, err := svc.Start(context.Background(), input(caller, versionID))
		require.NoError(t, err)

		require.NoError(t, svc.Terminate(context.Background(), tenantID, got.ID, uuid.New(), "reason"))
		for _, c := range temporal.terminateCalls() {
			assert.Empty(t, c.RunID)
		}
	})

	t.Run("a failed lookup of a live instance fails the start", func(t *testing.T) {
		svc, instances, _, _, _, _, definitions, _ := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))
		instances.liveErr = errors.New("database unavailable")

		_, err := svc.Start(context.Background(), input(caller, versionID))
		require.Error(t, err)
		assert.Empty(t, instances.byID)
	})

	t.Run("a stored context or override map that cannot be read is not the same start", func(t *testing.T) {
		for name, corrupt := range map[string]func(*domain.Instance){
			"context":      func(i *domain.Instance) { i.ContextJSON = []byte(`{`) },
			"override map": func(i *domain.Instance) { i.OverrideMap = []byte(`[`) },
		} {
			svc, instances, _, _, _, _, definitions, _ := newInstanceServiceHarness()
			definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))
			_, err := svc.Start(context.Background(), input(caller, versionID))
			require.NoError(t, err, name)
			for _, inst := range instances.byID {
				corrupt(inst)
			}
			_, err = svc.Start(context.Background(), input(caller, versionID))
			assert.ErrorIs(t, err, port.ErrDuplicateBusinessKey, name)
		}
	})

	t.Run("a close of an instance terminated while starting names the run", func(t *testing.T) {
		svc, _, _, _, _, temporal, definitions, _ := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))
		temporal.startFunc = func(_ context.Context, in port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
			require.NoError(t, svc.Terminate(context.Background(), tenantID, in.InstanceID, uuid.New(), "admin requested"))
			return port.StartWorkflowOutput{TemporalWorkflowID: in.TemporalWorkflowID, TemporalRunID: "run-1"}, nil
		}

		got, err := svc.Start(context.Background(), input(caller, versionID))
		require.NoError(t, err)
		assert.Equal(t, port.InstanceStatusTerminated, got.Status)
		calls := temporal.terminateCalls()
		require.NotEmpty(t, calls)
		assert.Equal(t, "run-1", calls[len(calls)-1].RunID)
	})
}

func TestInstanceService_TerminateNamesTheRun(t *testing.T) {
	svc, instances, _, _, _, temporal, _, _ := newInstanceServiceHarness()
	tenantID := uuid.New()
	inst := &domain.Instance{
		ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning, RecordVersion: 1,
		TemporalWorkflowID: "tenant:subject:biz", TemporalRunID: "run-5",
	}
	instances.byID[inst.ID] = inst

	require.NoError(t, svc.Terminate(context.Background(), tenantID, inst.ID, uuid.New(), "reason"))

	calls := temporal.terminateCalls()
	require.Len(t, calls, 2)
	for _, c := range calls {
		assert.Equal(t, "run-5", c.RunID)
	}
}
