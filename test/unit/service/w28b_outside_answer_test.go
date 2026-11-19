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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

var errW28bSignalRejected = errors.New("w28b signal rejected")

// w28bCountQueries replaces the harness's workflow query with one that also counts how often it is asked.
func w28bCountQueries(t *testing.T, temporal *fakeTemporalClient, workflowID string, waiting func() []string) *int {
	t.Helper()
	asked := 0
	temporal.queryFunc = func(_ context.Context, queriedWorkflowID, queryType string, out any) error {
		asked++
		assert.Equal(t, workflowID, queriedWorkflowID)
		assert.Equal(t, port.QueryWaitingExternal, queryType)
		raw, err := json.Marshal(waiting())
		require.NoError(t, err)
		return json.Unmarshal(raw, out)
	}
	return &asked
}

func TestAnswerOutsideWait_UnknownInstance_IsNotFoundForACallerWhoWouldAlsoBeRefusedAsNonAssignee(t *testing.T) {
	stranger := port.ReadScope{CallerUserID: uuid.New(), Departments: []port.DepartmentRole{{DepartmentID: uuid.New()}}}
	cases := []struct {
		name       string
		knownID    bool
		wantErr    error
		wantNotErr error
	}{
		{name: "no such instance is not found, not refused", knownID: false, wantErr: port.ErrInstanceNotFound, wantNotErr: port.ErrNotAuthorizedForRead},
		{name: "an instance the caller has no task on is refused", knownID: true, wantErr: port.ErrNotAuthorizedForRead, wantNotErr: port.ErrInstanceNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, inst, temporal := outsideAnswerHarness(t, domain.InstanceStatusRunning, outsideKey)
			asked := w28bCountQueries(t, temporal, inst.TemporalWorkflowID, func() []string { return []string{outsideKey} })
			instanceID := uuid.New()
			if tc.knownID {
				instanceID = inst.ID
			}

			err := svc.AnswerOutsideWait(context.Background(), inst.TenantID, instanceID, stranger, outsideKey, nil)

			require.ErrorIs(t, err, tc.wantErr)
			assert.NotErrorIs(t, err, tc.wantNotErr)
			assert.Empty(t, temporal.signals)
			assert.Zero(t, *asked, "the workflow is not asked")
		})
	}
}

func TestAnswerOutsideWait_UnknownKeyOnAnInstanceThatIsNotRunning_IsWaitNotFoundNotAStateError(t *testing.T) {
	cases := []struct {
		name   string
		status domain.InstanceStatus
	}{
		{name: "completed", status: domain.InstanceStatusCompleted},
		{name: "terminated", status: domain.InstanceStatusTerminated},
		{name: "failed", status: domain.InstanceStatusFailed},
		{name: "paused", status: domain.InstanceStatusPaused},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, inst, temporal := outsideAnswerHarness(t, tc.status, "finance/Unknown")
			asked := w28bCountQueries(t, temporal, inst.TemporalWorkflowID, func() []string { return []string{"finance/Unknown"} })

			err := svc.AnswerOutsideWait(context.Background(), inst.TenantID, inst.ID, adminScope(), "finance/Unknown", nil)

			require.ErrorIs(t, err, port.ErrWaitNotFound)
			assert.NotErrorIs(t, err, port.ErrInstanceAlreadyTerminal)
			assert.NotErrorIs(t, err, port.ErrInvalidInstanceState)
			assert.Empty(t, temporal.signals)
			assert.Zero(t, *asked, "the key is refused before the workflow is asked")
		})
	}
}

func TestAnswerOutsideWait_AnsweredTwice_TheSecondFindsTheKeyConsumedAndIsRefused(t *testing.T) {
	svc, inst, temporal := outsideAnswerHarness(t, domain.InstanceStatusRunning, outsideKey)
	waiting := []string{outsideKey}
	var queried [][]string
	asked := w28bCountQueries(t, temporal, inst.TemporalWorkflowID, func() []string {
		queried = append(queried, append([]string(nil), waiting...))
		return waiting
	})
	temporal.signalFunc = func(_ context.Context, _ string, _ uuid.UUID, name string, _ any) error {
		assert.Equal(t, port.SignalInstanceExternalAnswer, name)
		waiting = nil
		return nil
	}

	first := svc.AnswerOutsideWait(context.Background(), inst.TenantID, inst.ID, adminScope(), outsideKey, map[string]any{"bid": "1"})
	second := svc.AnswerOutsideWait(context.Background(), inst.TenantID, inst.ID, adminScope(), outsideKey, map[string]any{"bid": "2"})

	require.NoError(t, first)
	require.ErrorIs(t, second, port.ErrNotWaiting)
	assert.Equal(t, 2, *asked, "each answer asks the workflow what it waits for")
	assert.Equal(t, [][]string{{outsideKey}, nil}, queried, "the consumed key has left the waiting list")
	require.Len(t, temporal.signals, 1, "only the first answer is signalled")
	assert.Equal(t, inst.TemporalWorkflowID, temporal.signals[0].TemporalWorkflowID)
	assert.Equal(t, inst.ID, temporal.signals[0].InstanceID)
	raw, err := json.Marshal(temporal.signals[0].Payload)
	require.NoError(t, err)
	assert.JSONEq(t, `{"Key":"finance/Receive_Bid","Variables":{"bid":"1"}}`, string(raw))
}

func TestAnswerOutsideWait_InstanceOfAnotherTenant_IsNotFoundAndNothingIsRead(t *testing.T) {
	scopes := map[string]port.ReadScope{
		"admin":     adminScope(),
		"non-admin": {CallerUserID: uuid.New()},
	}
	for name, scope := range scopes {
		t.Run(name, func(t *testing.T) {
			w := cseNewOutsideWorld(compiledPlanJSON(t,
				dsl.StageDef{Type: "receive_task", NodeID: "Receive_Bid", Extras: map[string]string{"message": "bid-received"}}))
			otherTenant := uuid.New()

			err := w.svc.AnswerOutsideWait(context.Background(), otherTenant, w.instance.ID, scope, outsideKey, nil)

			require.ErrorIs(t, err, port.ErrInstanceNotFound)
			assert.Equal(t, []cseLookup{{Tenant: otherTenant, ID: w.instance.ID}}, w.instances.calls, "the lookup is made under the caller's tenant")
			assert.Empty(t, w.defs.calls, "the other tenant's plan is not read")
			w.requireUntouchedWorkflow(t)
		})
	}

	t.Run("the owning tenant finds the same instance", func(t *testing.T) {
		w := cseNewOutsideWorld(compiledPlanJSON(t,
			dsl.StageDef{Type: "receive_task", NodeID: "Receive_Bid", Extras: map[string]string{"message": "bid-received"}}))

		err := w.answer(outsideKey)

		require.NotErrorIs(t, err, port.ErrInstanceNotFound)
		require.ErrorIs(t, err, port.ErrNotWaiting, "the lookup succeeded; the fake workflow reports nothing waiting")
	})
}

func TestAnswerOutsideWait_CacheMissAndTheCacheWriteFails_StillSignalsAndOnlyLogs(t *testing.T) {
	svc, inst, temporal := outsideAnswerHarness(t, domain.InstanceStatusRunning, outsideKey)
	cache := newFakeCacheStore()
	cache.setErr = errors.New("w28b cache write refused")
	log := &fakeLogger{}
	svc.Cache = cache
	svc.Log = log

	err := svc.AnswerOutsideWait(context.Background(), inst.TenantID, inst.ID, adminScope(), outsideKey, map[string]any{"bid": "9"})

	require.NoError(t, err, "a failed cache write must not fail the answer")
	require.Len(t, temporal.signals, 1)
	assert.Equal(t, port.SignalInstanceExternalAnswer, temporal.signals[0].SignalName)
	assert.Equal(t, []string{"compiled-plan cache write-through failed"}, log.warnCalls)
	assert.Equal(t, 1, warnsWithCause(log, "w28b cache write refused"))
	assert.Empty(t, log.errorCalls)
}

func TestAnswerOutsideWait_SignalFails_TheServiceReturnsTheWorkflowClientsErrorUnchanged(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{name: "the execution no longer exists", err: fmt.Errorf("signal workflow: %w: %w", port.ErrWorkflowNotFound, errW28bSignalRejected)},
		{name: "the cluster cannot take the signal", err: fmt.Errorf("signal workflow: %w: %w", port.ErrTemporalUnavailable, errW28bSignalRejected)},
		{name: "the cluster refused the request", err: errW28bSignalRejected},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, inst, temporal := outsideAnswerHarness(t, domain.InstanceStatusRunning, outsideKey)
			temporal.signalFunc = func(context.Context, string, uuid.UUID, string, any) error { return tc.err }

			err := svc.AnswerOutsideWait(context.Background(), inst.TenantID, inst.ID, adminScope(), outsideKey, nil)

			require.Same(t, tc.err, err, "no wrapping that could change how the handler classifies the failure")
			require.Len(t, temporal.signals, 1, "the signal was tried once")
		})
	}
}

func TestAnswerOutsideWait_ReceiveTaskFedByAPlanSendTaskOrAUserTaskKey_IsWaitNotFound(t *testing.T) {
	receiveAndSend := compiledPlanJSON(t,
		dsl.StageDef{Type: "receive_task", NodeID: "Receive_Bid", Extras: map[string]string{"message": "bid-received"}},
		dsl.StageDef{Type: "send_task", NodeID: "Send_Bid", Extras: map[string]string{"message": "bid-received"}},
	)
	receiveAndOtherSend := compiledPlanJSON(t,
		dsl.StageDef{Type: "receive_task", NodeID: "Receive_Bid", Extras: map[string]string{"message": "bid-received"}},
		dsl.StageDef{Type: "send_task", NodeID: "Send_Other", Extras: map[string]string{"message": "something-else"}},
	)
	userTaskPlan := compiledPlanJSON(t,
		dsl.StageDef{Type: "receive_task", NodeID: "Receive_Bid", Extras: map[string]string{"message": "bid-received"}},
		dsl.StageDef{Type: "userTask", NodeID: "review", Role: "approver"},
	)
	cases := []struct {
		name       string
		plan       string
		key        string
		answerable bool
	}{
		{name: "a receive task whose message a send task of the plan sends", plan: receiveAndSend, key: outsideKey},
		{name: "the send task itself", plan: receiveAndSend, key: "finance/Send_Bid"},
		{name: "a user task node key", plan: userTaskPlan, key: "finance/review"},
		{name: "a receive task whose message no send task of the plan sends", plan: receiveAndOtherSend, key: outsideKey, answerable: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, inst, temporal, queries := csbOutsideSetup(t, tc.plan, domain.InstanceStatusRunning, tc.key)

			err := svc.AnswerOutsideWait(context.Background(), inst.TenantID, inst.ID, adminScope(), tc.key, nil)

			if tc.answerable {
				require.NoError(t, err)
				require.Len(t, temporal.signals, 1)
				return
			}
			require.ErrorIs(t, err, port.ErrWaitNotFound)
			assert.Empty(t, temporal.signals)
			assert.Zero(t, *queries, "the workflow is not asked before the plan refuses the key")
		})
	}
}
