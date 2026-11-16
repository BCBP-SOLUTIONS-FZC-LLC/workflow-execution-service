package service_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	adapterhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

func TestTaskOverride_InstanceStatus_OnlyRunningOrDegradedIsAccepted(t *testing.T) {
	cases := []struct {
		status  domain.InstanceStatus
		refused bool
	}{
		{domain.InstanceStatusRunning, false},
		{domain.InstanceStatusDegraded, false},
		{domain.InstanceStatusPaused, true},
		{domain.InstanceStatusCompleted, true},
		{domain.InstanceStatusTerminated, true},
		{domain.InstanceStatusFailed, true},
	}
	for _, tc := range cases {
		t.Run(string(tc.status), func(t *testing.T) {
			h := newTaHarness(t, "single")
			h.instances.byID[h.instanceID].Status = tc.status

			_, err := h.svc.OverrideAssignee(context.Background(), h.overrideInput(uuid.New()))

			if !tc.refused {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, port.ErrInvalidInstanceState)
			h.requireNothingChanged(t)
			assert.Empty(t, h.elig.calls)
		})
	}
}

func TestTaskOverride_TaskWithNoActiveAssignee_AcceptsAndSignalsNoPreviousUser(t *testing.T) {
	cases := []struct {
		name  string
		mode  string
		setup func(h *taHarness)
	}{
		{"single mode, every assignment vacated", "single", func(h *taHarness) { h.vacateAll() }},
		{"all mode, nobody claimed yet", "all", func(*taHarness) {}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newTaHarness(t, tc.mode)
			tc.setup(h)
			newUser := uuid.New()
			in := h.overrideInput(newUser)

			got, err := h.svc.OverrideAssignee(context.Background(), in)

			require.NoError(t, err)
			assert.Equal(t, newUser, got.NewUserID)
			require.Len(t, h.overrides.creates, 1)
			assert.Equal(t, newUser, h.overrides.creates[0].Override.NewUserID)
			require.Len(t, h.outbox.signals, 1)
			wire := taDecodePayload(t, h.outbox.signals[0].Payload)
			assert.Empty(t, wire.OldUserID, "no previous user is carried, not the all-zero id")
			assert.Equal(t, newUser.String(), wire.NewUserID)
		})
	}
}

func TestTaskOverride_AllModeWithALead_RecordsTheLeadAsPreviousUser(t *testing.T) {
	h := newTaHarness(t, "all")
	h.other.IsLead = true

	got, err := h.svc.OverrideAssignee(context.Background(), h.overrideInput(uuid.New()))

	require.NoError(t, err)
	require.NotNil(t, got.PreviousUserID)
	assert.Equal(t, h.otherID, *got.PreviousUserID, "the caller row is listed first, but the lead is the holder")
	require.Len(t, h.overrides.creates, 1)
	require.NotNil(t, h.overrides.creates[0].Override.PreviousUserID)
	assert.Equal(t, h.otherID, *h.overrides.creates[0].Override.PreviousUserID)
	require.Len(t, h.outbox.signals, 1)
	assert.Equal(t, h.otherID.String(), taDecodePayload(t, h.outbox.signals[0].Payload).OldUserID)
}

func TestTaskOverride_NewUserAlreadyTheLead_IsANoOp(t *testing.T) {
	h := newTaHarness(t, "all")
	h.other.IsLead = true

	_, err := h.svc.OverrideAssignee(context.Background(), h.overrideInput(h.otherID))

	require.ErrorIs(t, err, port.ErrOverrideNoOp)
	h.requireNothingChanged(t)
}

func TestTaskOverride_NewUserNotEligible_StoresNothingAndSignalsNothing(t *testing.T) {
	h := newTaHarness(t, "single")
	h.elig.eligible = false
	newUser := uuid.New()

	_, err := h.svc.OverrideAssignee(context.Background(), h.overrideInput(newUser))

	require.ErrorIs(t, err, port.ErrAssigneeIneligible)
	h.requireNothingChanged(t)
	assert.Equal(t, []csdEligibilityCall{{h.tenantID, newUser, h.deptID, "approver"}}, h.elig.calls, "eligibility is asked once, for the plan's level")
}

func TestTaskOverride_EligibilityUnavailable_StoresNothingAndSignalsNothing(t *testing.T) {
	h := newTaHarness(t, "single")
	h.elig.err = fmt.Errorf("%w: eligibility endpoint returned 503", adapterhttp.ErrUpstreamUnavailable)

	_, err := h.svc.OverrideAssignee(context.Background(), h.overrideInput(uuid.New()))

	require.ErrorIs(t, err, adapterhttp.ErrUpstreamUnavailable)
	h.requireNothingChanged(t)
}

func TestTaskOverride_NodeMissingFromThePlan_IsRefusedBeforeAnythingIsStored(t *testing.T) {
	h := newTaHarness(t, "single")
	h.definitions.resp = publishedCompiledWorkflow(uuid.New(), h.versionID, compiledPlanJSONWithDept(t, dsl.DepartmentDef{
		ID: "finance", IAMDepartmentID: h.deptID.String(), Stages: []dsl.StageDef{csdOtherStage()},
	}))

	_, err := h.svc.OverrideAssignee(context.Background(), h.overrideInput(uuid.New()))

	require.ErrorIs(t, err, port.ErrAssigneeIneligible)
	assert.Contains(t, err.Error(), h.task.ID.String())
	h.requireNothingChanged(t)
	assert.Empty(t, h.elig.calls, "with no stage there is no level to ask about")
}

func TestTaskOverride_InstanceRowMissing_StoresNothing(t *testing.T) {
	h := newTaHarness(t, "single")
	delete(h.instances.byID, h.instanceID)

	_, err := h.svc.OverrideAssignee(context.Background(), h.overrideInput(uuid.New()))

	require.ErrorIs(t, err, port.ErrInstanceNotFound)
	h.requireNothingChanged(t)
}

func TestTaskOverride_Success_BumpsTheVersionStoresTheRowAndQueuesTheSignalInOneTransaction(t *testing.T) {
	h := newTaHarness(t, "single")
	newUser := uuid.New()
	in := h.overrideInput(newUser)

	got, err := h.svc.OverrideAssignee(context.Background(), in)

	require.NoError(t, err)
	assert.Equal(t, []string{"tx-begin", "bump", "override-create", "signal-enqueue", "tx-commit"}, h.log.events)
	assert.Equal(t, []taBumpCall{{h.tenantID, h.task.ID, csdTaskVersion, true}}, h.taskRepo.bumps)
	require.Len(t, h.overrides.creates, 1)
	assert.True(t, h.overrides.creates[0].InTx)
	assert.Equal(t, []string{h.tenantID.String()}, h.tx.guTenants, "the transaction carries the caller's tenant")
	assert.EqualValues(t, csdTaskVersion+1, h.task.RecordVersion)
	assert.EqualValues(t, csdTaskVersion+1, got.RecordVersion, "the answer carries the version the task now has")
	require.NotNil(t, got.PreviousUserID)
	assert.Equal(t, h.callerID, *got.PreviousUserID)
	assert.Empty(t, h.temporalLog.signals, "the workflow is not signalled directly; the relay delivers the queued signal")

	require.Len(t, h.outbox.signals, 1)
	sig := h.outbox.signals[0]
	assert.True(t, sig.InTx, "the signal is queued in the transaction that stores the row")
	assert.Equal(t, h.tenantID, sig.TenantID)
	assert.Equal(t, h.instanceID, sig.InstanceID)
	assert.Equal(t, h.instances.byID[h.instanceID].TemporalWorkflowID, sig.TemporalWorkflowID)
	assert.Equal(t, port.SignalInstanceReassign, sig.Name)
	wire := taDecodePayload(t, sig.Payload)
	assert.Equal(t, h.task.ID.String(), wire.TaskID)
	assert.Equal(t, h.callerID.String(), wire.OldUserID)
	assert.Equal(t, newUser.String(), wire.NewUserID)
	assert.Equal(t, in.ActorUserID.String(), wire.AdminUserID)
	assert.EqualValues(t, csdTaskVersion+1, wire.RecordVersion, "the move the workflow applies expects the bumped version")
}

func TestTaskOverride_CompetingAdmin_LosesWithRecordVersionConflictAndStoresNoRow(t *testing.T) {
	h := newTaHarness(t, "single")
	h.taskRepo.beforeBump = func() { h.task.RecordVersion++ }

	_, err := h.svc.OverrideAssignee(context.Background(), h.overrideInput(uuid.New()))

	require.ErrorIs(t, err, port.ErrRecordVersionConflict)
	assert.Empty(t, h.overrides.creates, "the loser stores no row")
	assert.Empty(t, h.outbox.signals, "the loser queues no signal")
	assert.Equal(t, 1, h.tx.rolledBack)
}

func TestTaskOverride_TwoAdminsWithTheSameVersion_OnlyTheFirstPersists(t *testing.T) {
	h := newTaHarness(t, "single")
	first, second := uuid.New(), uuid.New()

	_, err := h.svc.OverrideAssignee(context.Background(), h.overrideInput(first))
	require.NoError(t, err)
	_, err = h.svc.OverrideAssignee(context.Background(), h.overrideInput(second))

	require.ErrorIs(t, err, port.ErrRecordVersionConflict)
	require.Len(t, h.overrides.creates, 1)
	assert.Equal(t, first, h.overrides.creates[0].Override.NewUserID)
	assert.Len(t, h.outbox.signals, 1)
}

func TestTaskOverride_TemporalUnavailable_StillStoresTheMoveAndAnswersSuccess(t *testing.T) {
	h := newTaHarness(t, "single")
	h.temporalLog.err = errors.New("ta: temporal down")

	got, err := h.svc.OverrideAssignee(context.Background(), h.overrideInput(uuid.New()))

	require.NoError(t, err, "the signal is queued with the row, so a Temporal outage cannot lose the move")
	assert.EqualValues(t, csdTaskVersion+1, got.RecordVersion)
	assert.Len(t, h.overrides.creates, 1)
	assert.Len(t, h.outbox.signals, 1)
	assert.Empty(t, h.temporalLog.signals)
}

func TestTaskOverride_QueueingTheSignalFails_RollsBackTheRowAndTheBump(t *testing.T) {
	h := newTaHarness(t, "single")
	cause := errors.New("ta: outbox write failed")
	h.outbox.err = cause

	_, err := h.svc.OverrideAssignee(context.Background(), h.overrideInput(uuid.New()))

	require.ErrorIs(t, err, cause)
	assert.Equal(t, []string{"tx-begin", "bump", "override-create", "signal-enqueue", "tx-rollback"}, h.log.events)
	assert.Equal(t, 1, h.tx.rolledBack)
}

func TestTaskOverride_CreateFails_RollsBackTheBump(t *testing.T) {
	h := newTaHarness(t, "single")
	cause := errors.New("ta: insert failed")
	h.overrides.err = cause

	_, err := h.svc.OverrideAssignee(context.Background(), h.overrideInput(uuid.New()))

	require.ErrorIs(t, err, cause)
	assert.Equal(t, 1, h.tx.rolledBack, "the bump and the insert share one transaction")
	assert.Empty(t, h.outbox.signals)
}
