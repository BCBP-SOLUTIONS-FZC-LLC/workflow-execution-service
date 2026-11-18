package service_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

type fxaReversal struct {
	svc      *service.DelegationReconciler
	outbox   *fakeOutbox
	temporal *fakeTemporalClient
	instance *domain.Instance
	in       port.DelegationReversalInput
}

func fxaReversalOf(t *testing.T, status domain.InstanceStatus, availability string) *fxaReversal {
	t.Helper()
	svc, instances, tasks, assignments, outbox, temporal, definitions, _ := newDelegationReconcilerHarness()
	store := newFakeUserAvailability()
	svc.Availability = store
	tenantID, delegatorID, delegateID, delegationID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	inst, _, a := seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, delegateID, "biz-1", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)
	inst.Status = status
	a.Reason = "delegation:" + delegationID.String()
	if availability != "" {
		require.NoError(t, store.byRecord(tenantID, delegatorID, availability))
	}
	return &fxaReversal{svc: svc, outbox: outbox, temporal: temporal, instance: inst, in: port.DelegationReversalInput{
		TenantID: tenantID, DelegationID: delegationID, DelegatorID: delegatorID, DelegateID: delegateID, EndedReason: "expired",
	}}
}

func (r *fxaReversal) directSignals(name string) int {
	n := 0
	for _, s := range r.temporal.signals {
		if s.SignalName == name {
			n++
		}
	}
	return n
}

func TestDelegationReverse_AFailedCommitSendsNoPauseOrResumeToTheWorkflow(t *testing.T) {
	cases := []struct {
		name         string
		status       domain.InstanceStatus
		availability string
		signal       string
	}{
		{"a delegator who is out of office gets a pause", domain.InstanceStatusRunning, "ooo", port.SignalInstancePause},
		{"a delegator who is available gets a resume", domain.InstanceStatusPaused, "", port.SignalInstanceResume},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := fxaReversalOf(t, tc.status, tc.availability)
			r.svc.Transactor = fxaCommitFailsTx{}

			err := r.svc.Reverse(context.Background(), r.in)

			require.ErrorIs(t, err, errFxaCommit)
			assert.Zero(t, r.directSignals(tc.signal), "the workflow is not signalled before the move commits")
		})
	}
}

func TestDelegationReverse_ThePauseOrResumeIsRecordedWithTheMoveForTheRelay(t *testing.T) {
	cases := []struct {
		name         string
		status       domain.InstanceStatus
		availability string
		signal       string
	}{
		{"a delegator who is out of office gets a pause", domain.InstanceStatusRunning, "ooo", port.SignalInstancePause},
		{"a delegator who is available gets a resume", domain.InstanceStatusPaused, "", port.SignalInstanceResume},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := fxaReversalOf(t, tc.status, tc.availability)

			require.NoError(t, r.svc.Reverse(context.Background(), r.in))

			assert.Zero(t, r.directSignals(tc.signal), "nothing is sent to the workflow directly")
			recorded := r.outbox.signalsNamed(tc.signal)
			require.Len(t, recorded, 1, "one signal for the instance")
			assert.Equal(t, r.instance.ID, recorded[0].InstanceID)
			assert.Equal(t, r.instance.TemporalWorkflowID, recorded[0].TemporalWorkflowID)
			raw, err := json.Marshal(recorded[0].Payload)
			require.NoError(t, err)
			assert.Contains(t, string(raw), domain.InitiatorOOO)
			assert.Len(t, r.outbox.signalsNamed(port.SignalInstanceReassign), 1, "the move's own signal is recorded beside it")
			for _, tenant := range r.outbox.signalTenants {
				assert.Equal(t, r.in.TenantID, tenant)
			}
		})
	}
}
