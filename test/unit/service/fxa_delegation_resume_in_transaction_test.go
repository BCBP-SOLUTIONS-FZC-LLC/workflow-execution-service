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

var errFxaCommit = errors.New("commit failed")

// fxaCommitFailsTx runs the work and then reports that the commit failed.
type fxaCommitFailsTx struct{}

func (fxaCommitFailsTx) RunInTx(ctx context.Context, fn func(context.Context) error) error {
	if err := fn(ctx); err != nil {
		return err
	}
	return errFxaCommit
}

func (t fxaCommitFailsTx) RunInTxWithRetry(ctx context.Context, fn func(context.Context) error) error {
	return t.RunInTx(ctx, fn)
}

func fxaPausedRowOfAbsentDelegator(t *testing.T) (*dlgfixRecon, dlgfixRecRow) {
	t.Helper()
	h := dlgfixNewRecon()
	row := h.seed(t, "reviewer")
	row.instance.Status = domain.InstanceStatusPaused
	require.NoError(t, h.store.byRecord(h.tenantID, h.delegator, "ooo"))
	return h, row
}

func TestDelegationReroute_AFailedCommitSendsNoResumeToTheWorkflow(t *testing.T) {
	h, row := fxaPausedRowOfAbsentDelegator(t)
	h.svc.Transactor = fxaCommitFailsTx{}

	err := h.reroute()

	require.ErrorIs(t, err, errFxaCommit)
	assert.Zero(t, fxaDirectResumes(h, row.instance.ID), "the workflow is not resumed before the move commits")
}

func TestDelegationReroute_TheResumeIsRecordedWithTheMoveForTheRelay(t *testing.T) {
	h, row := fxaPausedRowOfAbsentDelegator(t)

	require.NoError(t, h.reroute())

	assert.Zero(t, fxaDirectResumes(h, row.instance.ID), "nothing is sent to the workflow directly")
	resumes := h.outbox.signalsNamed(port.SignalInstanceResume)
	require.Len(t, resumes, 1, "one resume for the instance")
	assert.Equal(t, row.instance.ID, resumes[0].InstanceID)
	assert.Equal(t, row.instance.TemporalWorkflowID, resumes[0].TemporalWorkflowID)
	raw, err := json.Marshal(resumes[0].Payload)
	require.NoError(t, err)
	assert.Contains(t, string(raw), domain.InitiatorOOO, "only the pause that out of office made can be released by it")
	assert.Len(t, h.outbox.signalsNamed(port.SignalInstanceReassign), 1, "the move's own signal is recorded beside it")
	for _, tenant := range h.outbox.signalTenants {
		assert.Equal(t, h.tenantID, tenant)
	}
}

func fxaDirectResumes(h *dlgfixRecon, instanceID uuid.UUID) int {
	n := 0
	for _, s := range h.temporal.signals {
		if s.SignalName == port.SignalInstanceResume && s.InstanceID == instanceID {
			n++
		}
	}
	return n
}
