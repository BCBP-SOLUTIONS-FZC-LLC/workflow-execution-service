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

// fxcFailFirstTx fails the first transaction with a version conflict before its work runs.
type fxcFailFirstTx struct {
	port.Transactor
	failed bool
}

func (t *fxcFailFirstTx) RunInTxWithRetry(ctx context.Context, fn func(context.Context) error) error {
	if !t.failed {
		t.failed = true
		return domain.ErrRecordVersionConflict
	}
	return t.Transactor.RunInTxWithRetry(ctx, fn)
}

func TestInstanceService_Terminate_AFailedCommitLeavesTheRowRunningAndTheRetryFinishesTheTermination(t *testing.T) {
	svc, instances, _, _, _, temporal, _, _ := newInstanceServiceHarness()
	svc.Transactor = &fxcFailFirstTx{Transactor: svc.Transactor}
	tenantID := uuid.New()
	inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning, RecordVersion: 1, TemporalWorkflowID: "tenant:biz", TemporalRunID: "run-1"}
	instances.byID[inst.ID] = inst

	require.Error(t, svc.Terminate(context.Background(), tenantID, inst.ID, uuid.New(), "reason"))
	assert.Equal(t, domain.InstanceStatusRunning, inst.Status, "the failed commit leaves the row as it was, so the caller can retry")

	require.NoError(t, svc.Terminate(context.Background(), tenantID, inst.ID, uuid.New(), "reason"))
	assert.Equal(t, domain.InstanceStatusTerminated, inst.Status, "the retry completes the termination")
	for _, call := range temporal.terminateCalls() {
		assert.Equal(t, terminateCall{TemporalWorkflowID: "tenant:biz", RunID: "run-1", Reason: "reason"}, call)
	}
}
