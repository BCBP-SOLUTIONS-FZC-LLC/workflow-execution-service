package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type tceTx struct {
	open      bool
	guTenants []string
}

func (t *tceTx) RunInTx(ctx context.Context, fn func(context.Context) error) error {
	guc, _ := pgcommon.GUCSetFromContext(ctx)
	t.guTenants = append(t.guTenants, guc.TenantID)
	t.open = true
	defer func() { t.open = false }()
	return fn(ctx)
}

func (t *tceTx) RunInTxWithRetry(ctx context.Context, fn func(context.Context) error) error {
	return t.RunInTx(ctx, fn)
}

type tceOutbox struct {
	*fakeOutbox
	tx         *tceTx
	enqueuedIn []bool
}

func (o *tceOutbox) Enqueue(ctx context.Context, env events.Envelope[json.RawMessage]) error {
	o.enqueuedIn = append(o.enqueuedIn, o.tx.open)
	return o.fakeOutbox.Enqueue(ctx, env)
}

type tceClaims struct {
	*csdAssignmentRepo
	tx        *tceTx
	claimedIn []bool
}

func (r *tceClaims) ClaimLead(ctx context.Context, tenantID, taskID, id uuid.UUID, version int64) (*domain.TaskAssignment, error) {
	r.claimedIn = append(r.claimedIn, r.tx.open)
	return r.csdAssignmentRepo.ClaimLead(ctx, tenantID, taskID, id, version)
}

func TestTaskServiceClaim_EnqueuesTheClaimedEventWithTheLeadInTheSameTransactionAsTheClaim(t *testing.T) {
	t.Parallel()
	h := csdNewTaskHarness(t, "all", csdReviewStage())
	tx := &tceTx{}
	outbox := &tceOutbox{fakeOutbox: h.outbox, tx: tx}
	claims := &tceClaims{csdAssignmentRepo: h.assignments, tx: tx}
	h.svc.Transactor, h.svc.Outbox, h.svc.Assignments = tx, outbox, claims

	_, err := h.svc.Claim(context.Background(), h.tenantID, h.task.ID, h.callerID, csdTaskVersion)

	require.NoError(t, err)
	require.Len(t, h.outbox.enqueued, 1)
	env := h.outbox.enqueued[0]
	assert.Equal(t, domain.EventWorkflowTaskClaimed, env.Type)
	assert.Equal(t, h.tenantID.String(), env.TenantID)
	assert.Equal(t, "instances/"+h.instanceID.String(), env.Subject)
	var payload domain.WorkflowTaskClaimedPayload
	require.NoError(t, json.Unmarshal(env.Payload, &payload))
	assert.Equal(t, h.callerID, payload.ClaimedByUserID)
	assert.Equal(t, h.task.ID, payload.TaskID)
	assert.Equal(t, "finance/review", payload.NodeKey)
	assert.Equal(t, h.deptID, payload.DepartmentID)
	assert.Equal(t, h.instanceID, payload.WorkflowInstanceID)
	assert.ElementsMatch(t, []uuid.UUID{h.callerID, h.otherID}, payload.AssigneeUserIDs)
	assert.Equal(t, []bool{true}, claims.claimedIn, "the lead is claimed inside the transaction")
	assert.Equal(t, []bool{true}, outbox.enqueuedIn, "the event is enqueued inside the same transaction")
	assert.Equal(t, []string{h.tenantID.String()}, tx.guTenants, "the transaction runs under the tenant")
}

func TestTaskServiceClaim_ARepeatByTheLeadEnqueuesNoSecondEvent(t *testing.T) {
	t.Parallel()
	h := csdNewTaskHarness(t, "all", csdReviewStage())
	_, err := h.svc.Claim(context.Background(), h.tenantID, h.task.ID, h.callerID, csdTaskVersion)
	require.NoError(t, err)

	_, err = h.svc.Claim(context.Background(), h.tenantID, h.task.ID, h.callerID, csdTaskVersion+1)

	require.NoError(t, err)
	assert.Len(t, h.outbox.enqueued, 1)
}

func TestTaskServiceClaim_AFailedEnqueueFailsTheClaim(t *testing.T) {
	t.Parallel()
	h := csdNewTaskHarness(t, "all", csdReviewStage())
	h.outbox.enqueueErr = errors.New("outbox down")

	_, err := h.svc.Claim(context.Background(), h.tenantID, h.task.ID, h.callerID, csdTaskVersion)

	require.ErrorIs(t, err, h.outbox.enqueueErr)
}

func TestTaskServiceClaim_AClaimRefusedByTheVersionEnqueuesNoEvent(t *testing.T) {
	t.Parallel()
	h := csdNewTaskHarness(t, "all", csdReviewStage())

	_, err := h.svc.Claim(context.Background(), h.tenantID, h.task.ID, h.callerID, csdTaskVersion-1)

	require.ErrorIs(t, err, port.ErrRecordVersionConflict)
	assert.Empty(t, h.outbox.enqueued)
}
