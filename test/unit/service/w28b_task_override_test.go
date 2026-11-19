package service_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

// w28bOrderedEligibility puts each eligibility question in the same log as the transaction events.
type w28bOrderedEligibility struct {
	port.EligibilityChecker
	inner *taEligibility
	log   *taLog
	tx    *taTx
	inTx  []bool
}

func (e *w28bOrderedEligibility) CheckEligibility(ctx context.Context, tenantID, userID, departmentID uuid.UUID, level string) (bool, error) {
	e.log.add("eligibility")
	e.inTx = append(e.inTx, e.tx.open)
	return e.inner.CheckEligibility(ctx, tenantID, userID, departmentID, level)
}

func TestTaskOverride_Success_AsksEligibilityOnceBeforeAnythingIsStored(t *testing.T) {
	h := newTaHarness(t, "single")
	ordered := &w28bOrderedEligibility{inner: h.elig, log: h.log, tx: h.tx}
	h.svc.Eligibility = ordered
	newUser := uuid.New()

	_, err := h.svc.OverrideAssignee(context.Background(), h.overrideInput(newUser))

	require.NoError(t, err)
	assert.Equal(t, []string{"eligibility", "tx-begin", "bump", "override-create", "signal-enqueue", "tx-commit"}, h.log.events,
		"one eligibility question, asked before the transaction opens, and none after the row is stored")
	assert.Equal(t, []csdEligibilityCall{{h.tenantID, newUser, h.deptID, "approver"}}, h.elig.calls)
	assert.Equal(t, []bool{false}, ordered.inTx, "the question is not asked inside the transaction")
	require.Len(t, h.overrides.creates, 1)
	assert.Equal(t, newUser, h.overrides.creates[0].Override.NewUserID)
	assert.Equal(t, h.task.NodeKey, h.overrides.creates[0].Override.NodeKey)
	require.Len(t, h.outbox.signals, 1)
	wire := taDecodePayload(t, h.outbox.signals[0].Payload)
	assert.Equal(t, h.task.ID.String(), wire.TaskID)
	assert.Equal(t, newUser.String(), wire.NewUserID)
}

func TestTaskOverride_RecordVersionOtherThanTheTasks_IsRefusedBeforeEligibilityOrAnyWrite(t *testing.T) {
	cases := []struct {
		name    string
		version int64
	}{
		{name: "one higher than the task", version: csdTaskVersion + 1},
		{name: "far higher than the task", version: csdTaskVersion + 100},
		{name: "one lower than the task", version: csdTaskVersion - 1},
	}
	const table = "workflow_task"
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obstest.Init(t)
			before := conflictCount(t, table)
			h := newTaHarness(t, "single")
			in := h.overrideInput(uuid.New())
			in.RecordVersion = tc.version

			_, err := h.svc.OverrideAssignee(context.Background(), in)

			require.ErrorIs(t, err, port.ErrRecordVersionConflict)
			h.requireNothingChanged(t)
			assert.Empty(t, h.elig.calls, "a version conflict is answered before the membership service is asked")
			assert.Empty(t, h.log.events, "no transaction was opened")
			assert.Equal(t, before+1, conflictCount(t, table), "the conflict is counted for the task table")
		})
	}
}

func TestTaskOverride_TaskNoLongerOpen_AnswersAlreadyResolvedBeforeTheVersionCheck(t *testing.T) {
	cases := []struct {
		name    string
		status  domain.TaskStatus
		version int64
	}{
		{name: "completed", status: domain.TaskStatusCompleted, version: csdTaskVersion},
		{name: "deferred", status: domain.TaskStatusDeferred, version: csdTaskVersion},
		{name: "failed", status: domain.TaskStatusFailed, version: csdTaskVersion},
		{name: "superseded", status: domain.TaskStatusSuperseded, version: csdTaskVersion},
		{name: "superseded with a stale record version", status: domain.TaskStatusSuperseded, version: csdTaskVersion + 7},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newTaHarness(t, "single")
			h.task.Status = tc.status
			in := h.overrideInput(uuid.New())
			in.RecordVersion = tc.version

			_, err := h.svc.OverrideAssignee(context.Background(), in)

			require.ErrorIs(t, err, port.ErrNodeAlreadyResolved)
			require.NotErrorIs(t, err, port.ErrRecordVersionConflict, "the status is judged before the version")
			assert.Empty(t, h.elig.calls)
			assert.Empty(t, h.log.events, "no transaction was opened")
			assert.EqualValues(t, csdTaskVersion, h.task.RecordVersion)
			assert.Equal(t, tc.status, h.task.Status)
			h.requireNothingChanged(t)
		})
	}
}
