package service_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// taLog is the order in which the fakes saw their calls.
type taLog struct{ events []string }

func (l *taLog) add(e string) { l.events = append(l.events, e) }

// taTx records the tenant carried on the context it was given and whether a callback is open.
type taTx struct {
	log        *taLog
	open       bool
	guTenants  []string
	rolledBack int
}

func (t *taTx) RunInTx(ctx context.Context, fn func(context.Context) error) error {
	return t.run(ctx, fn)
}

func (t *taTx) RunInTxWithRetry(ctx context.Context, fn func(context.Context) error) error {
	return t.run(ctx, fn)
}

func (t *taTx) run(ctx context.Context, fn func(context.Context) error) error {
	guc, _ := pgcommon.GUCSetFromContext(ctx)
	t.guTenants = append(t.guTenants, guc.TenantID)
	t.log.add("tx-begin")
	t.open = true
	err := fn(ctx)
	t.open = false
	if err != nil {
		t.rolledBack++
		t.log.add("tx-rollback")
		return err
	}
	t.log.add("tx-commit")
	return nil
}

type taBumpCall struct {
	TenantID      uuid.UUID
	TaskID        uuid.UUID
	RecordVersion int64
	InTx          bool
}

// taTasks adds the two reads and the version bump the admin actions use to the tenant-scoped task fake.
type taTasks struct {
	*csdTaskRepo
	tx         *taTx
	log        *taLog
	bumps      []taBumpCall
	beforeBump func()
}

func (r *taTasks) GetByInstanceAndNode(_ context.Context, tenantID, instanceID uuid.UUID, nodeKey string) (*domain.Task, error) {
	for _, t := range r.byID {
		if t.TenantID == tenantID && t.WorkflowInstanceID == instanceID && t.NodeKey == nodeKey {
			return t, nil
		}
	}
	return nil, domain.ErrNotFound
}

func (r *taTasks) BumpRecordVersion(_ context.Context, tenantID, id uuid.UUID, recordVersion int64) (*domain.Task, error) {
	r.bumps = append(r.bumps, taBumpCall{tenantID, id, recordVersion, r.tx.open})
	r.log.add("bump")
	if r.beforeBump != nil {
		r.beforeBump()
	}
	task, ok := r.byID[id]
	if !ok || task.TenantID != tenantID {
		return nil, domain.ErrNotFound
	}
	if task.RecordVersion != recordVersion {
		return nil, domain.ErrRecordVersionConflict
	}
	task.RecordVersion++
	return task, nil
}

type taCreateCall struct {
	Override domain.AssigneeOverride
	InTx     bool
}

type taOverrides struct {
	port.AssigneeOverrideRepository
	tx      *taTx
	log     *taLog
	creates []taCreateCall
	err     error
}

func (r *taOverrides) Create(_ context.Context, o *domain.AssigneeOverride) error {
	r.creates = append(r.creates, taCreateCall{*o, r.tx.open})
	r.log.add("override-create")
	return r.err
}

type taOutboxSignal struct {
	port.WorkflowSignal
	InTx bool
}

// taSignalOutbox records the signals queued for the relay and whether each was queued inside a transaction.
type taSignalOutbox struct {
	tx      *taTx
	log     *taLog
	signals []taOutboxSignal
	err     error
}

func (o *taSignalOutbox) EnqueueSignal(_ context.Context, sig port.WorkflowSignal) error {
	o.signals = append(o.signals, taOutboxSignal{sig, o.tx.open})
	o.log.add("signal-enqueue")
	return o.err
}

type taEligibility struct {
	port.EligibilityChecker
	eligible bool
	err      error
	calls    []csdEligibilityCall
}

func (e *taEligibility) CheckEligibility(_ context.Context, tenantID, userID, departmentID uuid.UUID, level string) (bool, error) {
	e.calls = append(e.calls, csdEligibilityCall{tenantID, userID, departmentID, level})
	return e.eligible && e.err == nil, e.err
}

type taSignal struct {
	csdPayloadSignal
	InTx bool
}

type taTemporal struct {
	port.TemporalClient
	tx       *taTx
	log      *taLog
	signals  []taSignal
	updates  []csdPayloadSignal
	onUpdate func()
	err      error
}

func (c *taTemporal) SignalWorkflow(_ context.Context, workflowID string, instanceID uuid.UUID, name string, payload any) error {
	c.signals = append(c.signals, taSignal{csdPayloadSignal{csdSignalCall{workflowID, instanceID, name}, payload}, c.tx.open})
	c.log.add("signal")
	return c.err
}

func (c *taTemporal) UpdateWorkflow(_ context.Context, workflowID string, instanceID uuid.UUID, name string, payload any) error {
	c.updates = append(c.updates, csdPayloadSignal{csdSignalCall{workflowID, instanceID, name}, payload})
	c.log.add("update")
	if c.onUpdate != nil {
		c.onUpdate()
	}
	return nil
}

// taCountingAssignments answers the active-assignment count the way the repository does and records each call.
type taCountingAssignments struct {
	*csdAssignmentRepo
	counts [][]uuid.UUID
	err    error
}

func (r *taCountingAssignments) CountActiveByTasks(_ context.Context, tenantID uuid.UUID, taskIDs []uuid.UUID) (map[uuid.UUID]int, error) {
	r.counts = append(r.counts, taskIDs)
	if r.err != nil {
		return nil, r.err
	}
	out := map[uuid.UUID]int{}
	for _, a := range r.byID {
		if a.TenantID == tenantID && a.IsActive {
			out[a.TaskID]++
		}
	}
	return out, nil
}

type taReassignWire struct {
	TaskID        string
	OldUserID     string
	NewUserID     string
	AdminUserID   string
	RecordVersion int64
}

func taDecodeReassign(t *testing.T, s taSignal) taReassignWire {
	t.Helper()
	return taDecodePayload(t, s.Payload)
}

func taDecodePayload(t *testing.T, payload any) taReassignWire {
	t.Helper()
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	var wire taReassignWire
	require.NoError(t, json.Unmarshal(raw, &wire))
	return wire
}

// taHarness is the csd task harness with its repositories swapped for fakes that also see the transaction,
// the version bump and the override rows.
type taHarness struct {
	*csdTaskHarness
	log         *taLog
	tx          *taTx
	taskRepo    *taTasks
	overrides   *taOverrides
	elig        *taEligibility
	temporalLog *taTemporal
	counting    *taCountingAssignments
	outbox      *taSignalOutbox
}

func newTaHarness(t *testing.T, assigneeMode string) *taHarness {
	t.Helper()
	base := csdNewTaskHarness(t, assigneeMode, csdReviewStage())
	log := &taLog{}
	tx := &taTx{log: log}
	h := &taHarness{
		csdTaskHarness: base, log: log, tx: tx,
		taskRepo:    &taTasks{csdTaskRepo: base.tasks, tx: tx, log: log},
		overrides:   &taOverrides{tx: tx, log: log},
		elig:        &taEligibility{eligible: true},
		temporalLog: &taTemporal{tx: tx, log: log},
		counting:    &taCountingAssignments{csdAssignmentRepo: base.assignments},
		outbox:      &taSignalOutbox{tx: tx, log: log},
	}
	base.svc.Tasks = h.taskRepo
	base.svc.Overrides = h.overrides
	base.svc.Transactor = tx
	base.svc.Eligibility = h.elig
	base.svc.Temporal = h.temporalLog
	base.svc.Assignments = h.counting
	base.svc.Signals = h.outbox
	base.svc.Cache = newFakeCacheStore()
	return h
}

func (h *taHarness) overrideInput(newUserID uuid.UUID) port.AssigneeOverrideInput {
	return port.AssigneeOverrideInput{
		TenantID: h.tenantID, InstanceID: h.instanceID, NodeKey: h.task.NodeKey,
		NewUserID: newUserID, Reason: "on leave", ActorUserID: uuid.New(), RecordVersion: csdTaskVersion,
	}
}

func (h *taHarness) vacateAll() {
	h.caller.IsActive = false
	h.other.IsActive = false
}

func (h *taHarness) requireNothingChanged(t *testing.T) {
	t.Helper()
	require.Empty(t, h.overrides.creates, "no override row may be stored")
	require.Empty(t, h.taskRepo.bumps, "the task version must not be bumped")
	require.Empty(t, h.temporalLog.signals, "no signal may be sent")
	require.Empty(t, h.outbox.signals, "no signal may be queued")
	require.EqualValues(t, csdTaskVersion, h.task.RecordVersion)
}
