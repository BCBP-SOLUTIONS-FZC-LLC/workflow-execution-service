package service_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func w14bTenantOf(ctx context.Context) string {
	guc, ok := pgcommon.GUCSetFromContext(ctx)
	if !ok {
		return ""
	}
	return guc.TenantID
}

// w14bTxProbe runs the transaction callback directly and records, at its start
// and end, the tenant the context carries and whether the scope lock is held.
type w14bTxProbe struct {
	locker      *fakeLocker
	tenants     []string
	heldAtStart []bool
	heldAtEnd   []bool
}

func (p *w14bTxProbe) RunInTx(ctx context.Context, fn func(context.Context) error) error {
	p.tenants = append(p.tenants, w14bTenantOf(ctx))
	p.heldAtStart = append(p.heldAtStart, p.locker.held)
	err := fn(ctx)
	p.heldAtEnd = append(p.heldAtEnd, p.locker.held)
	return err
}

func (p *w14bTxProbe) RunInTxWithRetry(ctx context.Context, fn func(context.Context) error) error {
	return p.RunInTx(ctx, fn)
}

// w14bWriteLog records the tenant carried by the context of each write.
type w14bWriteLog struct{ writes []string }

func (l *w14bWriteLog) note(what string, ctx context.Context) {
	l.writes = append(l.writes, what+":"+w14bTenantOf(ctx))
}

type w14bTenantAssignments struct {
	*fakeAssignmentRepo
	log *w14bWriteLog
}

func (r *w14bTenantAssignments) VacateActive(ctx context.Context, tenantID, id uuid.UUID) (*domain.TaskAssignment, error) {
	r.log.note("vacate", ctx)
	return r.fakeAssignmentRepo.VacateActive(ctx, tenantID, id)
}

func (r *w14bTenantAssignments) Create(ctx context.Context, a *domain.TaskAssignment) error {
	r.log.note("create", ctx)
	return r.fakeAssignmentRepo.Create(ctx, a)
}

type w14bTenantOutbox struct {
	*fakeOutbox
	log *w14bWriteLog
}

func (o *w14bTenantOutbox) Enqueue(ctx context.Context, env events.Envelope[json.RawMessage]) error {
	o.log.note("event", ctx)
	return o.fakeOutbox.Enqueue(ctx, env)
}

func (o *w14bTenantOutbox) EnqueueSignal(ctx context.Context, sig port.WorkflowSignal) error {
	o.log.note("signal", ctx)
	return o.fakeOutbox.EnqueueSignal(ctx, sig)
}

type w14bTransactionRig struct {
	svcReroute func() error
	svcReverse func() error
	probe      *w14bTxProbe
	log        *w14bWriteLog
	locker     *fakeLocker
	tenantID   uuid.UUID
	delegator  uuid.UUID
	delegate   uuid.UUID
	delegation uuid.UUID
}

func w14bNewTransactionRig(t *testing.T, heldByDelegator bool) *w14bTransactionRig {
	t.Helper()
	svc, instances, tasks, assignments, outbox, _, definitions, _ := newDelegationReconcilerHarness()
	rig := &w14bTransactionRig{
		locker: &fakeLocker{}, log: &w14bWriteLog{},
		tenantID: uuid.New(), delegator: uuid.New(), delegate: uuid.New(), delegation: uuid.New(),
	}
	holder := rig.delegator
	if !heldByDelegator {
		holder = rig.delegate
	}
	_, _, a := seedDelegationFixture(instances, tasks, assignments, definitions, rig.tenantID, holder, "biz-w14b", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)
	if !heldByDelegator {
		a.Reason = "delegation:" + rig.delegation.String()
	}
	rig.probe = &w14bTxProbe{locker: rig.locker}
	tenantOutbox := &w14bTenantOutbox{fakeOutbox: outbox, log: rig.log}
	svc.Locks = rig.locker
	svc.Transactor = rig.probe
	svc.Assignments = &w14bTenantAssignments{fakeAssignmentRepo: assignments, log: rig.log}
	svc.Outbox = tenantOutbox
	svc.Signals = tenantOutbox
	rig.svcReroute = func() error {
		return svc.Reroute(context.Background(), port.DelegationRerouteInput{
			TenantID: rig.tenantID, DelegationID: rig.delegation, DelegatorID: rig.delegator, DelegateID: rig.delegate, Scope: "all",
		})
	}
	rig.svcReverse = func() error {
		return svc.Reverse(context.Background(), port.DelegationReversalInput{
			TenantID: rig.tenantID, DelegationID: rig.delegation, DelegatorID: rig.delegator, DelegateID: rig.delegate, EndedReason: "expired",
		})
	}
	return rig
}

func TestDelegationReroute_TheMoveTransactionCarriesTheTenantOnTheFourWrites(t *testing.T) {
	t.Parallel()
	rig := w14bNewTransactionRig(t, true)

	require.NoError(t, rig.svcReroute())

	tenant := rig.tenantID.String()
	assert.Equal(t, []string{tenant}, rig.probe.tenants, "the tenant is set on the context before the transaction starts")
	assert.ElementsMatch(t, []string{
		"vacate:" + tenant, "create:" + tenant, "event:" + tenant, "signal:" + tenant,
	}, rig.log.writes, "the vacate, the new assignment, the event and the signal all run under that tenant")
	assert.Equal(t, []bool{true}, rig.probe.heldAtStart, "a start moves its rows under the delegator's lock")
	assert.Equal(t, []bool{true}, rig.probe.heldAtEnd)
	assert.ElementsMatch(t, []string{
		port.UserAvailabilityScope(rig.tenantID, rig.delegator),
		port.DelegationScope(rig.tenantID, rig.delegation),
		port.DelegateScope(rig.tenantID, rig.delegate),
	}, rig.locker.keys, "a start holds the delegator's, the delegation's and the delegate's locks")
}

func TestDelegationReverse_TheMoveCommitsUnderTheDelegatorsLockAndTheTenant(t *testing.T) {
	t.Parallel()
	rig := w14bNewTransactionRig(t, false)

	require.NoError(t, rig.svcReverse())

	tenant := rig.tenantID.String()
	assert.Equal(t, []string{tenant}, rig.probe.tenants)
	assert.Equal(t, []bool{true}, rig.probe.heldAtStart, "the transaction starts with the delegator's availability lock held")
	assert.Equal(t, []bool{true}, rig.probe.heldAtEnd, "the transaction commits before the lock is released")
	assert.ElementsMatch(t, []string{
		"vacate:" + tenant, "create:" + tenant, "event:" + tenant, "signal:" + tenant, "signal:" + tenant,
	}, rig.log.writes, "the vacate, the new assignment, the event, the resume signal and the reassign signal all run under that tenant")
	scope := port.UserAvailabilityScope(rig.tenantID, rig.delegator)
	assert.ElementsMatch(t, []string{scope, scope}, rig.locker.keys, "once to record the end and once to move the work, both on the delegator's availability scope")
	assert.False(t, rig.locker.held, "the lock is released once the reversal returns")
}
