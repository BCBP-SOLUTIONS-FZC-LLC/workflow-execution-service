package service_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

type chainWorld struct {
	svc         *service.DelegationReconciler
	assignments *fakeAssignmentRepo
	tenantID    uuid.UUID
	a, b, c     uuid.UUID
}

// newChainWorld has user a holding one task and user b holding another, with no delegations yet.
func newChainWorld(t *testing.T) (*chainWorld, *domain.Task, *domain.Task) {
	t.Helper()
	svc, instances, tasks, assignments, _, _, definitions, _ := newDelegationReconcilerHarness()
	w := &chainWorld{svc: svc, assignments: assignments, tenantID: uuid.New(), a: uuid.New(), b: uuid.New(), c: uuid.New()}
	stage := dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}
	_, aTask, _ := seedDelegationFixture(instances, tasks, assignments, definitions, w.tenantID, w.a, "biz-a", stage, t)
	_, bTask, _ := seedDelegationFixture(instances, tasks, assignments, definitions, w.tenantID, w.b, "biz-b", stage, t)
	return w, aTask, bTask
}

func (w *chainWorld) reroute(t *testing.T, delegationID, delegator, delegate uuid.UUID) {
	t.Helper()
	require.NoError(t, w.svc.Reroute(context.Background(), port.DelegationRerouteInput{
		TenantID: w.tenantID, DelegationID: delegationID, DelegatorID: delegator, DelegateID: delegate, Scope: "all",
	}))
}

func (w *chainWorld) reverse(t *testing.T, delegationID, delegator, delegate uuid.UUID) {
	t.Helper()
	require.NoError(t, w.svc.Reverse(context.Background(), port.DelegationReversalInput{
		TenantID: w.tenantID, DelegationID: delegationID, DelegatorID: delegator, DelegateID: delegate, EndedReason: "expired",
	}))
}

func (w *chainWorld) holder(t *testing.T, task *domain.Task) *domain.TaskAssignment {
	t.Helper()
	var active []*domain.TaskAssignment
	for _, a := range w.assignments.byID {
		if a.TaskID == task.ID && a.IsActive {
			active = append(active, a)
		}
	}
	require.Len(t, active, 1, "a task has exactly one holder")
	return active[0]
}

func TestDelegationChain_WorkDelegatedTwiceKeepsTheTagOfEveryHop(t *testing.T) {
	w, aTask, bTask := newChainWorld(t)
	x, y := uuid.New(), uuid.New()

	w.reroute(t, x, w.a, w.b)
	w.reroute(t, y, w.b, w.c)

	forA := w.holder(t, aTask)
	assert.Equal(t, w.c, forA.UserID, "a's work, now with c")
	assert.Equal(t, "delegation:"+x.String()+":"+w.a.String()+";delegation:"+y.String()+":"+w.b.String(), forA.Reason)
	forB := w.holder(t, bTask)
	assert.Equal(t, w.c, forB.UserID)
	assert.Equal(t, "delegation:"+y.String()+":"+w.b.String(), forB.Reason, "b's own work carries only b's delegation")
}

func TestDelegationChain_EndingTheFirstDelegationReturnsTheWorkWhereverItSits(t *testing.T) {
	w, aTask, bTask := newChainWorld(t)
	x, y := uuid.New(), uuid.New()
	w.reroute(t, x, w.a, w.b)
	w.reroute(t, y, w.b, w.c)

	w.reverse(t, x, w.a, w.b)

	forA := w.holder(t, aTask)
	assert.Equal(t, w.a, forA.UserID, "a's work comes back to a from c")
	assert.Empty(t, forA.Reason, "no delegation covers the restored row any more")
	forB := w.holder(t, bTask)
	assert.Equal(t, w.c, forB.UserID, "b's own work stays with c while b's delegation runs")
	assert.Contains(t, forB.Reason, y.String())
}

func TestDelegationChain_EndingTheSecondDelegationReturnsTheWorkToItsDelegator(t *testing.T) {
	w, aTask, bTask := newChainWorld(t)
	x, y := uuid.New(), uuid.New()
	w.reroute(t, x, w.a, w.b)
	w.reroute(t, y, w.b, w.c)

	w.reverse(t, y, w.b, w.c)

	forA := w.holder(t, aTask)
	assert.Equal(t, w.b, forA.UserID, "a's work goes back to b, who still holds it for a")
	assert.Equal(t, "delegation:"+x.String()+":"+w.a.String(), forA.Reason)
	forB := w.holder(t, bTask)
	assert.Equal(t, w.b, forB.UserID)
	assert.Empty(t, forB.Reason)

	w.reverse(t, x, w.a, w.b)
	assert.Equal(t, w.a, w.holder(t, aTask).UserID, "ending the first delegation afterwards still reaches a")
}
