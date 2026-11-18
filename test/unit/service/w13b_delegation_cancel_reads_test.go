package service_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
)

// w13bReadTasks records the tenant and id of each task read and fails the ones it was told to.
type w13bReadTasks struct {
	*fakeTaskRepo
	fail  map[uuid.UUID]error
	reads []csdIDCall
}

func (r *w13bReadTasks) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Task, error) {
	r.reads = append(r.reads, csdIDCall{TenantID: tenantID, ID: id})
	if err := r.fail[id]; err != nil {
		return nil, err
	}
	return r.fakeTaskRepo.GetByID(ctx, tenantID, id)
}

// w13bReadInstances records the tenant and id of each instance read and fails the ones it was told to.
type w13bReadInstances struct {
	*fakeInstanceRepo
	fail  map[uuid.UUID]error
	reads []csdIDCall
}

func (r *w13bReadInstances) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	r.reads = append(r.reads, csdIDCall{TenantID: tenantID, ID: id})
	if err := r.fail[id]; err != nil {
		return nil, err
	}
	return r.fakeInstanceRepo.GetByID(ctx, tenantID, id)
}

func TestDelegationCancel_ARowCannotBeReadForAnySelectedAssignment_FailsBeforeTheTransactionOpens(t *testing.T) {
	cases := []struct {
		name          string
		fail          func(second dlgfixHeld, tasks *w13bReadTasks, instances *w13bReadInstances)
		wantName      func(second dlgfixHeld) string
		wantTaskReads int
		wantInstReads int
	}{
		{
			name: "the task of the second assignment cannot be read",
			fail: func(second dlgfixHeld, tasks *w13bReadTasks, _ *w13bReadInstances) {
				tasks.fail[second.task.ID] = errW13bRepo
			},
			wantName: func(second dlgfixHeld) string {
				return "read task of assignment " + second.assignment.ID.String()
			},
			wantTaskReads: 2, wantInstReads: 1,
		},
		{
			name: "the instance of the second assignment cannot be read",
			fail: func(second dlgfixHeld, _ *w13bReadTasks, instances *w13bReadInstances) {
				instances.fail[second.instance.ID] = errW13bRepo
			},
			wantName: func(second dlgfixHeld) string {
				return "read instance of task " + second.task.ID.String()
			},
			wantTaskReads: 2, wantInstReads: 2,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := dlgfixNewClient(t)
			tasks := &w13bReadTasks{fakeTaskRepo: h.tasks, fail: map[uuid.UUID]error{}}
			instances := &w13bReadInstances{fakeInstanceRepo: h.instances, fail: map[uuid.UUID]error{}}
			ended := &w7bEnded{fakeEndedDelegations: newFakeEndedDelegations()}
			h.svc.Tasks, h.svc.Instances, h.svc.Ended = tasks, instances, ended
			first, second := h.seed(t, "reviewer"), h.seed(t, "reviewer")
			tc.fail(second, tasks, instances)

			count, err := h.cancel()

			require.ErrorIs(t, err, errW13bRepo)
			assert.ErrorContains(t, err, tc.wantName(second), "the error names what could not be read")
			assert.Zero(t, count)
			assert.Len(t, tasks.reads, tc.wantTaskReads, "the reads stop at the failing assignment")
			assert.Len(t, instances.reads, tc.wantInstReads)
			for _, read := range tasks.reads {
				assert.Equal(t, h.tenantID, read.TenantID)
			}
			for _, read := range instances.reads {
				assert.Equal(t, h.tenantID, read.TenantID)
			}
			assert.Equal(t, csdIDCall{TenantID: h.tenantID, ID: first.task.ID}, tasks.reads[0], "assignments are read in the order they were listed")
			assert.Equal(t, csdIDCall{TenantID: h.tenantID, ID: first.instance.ID}, instances.reads[0])
			assert.Empty(t, h.tx.tenants, "no transaction was opened: every read finished first")
			assert.Zero(t, h.tx.commits)
			assert.Empty(t, h.assignments.vacated, "nothing was vacated")
			assert.True(t, first.assignment.IsActive)
			assert.True(t, second.assignment.IsActive)
			assert.Empty(t, h.outbox.signals)
			assert.Empty(t, ended.delegateMarks, "the end is not recorded for a call that failed")
			assert.Empty(t, ended.marked)
		})
	}

	t.Run("control: with every read working both rows are vacated in one transaction", func(t *testing.T) {
		h := dlgfixNewClient(t)
		tasks := &w13bReadTasks{fakeTaskRepo: h.tasks, fail: map[uuid.UUID]error{}}
		instances := &w13bReadInstances{fakeInstanceRepo: h.instances, fail: map[uuid.UUID]error{}}
		ended := &w7bEnded{fakeEndedDelegations: newFakeEndedDelegations()}
		h.svc.Tasks, h.svc.Instances, h.svc.Ended = tasks, instances, ended
		first, second := h.seed(t, "reviewer"), h.seed(t, "reviewer")

		count, err := h.cancel()

		require.NoError(t, err)
		assert.Equal(t, 2, count)
		assert.False(t, first.assignment.IsActive)
		assert.False(t, second.assignment.IsActive)
		assert.Equal(t, 1, h.tx.commits)
		assert.Equal(t, []endedDelegateKey{{h.tenantID, h.oldDelegate}}, ended.delegateMarks, "the end is recorded for the delegate")
		assert.Len(t, tasks.reads, 2)
		assert.Len(t, instances.reads, 2)
		assert.Equal(t, []string{h.tenantID.String()}, h.tx.tenants)
	})
}
