package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// csbVisibilityInstances records the visibility read of a non-admin caller and
// can fail it, as a database error would.
type csbVisibilityInstances struct {
	*fakeInstanceRepo
	failWith error
	tenants  []uuid.UUID
	filters  []port.InstanceListFilter
	pages    []port.PageRequest
}

func (r *csbVisibilityInstances) ListByTenant(ctx context.Context, tenantID uuid.UUID, filter port.InstanceListFilter, page port.PageRequest) ([]*domain.Instance, *port.Cursor, error) {
	r.tenants = append(r.tenants, tenantID)
	r.filters = append(r.filters, filter)
	r.pages = append(r.pages, page)
	if r.failWith != nil {
		return nil, nil, r.failWith
	}
	return r.fakeInstanceRepo.ListByTenant(ctx, tenantID, filter, page)
}

func TestInstanceService_Get_AsksTheVisibilityReadForTheRequestedInstanceOnly(t *testing.T) {
	cases := []struct {
		name  string
		scope func(caller, dept uuid.UUID) port.ReadScope
		// assigned decides whether the caller also holds an active assignment on the other instance's task.
		assigned bool
	}{
		{
			name:     "an active assignment on a task of the other instance",
			scope:    func(caller, _ uuid.UUID) port.ReadScope { return port.ReadScope{CallerUserID: caller} },
			assigned: true,
		},
		{
			name: "a department match on a task of the other instance",
			scope: func(caller, dept uuid.UUID) port.ReadScope {
				return port.ReadScope{CallerUserID: caller, Departments: []port.DepartmentRole{{DepartmentID: dept}}}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, instances, tasks, assignments, _, _, _, _ := newInstanceServiceHarness()
			repo := &csbVisibilityInstances{fakeInstanceRepo: instances}
			svc.Instances = repo
			tenantID, caller, matchedDept := uuid.New(), uuid.New(), uuid.New()

			refused := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning}
			matched := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning}
			instances.byID[refused.ID], instances.byID[matched.ID] = refused, matched
			unrelatedTask := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: refused.ID, DepartmentID: uuid.New()}
			matchedTask := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: matched.ID, DepartmentID: matchedDept}
			tasks.byID[unrelatedTask.ID], tasks.byID[matchedTask.ID] = unrelatedTask, matchedTask
			if tc.assigned {
				assignments.add(&domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: matchedTask.ID, UserID: caller, IsActive: true})
			}
			scope := tc.scope(caller, matchedDept)

			_, _, err := svc.Get(context.Background(), tenantID, refused.ID, scope)

			require.ErrorIs(t, err, port.ErrNotAuthorizedForRead)
			require.Len(t, repo.filters, 1)
			assert.Equal(t, tenantID, repo.tenants[0])
			require.NotNil(t, repo.filters[0].InstanceID)
			assert.Equal(t, refused.ID, *repo.filters[0].InstanceID, "the visibility read is for the requested instance only")
			require.NotNil(t, repo.filters[0].Scope)
			assert.Equal(t, caller, repo.filters[0].Scope.CallerUserID)
			if tc.assigned {
				assert.Empty(t, repo.filters[0].Scope.DepartmentIDs)
			} else {
				assert.Equal(t, []uuid.UUID{matchedDept}, repo.filters[0].Scope.DepartmentIDs)
			}

			got, gotTasks, err := svc.Get(context.Background(), tenantID, matched.ID, scope)
			require.NoError(t, err, "the same caller sees the instance the match is on")
			assert.Equal(t, matched.ID, got.ID)
			require.Len(t, gotTasks, 1)
			assert.Equal(t, matchedTask.ID, gotTasks[0].ID)
		})
	}
}

func TestInstanceService_Get_AFailedVisibilityReadOfANonAdminIsNotARefusal(t *testing.T) {
	svc, instances, _, _, _, _, _, _ := newInstanceServiceHarness()
	errVisibility := errors.New("visibility query failed")
	repo := &csbVisibilityInstances{fakeInstanceRepo: instances, failWith: errVisibility}
	svc.Instances = repo
	tenantID, caller := uuid.New(), uuid.New()
	inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning}
	instances.byID[inst.ID] = inst

	got, tasks, err := svc.Get(context.Background(), tenantID, inst.ID, port.ReadScope{CallerUserID: caller})

	require.ErrorIs(t, err, errVisibility)
	assert.NotErrorIs(t, err, port.ErrNotAuthorizedForRead, "a database failure is not reported as the caller being unauthorised")
	assert.Nil(t, got)
	assert.Nil(t, tasks)
	require.Len(t, repo.filters, 1)
	assert.Equal(t, tenantID, repo.tenants[0])
	require.NotNil(t, repo.filters[0].InstanceID)
	assert.Equal(t, inst.ID, *repo.filters[0].InstanceID)
	require.NotNil(t, repo.filters[0].Scope)
	assert.Equal(t, caller, repo.filters[0].Scope.CallerUserID)
}
