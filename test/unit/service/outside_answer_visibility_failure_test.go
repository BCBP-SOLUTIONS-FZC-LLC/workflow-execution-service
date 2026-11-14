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

var errCsfVisibilityLookup = errors.New("assignment visibility lookup unavailable")

type csfVisibilityCall struct {
	TenantID uuid.UUID
	Filter   port.InstanceListFilter
}

// csfVisibilityRepo fails the visibility lookup (InstanceRepository.ListByTenant)
// and records the tenant and filter it was asked for.
type csfVisibilityRepo struct {
	*fakeInstanceRepo
	calls []csfVisibilityCall
}

func (r *csfVisibilityRepo) ListByTenant(_ context.Context, tenantID uuid.UUID, filter port.InstanceListFilter, _ port.PageRequest) ([]*domain.Instance, *port.Cursor, error) {
	r.calls = append(r.calls, csfVisibilityCall{TenantID: tenantID, Filter: filter})
	return nil, nil, errCsfVisibilityLookup
}

func TestAnswerOutsideWait_AssigneeLookupFails_IsAnErrorNotARefusalAndSignalsNothing(t *testing.T) {
	svc, inst, temporal := outsideAnswerHarness(t, domain.InstanceStatusRunning, outsideKey)
	repo := &csfVisibilityRepo{fakeInstanceRepo: svc.Instances.(*fakeInstanceRepo)}
	svc.Instances = repo
	caller := uuid.New()

	err := svc.AnswerOutsideWait(context.Background(), inst.TenantID, inst.ID, port.ReadScope{CallerUserID: caller}, outsideKey, nil)

	require.ErrorIs(t, err, errCsfVisibilityLookup)
	assert.NotErrorIs(t, err, port.ErrNotAuthorizedForRead, "a failed lookup must not be read as 'not allowed'")
	require.Len(t, repo.calls, 1)
	assert.Equal(t, inst.TenantID, repo.calls[0].TenantID)
	require.NotNil(t, repo.calls[0].Filter.InstanceID)
	assert.Equal(t, inst.ID, *repo.calls[0].Filter.InstanceID)
	require.NotNil(t, repo.calls[0].Filter.Scope)
	assert.Equal(t, caller, repo.calls[0].Filter.Scope.CallerUserID)
	assert.Empty(t, repo.calls[0].Filter.Scope.DepartmentIDs, "an outside answer is open to assignees only, never to a department")
	assert.Empty(t, temporal.signals, "an unverified caller must not signal the workflow")
}

func TestAnswerOutsideWait_AssigneeLookupFails_AnAdminNeverRunsTheLookup(t *testing.T) {
	svc, inst, temporal := outsideAnswerHarness(t, domain.InstanceStatusRunning, outsideKey)
	repo := &csfVisibilityRepo{fakeInstanceRepo: svc.Instances.(*fakeInstanceRepo)}
	svc.Instances = repo

	err := svc.AnswerOutsideWait(context.Background(), inst.TenantID, inst.ID, adminScope(), outsideKey, nil)

	require.NoError(t, err)
	assert.Empty(t, repo.calls)
	assert.Len(t, temporal.signals, 1)
}
