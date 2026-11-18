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

var errW14bSerialization = errors.New("w14b serialization failure")

// w14bRetryingTransactor reruns the callback while it fails with the one
// retryable error, as the serializable transactor does for a serialization
// failure, up to three attempts.
type w14bRetryingTransactor struct{ attempts int }

func (w14bRetryingTransactor) RunInTx(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}

func (r *w14bRetryingTransactor) RunInTxWithRetry(ctx context.Context, fn func(context.Context) error) error {
	var err error
	for range 3 {
		r.attempts++
		if err = fn(ctx); !errors.Is(err, errW14bSerialization) {
			return err
		}
	}
	return err
}

type w14bInstanceCall struct {
	tenantID uuid.UUID
	id       uuid.UUID
	version  int64
}

// w14bRacingInstances lets a concurrent writer change the instance between the
// callback's read and its write: the first UpdateStatus runs raceOnce, then
// fails with the retryable error.
type w14bRacingInstances struct {
	*fakeInstanceRepo
	raceOnce func(*fakeInstanceRepo, uuid.UUID)
	raced    bool
	reads    []w14bInstanceCall
	updates  []w14bInstanceCall
}

func (r *w14bRacingInstances) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	inst, err := r.fakeInstanceRepo.GetByID(ctx, tenantID, id)
	if err == nil {
		r.reads = append(r.reads, w14bInstanceCall{tenantID: tenantID, id: id, version: inst.RecordVersion})
	}
	return inst, err
}

func (r *w14bRacingInstances) UpdateStatus(ctx context.Context, tenantID, id uuid.UUID, status domain.InstanceStatus, recordVersion int64) (*domain.Instance, error) {
	r.updates = append(r.updates, w14bInstanceCall{tenantID: tenantID, id: id, version: recordVersion})
	if !r.raced {
		r.raced = true
		r.raceOnce(r.fakeInstanceRepo, id)
		return nil, errW14bSerialization
	}
	return r.fakeInstanceRepo.UpdateStatus(ctx, tenantID, id, status, recordVersion)
}

func TestTenantLifecycleReconciler_Offboard_RetriedTerminationReadsTheInstanceAgain(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name           string
		race           func(*fakeInstanceRepo, uuid.UUID)
		wantStatus     domain.InstanceStatus
		wantUpdates    []int64
		wantTerminated int
	}{
		{
			name: "a write that moved the version is read on the rerun and the instance is terminated",
			race: func(repo *fakeInstanceRepo, id uuid.UUID) {
				written := *repo.byID[id]
				written.RecordVersion++
				repo.byID[id] = &written
			},
			wantStatus:     domain.InstanceStatusTerminated,
			wantUpdates:    []int64{1, 2},
			wantTerminated: 1,
		},
		{
			name: "an instance another writer finished meanwhile is found terminal on the rerun and left alone",
			race: func(repo *fakeInstanceRepo, id uuid.UUID) {
				written := *repo.byID[id]
				written.RecordVersion++
				written.Status = domain.InstanceStatusCompleted
				repo.byID[id] = &written
			},
			wantStatus:     domain.InstanceStatusCompleted,
			wantUpdates:    []int64{1},
			wantTerminated: 0,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			svc, instances, _, _, _, outbox, temporal := newTenantLifecycleHarness()
			tx := &w14bRetryingTransactor{}
			racing := &w14bRacingInstances{fakeInstanceRepo: instances, raceOnce: tc.race}
			svc.Instances = racing
			svc.Transactor = tx
			tenantID := uuid.New()
			inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning, TemporalWorkflowID: "tenant:busy", RecordVersion: 1}
			instances.byID[inst.ID] = inst

			require.NoError(t, svc.Apply(context.Background(), port.TenantLifecycleInput{TenantID: tenantID, Status: "offboarded", PreviousStatus: "active"}))

			assert.Equal(t, 2, tx.attempts, "the callback ran again after the retryable failure")
			assert.Equal(t, tc.wantStatus, instances.byID[inst.ID].Status)
			var versions []int64
			for _, u := range racing.updates {
				assert.Equal(t, tenantID, u.tenantID)
				assert.Equal(t, inst.ID, u.id)
				versions = append(versions, u.version)
			}
			assert.Equal(t, tc.wantUpdates, versions, "each write carries the version read in its own attempt")
			require.Len(t, racing.reads, 2, "one read per attempt")
			var readVersions []int64
			for _, r := range racing.reads {
				assert.Equal(t, tenantID, r.tenantID)
				assert.Equal(t, inst.ID, r.id)
				readVersions = append(readVersions, r.version)
			}
			assert.Equal(t, []int64{1, 2}, readVersions, "the rerun reads the version the concurrent write left")
			var terminatedEvents int
			for _, env := range outbox.enqueued {
				if env.Type == domain.EventWorkflowInstanceTerminated {
					terminatedEvents++
				}
			}
			assert.Equal(t, tc.wantTerminated, terminatedEvents)
			assert.Len(t, temporal.terminateCalls(), 2, "the workflow is closed before and after the write, not once per attempt")
		})
	}
}
