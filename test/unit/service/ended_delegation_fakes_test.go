package service_test

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type endedDelegationKey struct{ tenantID, delegationID uuid.UUID }

type endedDelegateKey struct{ tenantID, delegateID uuid.UUID }

// fakeEndedDelegations remembers ended delegations per tenant and records what it was asked.
type fakeEndedDelegations struct {
	mu       sync.Mutex
	ended    map[endedDelegationKey]bool
	marked   []endedDelegationKey
	markErr  error
	checkErr error
	onMark   func()
	onCheck  func()

	delegates map[endedDelegateKey]time.Time
}

func newFakeEndedDelegations() *fakeEndedDelegations {
	return &fakeEndedDelegations{ended: map[endedDelegationKey]bool{}, delegates: map[endedDelegateKey]time.Time{}}
}

func (f *fakeEndedDelegations) MarkEnded(_ context.Context, tenantID, delegationID uuid.UUID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.onMark != nil {
		f.onMark()
	}
	if f.markErr != nil {
		return f.markErr
	}
	key := endedDelegationKey{tenantID, delegationID}
	f.ended[key] = true
	f.marked = append(f.marked, key)
	return nil
}

func (f *fakeEndedDelegations) IsEnded(_ context.Context, tenantID, delegationID uuid.UUID) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.onCheck != nil {
		f.onCheck()
	}
	if f.checkErr != nil {
		return false, f.checkErr
	}
	return f.ended[endedDelegationKey{tenantID, delegationID}], nil
}

var _ port.EndedDelegationRepository = (*fakeEndedDelegations)(nil)

func (f *fakeEndedDelegations) MarkDelegateEnded(_ context.Context, tenantID, delegateID uuid.UUID, endedAt time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := endedDelegateKey{tenantID, delegateID}
	if endedAt.After(f.delegates[key]) {
		f.delegates[key] = endedAt
	}
	return nil
}

func (f *fakeEndedDelegations) DelegateEndedAt(_ context.Context, tenantID, delegateID uuid.UUID) (time.Time, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	at, ok := f.delegates[endedDelegateKey{tenantID, delegateID}]
	return at, ok, nil
}
