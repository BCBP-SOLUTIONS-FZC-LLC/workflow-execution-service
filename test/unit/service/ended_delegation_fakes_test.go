package service_test

import (
	"context"
	"sync"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type endedDelegationKey struct{ tenantID, delegationID uuid.UUID }

// fakeEndedDelegations remembers ended delegations per tenant and records what it was asked.
type fakeEndedDelegations struct {
	mu       sync.Mutex
	ended    map[endedDelegationKey]bool
	marked   []endedDelegationKey
	markErr  error
	checkErr error
	onMark   func()
	onCheck  func()
}

func newFakeEndedDelegations() *fakeEndedDelegations {
	return &fakeEndedDelegations{ended: map[endedDelegationKey]bool{}}
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
