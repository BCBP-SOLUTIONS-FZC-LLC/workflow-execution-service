package main

import "context"

type slotState int

const (
	slotHeld slotState = iota
	slotParked
	slotAbandoned
)

// claimSlot takes a run slot, else a parking token so the read loop goes on with other types. A pool has as many
// tokens as slots, so at most twice its size is in flight for one type. At the parking bound it waits for a slot,
// as the loop did before parking existed, and a shutdown ends that wait (§S6.5 step 2).
func claimSlot(ctx context.Context, pool *typePool) slotState {
	select {
	case pool.sem <- struct{}{}:
		return slotHeld
	default:
	}
	select {
	case pool.parked <- struct{}{}:
		return slotParked
	default:
	}
	select {
	case pool.sem <- struct{}{}:
		return slotHeld
	case <-ctx.Done():
		return slotAbandoned
	}
}

// waitParked waits for a slot for a parked entry, which stays in flight (and touched) meanwhile; a shutdown
// releases it unacknowledged for the next worker.
func waitParked(ctx context.Context, pool *typePool) bool {
	defer func() { <-pool.parked }()
	select {
	case pool.sem <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}
