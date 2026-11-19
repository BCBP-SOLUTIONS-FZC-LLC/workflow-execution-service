package service_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTenantPurger_ACancelledContextStopsTheTickWithoutCountingAFailureOrBackingOffTheTenant(t *testing.T) {
	first, second := uuid.New(), uuid.New()
	ctx, cancel := context.WithCancel(context.Background())
	r := &fakeRetention{due: dueTenants(first, second), onList: cancel}
	d := &fakeDataPurger{}
	rec, failed := map[string]int64{}, 0

	err := newPurger(r, d, rec, &failed).Tick(ctx)

	require.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, failed, "a shutdown is not a purge failure")
	assert.Empty(t, r.failures, "no tenant is given a retry backoff because the process stopped")
	assert.Empty(t, d.calls, "the second tenant is not attempted either")
}
