package service_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// w12aFlagsReader answers each tenant with the list it was given (nil and empty included)
// or with the error it was told to fail with, and records the tenant of every read.
type w12aFlagsReader struct {
	mu    sync.Mutex
	flags map[uuid.UUID][]string
	err   error
	reads []uuid.UUID
}

func (r *w12aFlagsReader) FeatureFlags(_ context.Context, tenantID uuid.UUID) ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reads = append(r.reads, tenantID)
	if r.err != nil {
		return nil, r.err
	}
	return r.flags[tenantID], nil
}

func (r *w12aFlagsReader) readCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.reads)
}

func (r *w12aFlagsReader) setErr(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.err = err
}

func TestEntitlementsGrants_HasFeature_MatchesTheFlagNameExactlyAndAnEmptySetGrantsNothing(t *testing.T) {
	cases := []struct {
		name    string
		flags   []string
		feature string
		want    bool
	}{
		{name: "a flag in the set", flags: []string{"sso_enabled", "Dedicated_Queue"}, feature: "Dedicated_Queue", want: true},
		{name: "a flag not in the set", flags: []string{"sso_enabled"}, feature: "dedicated_queue", want: false},
		{name: "the same name in lower case", flags: []string{"Dedicated_Queue"}, feature: "dedicated_queue", want: false},
		{name: "the same name in upper case", flags: []string{"dedicated_queue"}, feature: "DEDICATED_QUEUE", want: false},
		{name: "the same name with a trailing space", flags: []string{"dedicated_queue"}, feature: "dedicated_queue ", want: false},
		{name: "an empty feature name", flags: []string{"dedicated_queue"}, feature: "", want: false},
		{name: "an empty list", flags: []string{}, feature: "dedicated_queue", want: false},
		{name: "a nil list", flags: nil, feature: "dedicated_queue", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tenant, other := uuid.New(), uuid.New()
			reader := &w12aFlagsReader{flags: map[uuid.UUID][]string{tenant: tc.flags, other: {"dedicated_queue", "Dedicated_Queue", ""}}}
			grants, clock := newGrants(t, reader)

			has, err := grants.HasFeature(context.Background(), tenant, tc.feature)

			require.NoError(t, err, "an empty answer is an answer, not a failure")
			assert.Equal(t, tc.want, has)
			clock.advance(time.Second)
			again, err := grants.HasFeature(context.Background(), tenant, tc.feature)
			require.NoError(t, err)
			assert.Equal(t, tc.want, again, "the set is cached as read, empty or not")
			otherHas, err := grants.HasFeature(context.Background(), other, "dedicated_queue")
			require.NoError(t, err)
			assert.True(t, otherHas, "another tenant has its own set, not this one's")
			assert.Equal(t, []uuid.UUID{tenant, other}, reader.reads, "one read per tenant")
		})
	}
}

type w12aReaderError struct{ status int }

func (e *w12aReaderError) Error() string { return fmt.Sprintf("commercial answered %d", e.status) }

func TestEntitlementsGrants_HasFeature_ReadFailsWithNothingHeld_IsFalseAndAnUnwrappableUnavailableError(t *testing.T) {
	cause := &w12aReaderError{status: 503}

	check := func(t *testing.T, tenant uuid.UUID, has bool, err error) {
		t.Helper()
		assert.False(t, has, "a failed read is never turned into an answer")
		require.Error(t, err)
		assert.ErrorIs(t, err, port.ErrEntitlementsUnavailable)
		assert.ErrorIs(t, err, cause, "the cause is reachable")
		var got *w12aReaderError
		require.ErrorAs(t, err, &got)
		assert.Equal(t, 503, got.status)
		assert.ErrorContains(t, err, tenant.String(), "the message names the tenant")
		assert.ErrorContains(t, err, "10m0s", "and the freshness bound")
		assert.ErrorContains(t, err, "commercial answered 503", "and the cause")
	}

	t.Run("the read that fails", func(t *testing.T) {
		tenant := uuid.New()
		reader := &w12aFlagsReader{flags: map[uuid.UUID][]string{tenant: {grantFlag}}, err: cause}
		grants, _ := newGrants(t, reader)

		has, err := grants.HasFeature(context.Background(), tenant, grantFlag)

		check(t, tenant, has, err)
		assert.Equal(t, 1, reader.readCount())
	})

	t.Run("the call inside the backoff that follows, with no new read", func(t *testing.T) {
		tenant := uuid.New()
		reader := &w12aFlagsReader{flags: map[uuid.UUID][]string{tenant: {grantFlag}}, err: cause}
		grants, clock := newGrants(t, reader)
		_, err := grants.HasFeature(context.Background(), tenant, grantFlag)
		require.Error(t, err)
		clock.advance(time.Second)

		has, err := grants.HasFeature(context.Background(), tenant, grantFlag)

		check(t, tenant, has, err)
		assert.Equal(t, 1, reader.readCount(), "the backoff blocks the second read")
	})

	t.Run("the same tenant is answered once the read works again", func(t *testing.T) {
		tenant := uuid.New()
		reader := &w12aFlagsReader{flags: map[uuid.UUID][]string{tenant: {grantFlag}}, err: errors.New("down")}
		grants, clock := newGrants(t, reader)
		_, err := grants.HasFeature(context.Background(), tenant, grantFlag)
		require.ErrorIs(t, err, port.ErrEntitlementsUnavailable)
		reader.setErr(nil)
		clock.advance(6 * time.Second)

		has, err := grants.HasFeature(context.Background(), tenant, grantFlag)

		require.NoError(t, err)
		assert.True(t, has)
	})
}
