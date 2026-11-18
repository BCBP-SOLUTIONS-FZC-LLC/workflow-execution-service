package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type w12aIdleReader struct{}

func (w12aIdleReader) FeatureFlags(context.Context, uuid.UUID) ([]string, error) { return nil, nil }

var errW12aCommercial = errors.New("commercial answered 503")

func TestEntitlementsGrants_Unavailable_NamesTheTenantTheBoundAndTheCause(t *testing.T) {
	grants, err := NewEntitlementsGrants(EntitlementsGrantsDeps{Reader: w12aIdleReader{}, TTL: time.Minute, FreshnessBound: 10 * time.Minute})
	require.NoError(t, err)
	tenantID := uuid.New()

	got := grants.unavailable(tenantID, errW12aCommercial)

	assert.ErrorContains(t, got, "the feature flags of tenant "+tenantID.String()+" have not been read within 10m0s: commercial answered 503")
}
