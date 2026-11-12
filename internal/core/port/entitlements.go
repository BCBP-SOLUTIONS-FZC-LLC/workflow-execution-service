package port

import (
	"context"
	"errors"

	"github.com/google/uuid"
)

var ErrEntitlementsUnavailable = errors.New("entitlements service unavailable")

// EntitlementsReader reads a tenant's whole feature-flag set from the Entitlements effective set.
type EntitlementsReader interface {
	FeatureFlags(ctx context.Context, tenantID uuid.UUID) ([]string, error)
}

// FeatureGrants answers whether a tenant holds one named grant. An error means the answer is unknown.
type FeatureGrants interface {
	HasFeature(ctx context.Context, tenantID uuid.UUID, feature string) (bool, error)
}
