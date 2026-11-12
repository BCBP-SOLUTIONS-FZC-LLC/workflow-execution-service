package main

import (
	"fmt"

	outboundhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

// newFeatureGrants returns nil when ENTITLEMENTS_BASE_URL is empty, which leaves the task queue to the registry alone.
// A setting the cache refuses is an error, so a bad value stops startup instead of switching the grant off.
func newFeatureGrants(cfg *config.Config, log port.Logger) (port.FeatureGrants, error) {
	if cfg.EntitlementsBaseURL == "" {
		return nil, nil
	}
	grants, err := service.NewEntitlementsGrants(service.EntitlementsGrantsDeps{
		Reader:         outboundhttp.NewEntitlementsClient(cfg.EntitlementsBaseURL, cfg.EntitlementsClientTimeout),
		TTL:            cfg.EntitlementsCacheTTL,
		FreshnessBound: cfg.EntitlementsFreshnessBound,
		Log:            log,
	})
	if err != nil {
		return nil, fmt.Errorf("entitlements grants: %w", err)
	}
	return grants, nil
}

// newInstanceService adds the feature grants to the instance service's other dependencies.
func newInstanceService(cfg *config.Config, log port.Logger, deps service.InstanceService) (*service.InstanceService, error) {
	grants, err := newFeatureGrants(cfg, log)
	if err != nil {
		return nil, err
	}
	deps.Grants = grants
	return &deps, nil
}
