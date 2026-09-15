package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/pkg/connectors/aliasconfig"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/pkg/connectors/restcall"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/pkg/registry"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func runAliasRefreshLoop(ctx context.Context, d *deps, cfg *config.Config, log port.Logger) {
	if cfg.ConnectorAliasRefreshInterval <= 0 {
		return
	}
	ticker := time.NewTicker(cfg.ConnectorAliasRefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fresh, err := fetchAliases(ctx, cfg.DefinitionServiceInternalHTTPAddr, cfg.InternalAPIToken, cfg.ConnectorAliasFetchTimeout)
			if err != nil {
				log.Warn("connector alias refresh failed; keeping the previous registry", map[string]any{"error": err.Error()})
				continue
			}
			pool, ok := d.pools[registry.TypeRestCall]
			if !ok {
				continue
			}
			pool.setConnector(restcall.New(fresh, nil, cfg.InternalAPIToken))
			log.Info("connector alias registry refreshed", map[string]any{"rest_call_aliases": len(fresh.RestCall)})
		}
	}
}

func fetchAliases(ctx context.Context, addr, internalToken string, timeout time.Duration) (aliasconfig.Config, error) {
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	url := strings.TrimRight(addr, "/") + "/api/v1/internal/connector-aliases"
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return aliasconfig.Config{}, fmt.Errorf("build alias fetch request: %w", err)
	}
	req.Header.Set("x-internal-token", internalToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return aliasconfig.Config{}, fmt.Errorf("fetch aliases from %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return aliasconfig.Config{}, fmt.Errorf("fetch aliases from %s: status %d", url, resp.StatusCode)
	}

	var cfg aliasconfig.Config
	if err := json.NewDecoder(resp.Body).Decode(&cfg); err != nil {
		return aliasconfig.Config{}, fmt.Errorf("decode alias registry response: %w", err)
	}
	return cfg, nil
}
