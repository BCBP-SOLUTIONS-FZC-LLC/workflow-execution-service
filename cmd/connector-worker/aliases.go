package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/pkg/connectors/aliasconfig"
)

func fetchAliases(ctx context.Context, addr, internalToken string, timeout time.Duration) (aliasconfig.Config, error) {
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	url := strings.TrimRight(addr, "/") + "/internal/connector-aliases"
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
