package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/connectors/aliasconfig"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/connectors/restcall"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/registry"

	adapterhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

// aliasFetchClient has no client timeout of its own: the request context carries it.
var aliasFetchClient = adapterhttp.NewClient(0)

// maxAliasBodyBytes caps the registry answer; a real registry is a few KiB.
const maxAliasBodyBytes = 1 << 20

// emptyAnswersToApply is how many empty registry answers in a row the worker takes as real: the registry
// really has no alias left, as when its last alias was deleted. Fewer are held back as a probable fault.
const emptyAnswersToApply = 3

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
			refreshAliases(ctx, d, cfg, log)
		}
	}
}

// refreshAliases applies one fresh registry answer. A failed fetch keeps the held
// registry, and so does an answer with no usable alias while the held registry has
// some: an empty object or a wrong shape must not wipe a working one. The third
// empty answer in a row is applied, so deleting the last alias reaches the workers.
func refreshAliases(ctx context.Context, d *deps, cfg *config.Config, log port.Logger) {
	defer func() { observability.SetGauge(observability.ConnectorAliasRegistryAgeSeconds, d.aliasAge().Seconds()) }()

	fresh, err := fetchAliases(ctx, cfg.DefinitionServiceURL, cfg.ConnectorAliasFetchTimeout, log)
	if err != nil {
		d.emptyAnswers = 0
		log.Warn("connector alias refresh failed; keeping the previous registry", map[string]any{"error": err.Error(), "registry_age_seconds": d.aliasAge().Seconds()})
		return
	}
	if aliasCount(fresh) == 0 && aliasCount(d.currentAliases()) > 0 {
		d.emptyAnswers++
		if d.emptyAnswers < emptyAnswersToApply {
			log.Warn("connector alias refresh returned no usable alias; keeping the previous registry", map[string]any{
				"previous_aliases": aliasCount(d.currentAliases()), "consecutive_empty_answers": d.emptyAnswers,
			})
			return
		}
		log.Warn("connector alias refresh returned no usable alias again; applying the empty registry", map[string]any{
			"previous_aliases": aliasCount(d.currentAliases()), "consecutive_empty_answers": d.emptyAnswers,
		})
	}
	d.emptyAnswers = 0
	pool, ok := d.pools[registry.TypeRestCall]
	if !ok {
		return
	}
	pool.setConnector(restcall.New(fresh, nil, aliasCallToken(cfg)))
	d.setAliases(fresh)
	log.Info("connector alias registry refreshed", map[string]any{"rest_call_aliases": len(fresh.RestCall)})
}

// refusalKind names which part of a row the library refused, from a fixed set of words.
func refusalKind(err error) string {
	text := err.Error()
	switch {
	case strings.Contains(text, "duplicate alias"):
		return "duplicate alias"
	case strings.Contains(text, "baseURL"):
		return "base URL"
	case strings.Contains(text, "pathTemplate"), strings.Contains(text, "path must"):
		return "path"
	case strings.Contains(text, "method"):
		return "method"
	default:
		return "invalid row"
	}
}

func aliasCount(c aliasconfig.Config) int { return len(c.RestCall) + len(c.SQLQuery) }

func fetchAliases(ctx context.Context, addr string, timeout time.Duration, log port.Logger) (aliasconfig.Config, error) {
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	url := strings.TrimRight(addr, "/") + "/api/v1/internal/connector-aliases"
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return aliasconfig.Config{}, fmt.Errorf("build alias fetch request: %w", err)
	}
	req.Header.Set("x-user-id", domain.SystemCaller)
	req.Header.Set("x-tenant-roles", domain.SystemCaller)

	gincommon.InjectTraceContext(reqCtx, req.Header)
	resp, err := aliasFetchClient.Do(req)
	if err != nil {
		return aliasconfig.Config{}, fmt.Errorf("fetch aliases from %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return aliasconfig.Config{}, fmt.Errorf("fetch aliases from %s: status %d", url, resp.StatusCode)
	}

	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxAliasBodyBytes+1))
	if err != nil {
		return aliasconfig.Config{}, fmt.Errorf("read alias registry response: %w", err)
	}
	if len(raw) > maxAliasBodyBytes {
		return aliasconfig.Config{}, fmt.Errorf("alias registry response exceeds 1 MiB")
	}
	var cfg aliasconfig.Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return aliasconfig.Config{}, fmt.Errorf("decode alias registry response: %w", err)
	}
	return validAliasRows(cfg, log), nil
}

// validAliasRows keeps the rows the connector library accepts. connectors.New
// validates every alias, so a single row it refuses would stop every worker
// from starting; each bad row is skipped and named instead. The library only
// validates a whole config, so each row is checked as a one-row config.
// The library's refusal text quotes the base URL, userinfo included, so the log
// carries the alias and the kind of failure, never that text.
func validAliasRows(in aliasconfig.Config, log port.Logger) aliasconfig.Config {
	out := aliasconfig.Config{Version: in.Version}
	seenRest, seenSQL := map[string]bool{}, map[string]bool{}
	skip := func(kind, alias string, err error) {
		log.Warn("connector alias row skipped: the library refuses it", map[string]any{"kind": kind, "alias": alias, "failure": refusalKind(err)})
	}
	for _, e := range in.RestCall {
		err := aliasconfig.Config{RestCall: []aliasconfig.Endpoint{e}}.Validate()
		if err == nil && seenRest[e.Alias] {
			err = fmt.Errorf("duplicate alias %q", e.Alias)
		}
		if err != nil {
			skip("restCall", e.Alias, err)
			continue
		}
		seenRest[e.Alias] = true
		out.RestCall = append(out.RestCall, e)
	}
	for _, q := range in.SQLQuery {
		err := aliasconfig.Config{SQLQuery: []aliasconfig.Query{q}}.Validate()
		if err == nil && seenSQL[q.Alias] {
			err = fmt.Errorf("duplicate alias %q", q.Alias)
		}
		if err != nil {
			skip("sqlQuery", q.Alias, err)
			continue
		}
		seenSQL[q.Alias] = true
		out.SQLQuery = append(out.SQLQuery, q)
	}
	return out
}
