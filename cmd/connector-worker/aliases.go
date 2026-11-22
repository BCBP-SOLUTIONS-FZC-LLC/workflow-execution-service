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
	pool.setConnector(restcall.New(fresh, nil, ""))
	d.setAliases(fresh)
	log.Info("connector alias registry refreshed", map[string]any{"rest_call_aliases": len(fresh.RestCall)})
}

// The refusal kinds a skipped row is logged under, a fixed set of words.
const (
	refusalDuplicate = "duplicate alias"
	refusalBaseURL   = "base URL"
	refusalPath      = "path"
	refusalMethod    = "method"
	refusalRow       = "invalid row"
)

var (
	probeEndpoint = aliasconfig.Endpoint{
		Alias: "probe", Method: http.MethodGet, BaseURL: "http://probe.invalid", PathTemplate: "/",
	}
	probeQuery = aliasconfig.Query{
		Alias: "probe", BaseURL: "http://probe.invalid", Path: "/", QueryID: "probe",
	}
)

// endpointRefusal names the field the library refuses by putting each field of the row, one at a
// time, into a row that is valid otherwise; the library's own message is never read.
func endpointRefusal(e aliasconfig.Endpoint) string {
	refuses := func(edit func(*aliasconfig.Endpoint)) bool {
		row := probeEndpoint
		edit(&row)
		return aliasconfig.Config{RestCall: []aliasconfig.Endpoint{row}}.Validate() != nil
	}
	switch {
	case refuses(func(r *aliasconfig.Endpoint) { r.Method = e.Method }):
		return refusalMethod
	case refuses(func(r *aliasconfig.Endpoint) { r.BaseURL = e.BaseURL }):
		return refusalBaseURL
	case refuses(func(r *aliasconfig.Endpoint) { r.PathTemplate = e.PathTemplate }):
		return refusalPath
	default:
		return refusalRow
	}
}

func queryRefusal(q aliasconfig.Query) string {
	refuses := func(edit func(*aliasconfig.Query)) bool {
		row := probeQuery
		edit(&row)
		return aliasconfig.Config{SQLQuery: []aliasconfig.Query{row}}.Validate() != nil
	}
	switch {
	case refuses(func(r *aliasconfig.Query) { r.BaseURL = q.BaseURL }):
		return refusalBaseURL
	case refuses(func(r *aliasconfig.Query) { r.Path = q.Path }):
		return refusalPath
	default:
		return refusalRow
	}
}

func aliasCount(c aliasconfig.Config) int { return len(c.RestCall) }

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
	cfg, err := decodeAliasRows(raw, log)
	if err != nil {
		return aliasconfig.Config{}, fmt.Errorf("decode alias registry response: %w", err)
	}
	return validAliasRows(cfg, log), nil
}

// decodeAliasRows reads the registry answer one row at a time, so a row with a wrong-typed field
// (a string where the timeout is a number) is skipped and named instead of failing the whole fetch.
// An answer that is not a registry at all still fails.
func decodeAliasRows(raw []byte, log port.Logger) (aliasconfig.Config, error) {
	var rows struct {
		Version  int
		RestCall []json.RawMessage
		SQLQuery []json.RawMessage
	}
	if err := json.Unmarshal(raw, &rows); err != nil {
		return aliasconfig.Config{}, fmt.Errorf("decode alias registry: %w", err)
	}
	return aliasconfig.Config{
		Version:  rows.Version,
		RestCall: decodeEachRow[aliasconfig.Endpoint]("restCall", rows.RestCall, log),
		SQLQuery: decodeEachRow[aliasconfig.Query]("sqlQuery", rows.SQLQuery, log),
	}, nil
}

func decodeEachRow[T any](kind string, rows []json.RawMessage, log port.Logger) []T {
	var out []T
	for i, r := range rows {
		var row T
		if err := json.Unmarshal(r, &row); err != nil {
			log.Warn("connector alias row skipped: it cannot be decoded", map[string]any{"kind": kind, "index": i, "failure": "undecodable row"})
			continue
		}
		out = append(out, row)
	}
	return out
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
	skip := func(kind, alias, failure string) {
		log.Warn("connector alias row skipped: the library refuses it",
			map[string]any{"kind": kind, "alias": alias, "failure": failure})
	}
	for _, e := range in.RestCall {
		switch {
		case aliasconfig.Config{RestCall: []aliasconfig.Endpoint{e}}.Validate() != nil:
			skip("restCall", e.Alias, endpointRefusal(e))
			continue
		case seenRest[e.Alias]:
			skip("restCall", e.Alias, refusalDuplicate)
			continue
		}
		seenRest[e.Alias] = true
		out.RestCall = append(out.RestCall, e)
	}
	for _, q := range in.SQLQuery {
		switch {
		case aliasconfig.Config{SQLQuery: []aliasconfig.Query{q}}.Validate() != nil:
			skip("sqlQuery", q.Alias, queryRefusal(q))
			continue
		case seenSQL[q.Alias]:
			skip("sqlQuery", q.Alias, refusalDuplicate)
			continue
		}
		seenSQL[q.Alias] = true
		out.SQLQuery = append(out.SQLQuery, q)
	}
	return out
}
