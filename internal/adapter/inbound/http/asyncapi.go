package http

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"html"
	"net/http"
	"os"
	"sort"
	"strings"

	"github.com/gin-gonic/gin"
	"gopkg.in/yaml.v3"
)

const asyncAPISpecPath = "api/asyncapi.yaml"

//go:embed static/asyncapi.css
var asyncAPICSS string

//go:embed static/asyncapi.js
var asyncAPIJS string

type asyncSpec struct {
	AsyncAPI string          `yaml:"asyncapi"`
	Info     asyncInfo       `yaml:"info"`
	Servers  yaml.Node       `yaml:"servers"`
	Channels yaml.Node       `yaml:"channels"`
	Ops      yaml.Node       `yaml:"operations"`
	Comps    asyncComponents `yaml:"components"`
}

type asyncInfo struct {
	Title   string `yaml:"title"`
	Version string `yaml:"version"`
	Desc    string `yaml:"description"`
}

type asyncComponents struct {
	Messages map[string]asyncMessage `yaml:"messages"`
	Schemas  map[string]asyncSchema  `yaml:"schemas"`
}

type asyncMessage struct {
	Name        string    `yaml:"name"`
	Title       string    `yaml:"title"`
	Summary     string    `yaml:"summary"`
	Desc        string    `yaml:"description"`
	ContentType string    `yaml:"contentType"`
	Payload     asyncRef  `yaml:"payload"`
	Bindings    yaml.Node `yaml:"bindings"`
}

type asyncRef struct {
	Ref string `yaml:"$ref"`
}

type asyncSchema struct {
	Type          string               `yaml:"type"`
	Required      []string             `yaml:"required"`
	Properties    map[string]asyncProp `yaml:"properties"`
	PropertyOrder []string             // YAML insertion order; populated by UnmarshalYAML
	AllOf         []asyncSchema        `yaml:"allOf"`
	Ref           string               `yaml:"$ref"`
}

func (s *asyncSchema) UnmarshalYAML(value *yaml.Node) error {
	type rawSchema asyncSchema // breaks the method set so Decode doesn't recurse here
	if err := value.Decode((*rawSchema)(s)); err != nil {
		return fmt.Errorf("decode asyncapi schema node: %w", err)
	}
	for i := 0; i+1 < len(value.Content); i += 2 {
		if value.Content[i].Value == "properties" {
			propsNode := value.Content[i+1]
			if propsNode.Kind == yaml.MappingNode {
				s.PropertyOrder = make([]string, 0, len(propsNode.Content)/2)
				for j := 0; j+1 < len(propsNode.Content); j += 2 {
					s.PropertyOrder = append(s.PropertyOrder, propsNode.Content[j].Value)
				}
			}
			break
		}
	}
	return nil
}

type asyncProp struct {
	Type    string     `yaml:"type"`
	Format  string     `yaml:"format"`
	Desc    string     `yaml:"description"`
	Example any        `yaml:"example"`
	Enum    []string   `yaml:"enum"`
	Ref     string     `yaml:"$ref"`
	Items   *asyncProp `yaml:"items"`
}

// UnmarshalYAML lets Type accept either a scalar ("string") or the
// JSON-Schema nullable-field idiom (["string", "null"]), joining a sequence
// with "|" and dropping "null" — plain yaml.Unmarshal into a string field
// errors on the sequence form, which this spec's own promoted_from_version_id
// property already uses.
func (p *asyncProp) UnmarshalYAML(value *yaml.Node) error {
	type rawProp struct {
		Type    yaml.Node  `yaml:"type"`
		Format  string     `yaml:"format"`
		Desc    string     `yaml:"description"`
		Example any        `yaml:"example"`
		Enum    []string   `yaml:"enum"`
		Ref     string     `yaml:"$ref"`
		Items   *asyncProp `yaml:"items"`
	}
	var raw rawProp
	if err := value.Decode(&raw); err != nil {
		return fmt.Errorf("decode asyncapi prop node: %w", err)
	}
	*p = asyncProp{
		Type:    flattenTypeNode(&raw.Type),
		Format:  raw.Format,
		Desc:    raw.Desc,
		Example: raw.Example,
		Enum:    raw.Enum,
		Ref:     raw.Ref,
		Items:   raw.Items,
	}
	return nil
}

func flattenTypeNode(n *yaml.Node) string {
	switch n.Kind {
	case yaml.SequenceNode:
		var parts []string
		for _, c := range n.Content {
			if c.Value != "null" {
				parts = append(parts, c.Value)
			}
		}
		return strings.Join(parts, "|")
	case yaml.ScalarNode:
		return n.Value
	case yaml.DocumentNode, yaml.MappingNode, yaml.AliasNode:
		// "type:" is never one of these shapes in an AsyncAPI/JSON-Schema
		// property — fall back to empty rather than guess at n.Value's
		// meaning for a node kind that doesn't carry a scalar value.
		return ""
	default:
		return ""
	}
}

func AsyncAPIHandler(c *gin.Context) {
	raw, err := os.ReadFile(asyncAPISpecPath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not read AsyncAPI spec: " + err.Error()})
		return
	}

	var spec asyncSpec
	if err := yaml.Unmarshal(raw, &spec); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not parse AsyncAPI spec: " + err.Error()})
		return
	}

	var buf bytes.Buffer
	renderPage(&buf, &spec, os.Getenv("ENV"))

	c.Header("Content-Type", "text/html; charset=utf-8")
	c.String(http.StatusOK, buf.String())
}

func renderPage(w *bytes.Buffer, s *asyncSpec, env string) {
	w.WriteString("<!DOCTYPE html>\n<html lang=\"en\" data-theme=\"dark\">\n<head>\n<meta charset=\"UTF-8\">\n<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\n<title>")
	w.WriteString(html.EscapeString(s.Info.Title))
	w.WriteString(` — AsyncAPI</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500;600;700&display=swap" rel="stylesheet">
<style>
`)
	w.WriteString(asyncAPICSS)
	w.WriteString(`</style>
</head>
<body>
`)

	envLabel := strings.ToUpper(env)
	if envLabel == "" {
		envLabel = "DEV"
	}
	envColor := "#22c55e" // green — dev/unknown
	switch strings.ToLower(env) {
	case "prod", "production":
		envColor = "#ec4b3c"
	case "staging", "stage":
		envColor = "#f59e0b"
	}
	fmt.Fprintf(w, `<div class="header">
  <div>
    <h1>%s</h1>
    <p>Events published to <strong>wf-workflow-events</strong> SNS topic</p>
  </div>
  <span class="badge badge-version">v%s</span>
  <span class="badge" style="background:linear-gradient(to right,#bd0283,#ec4b3c);color:#fff">AsyncAPI %s</span>
  <span class="badge" style="background:%s;color:#fff;margin-left:auto">%s</span>
  <button id="theme-btn" class="theme-toggle" onclick="toggleTheme()" title="Toggle light/dark mode">🌙</button>
</div>
`, html.EscapeString(s.Info.Title), html.EscapeString(s.Info.Version), html.EscapeString(s.AsyncAPI),
		envColor, html.EscapeString(envLabel))

	w.WriteString(`<div class="layout"><nav class="sidebar">`)
	w.WriteString(`<div style="padding:.75rem 1rem .25rem"><input id="search" placeholder="Search..." autocomplete="off" style="width:100%;padding:6px 10px;background:#0d0d0d;border:1px solid var(--border);color:#fff;border-radius:6px;font-size:.8rem;outline:none"></div>`)
	w.WriteString(`<div class="sidebar-group"><div class="sidebar-label">Overview</div>`)
	w.WriteString(`<a href="#info">Info</a><a href="#servers">Servers</a></div>`)
	w.WriteString(`<div class="sidebar-group"><div class="sidebar-label">Messages</div>`)
	for _, name := range sortedKeys(s.Comps.Messages) {
		fmt.Fprintf(w, `<a href="#msg-%s">%s</a>`, name, html.EscapeString(name))
	}
	w.WriteString(`</div>`)
	w.WriteString(`<div class="sidebar-group"><div class="sidebar-label">Schemas</div>`)
	for _, name := range sortedKeys(s.Comps.Schemas) {
		fmt.Fprintf(w, `<a href="#schema-%s">%s</a>`, name, html.EscapeString(name))
	}
	w.WriteString(`</div></nav>`)

	w.WriteString(`<main class="content">`)

	w.WriteString(`<div class="section" id="info">`)
	w.WriteString(`<div class="section-title">Info</div>`)
	if s.Info.Desc != "" {
		short := s.Info.Desc
		if len(short) > 800 {
			short = short[:800] + "\n\n[truncated — see api/asyncapi.yaml for full description]"
		}
		fmt.Fprintf(w, `<div class="info-desc">%s</div>`, html.EscapeString(short))
	}
	w.WriteString(`</div>`)

	renderServers(w, &s.Servers)

	w.WriteString(`<div class="section" id="messages"><div class="section-title">Messages</div>`)
	for _, name := range sortedKeys(s.Comps.Messages) {
		msg, ok := s.Comps.Messages[name]
		if !ok {
			continue
		}
		renderMessage(w, name, &msg, &s.Comps)
	}
	w.WriteString(`</div>`)

	w.WriteString(`<div class="section" id="schemas"><div class="section-title">Schemas</div>`)
	for _, name := range sortedKeys(s.Comps.Schemas) {
		sc, ok := s.Comps.Schemas[name]
		if !ok {
			continue
		}
		renderSchema(w, name, &sc, &s.Comps)
	}
	w.WriteString(`</div>`)

	w.WriteString(`</main></div>
<button onclick="window.scrollTo({top:0,behavior:'smooth'})" title="Back to top" style="position:fixed;bottom:24px;right:24px;width:40px;height:40px;background:linear-gradient(135deg,#8426b0,#bd0283);color:#fff;border:none;border-radius:50%;font-size:1.1rem;cursor:pointer;box-shadow:0 4px 12px rgba(132,38,176,.4);z-index:100;display:flex;align-items:center;justify-content:center;transition:transform .15s" onmouseover="this.style.transform='scale(1.1)'" onmouseout="this.style.transform='scale(1)'">↑</button>
<script>
`)
	w.WriteString(asyncAPIJS)
	w.WriteString(`</script>
</body></html>`)
}

func renderServers(w *bytes.Buffer, node *yaml.Node) {
	if node == nil || node.Kind == 0 {
		return
	}
	w.WriteString(`<div class="section" id="servers"><div class="section-title">Servers</div>`)
	w.WriteString(`<div class="card">`)

	if node.Kind == yaml.MappingNode {
		for i := 0; i+1 < len(node.Content); i += 2 {
			name := node.Content[i].Value
			val := node.Content[i+1]

			host, proto, desc := "", "", ""
			for j := 0; j+1 < len(val.Content); j += 2 {
				switch val.Content[j].Value {
				case "host":
					host = val.Content[j+1].Value
				case "protocol":
					proto = val.Content[j+1].Value
				case "description":
					desc = val.Content[j+1].Value
					if len(desc) > 200 {
						desc = desc[:200] + "…"
					}
				}
			}
			fmt.Fprintf(w, `<div class="server-item">
  <div><span class="server-name">%s</span></div>
  <div><span class="server-proto">%s</span> <span class="server-host">%s</span></div>
  <div class="server-desc">%s</div>
</div>`, html.EscapeString(name), html.EscapeString(strings.ToUpper(proto)),
				html.EscapeString(host), html.EscapeString(strings.TrimSpace(desc)))
		}
	}
	w.WriteString(`</div></div>`)
}

// isInboundMessage reports whether a components.messages key documents a
// consumed (receive) event rather than one this service publishes. Inbound
// entries deliberately carry an "Inbound" key suffix — see api/asyncapi.yaml
// — since their payload schema is owned/registered by the producing service,
// not this one.
func isInboundMessage(name string) bool {
	return strings.HasSuffix(name, "Inbound")
}

func renderMessage(w *bytes.Buffer, name string, msg *asyncMessage, comps *asyncComponents) {
	title := msg.Title
	if title == "" {
		title = name
	}
	summary := strings.TrimSpace(msg.Summary)
	if summary == "" {
		summary = strings.TrimSpace(msg.Desc)
	}

	eventType := snsEventType(&msg.Bindings)

	badgeClass, badgeLabel := "m-send", "SEND"
	if isInboundMessage(name) {
		badgeClass, badgeLabel = "m-recv", "RECEIVE"
	}

	fmt.Fprintf(w, `<div class="card" id="msg-%s">
<div class="card-header">
  <span class="badge %s">%s</span>
  <span class="card-title">%s</span>
  %s
  <button class="toggle-btn" type="button" tabindex="-1" aria-hidden="true">▸</button>
</div>
<div class="card-body" style="display:none">
`, name, badgeClass, badgeLabel, html.EscapeString(title),
		func() string {
			if eventType != "" {
				return fmt.Sprintf(`<span class="sns-attr">event_type: %s</span>`, html.EscapeString(eventType))
			}
			return ""
		}())

	if summary != "" {
		fmt.Fprintf(w, `<p class="card-summary" style="margin-bottom:.75rem">%s</p>`, html.EscapeString(summary))
	}

	payloadRef := strings.TrimPrefix(msg.Payload.Ref, "#/components/schemas/")
	if payloadRef != "" {
		if sc, ok := comps.Schemas[payloadRef]; ok {
			fmt.Fprintf(w, `<div style="font-size:.78rem;color:var(--muted);margin-bottom:.5rem">Payload: <span style="color:var(--code);font-family:monospace">%s</span></div>`, html.EscapeString(payloadRef))
			renderPropsTable(w, resolveSchema(&sc, comps), payloadRef)
		}
	}

	w.WriteString(`</div></div>`)
}

func renderSchema(w *bytes.Buffer, name string, sc *asyncSchema, comps *asyncComponents) {
	fmt.Fprintf(w, `<div class="card" id="schema-%s">
<div class="card-header"><span class="card-title" style="font-family:monospace">%s</span><button class="toggle-btn" type="button" tabindex="-1" aria-hidden="true">▸</button></div>
<div class="card-body" style="display:none">`, name, html.EscapeString(name))
	renderPropsTable(w, resolveSchema(sc, comps), name)
	w.WriteString(`</div></div>`)
}

func renderPropsTable(w *bytes.Buffer, sc *asyncSchema, schemaID string) {
	if sc == nil || len(sc.Properties) == 0 {
		w.WriteString(`<p style="color:var(--muted);font-size:.8rem">No properties.</p>`)
		return
	}
	reqSet := make(map[string]bool, len(sc.Required))
	for _, r := range sc.Required {
		reqSet[r] = true
	}

	w.WriteString(`<table class="prop-table"><thead><tr>
<th>Field</th><th>Type</th><th>Required</th><th>Description</th>
</tr></thead><tbody>`)

	keys := sc.PropertyOrder
	if len(keys) == 0 {
		keys = sortedKeys(sc.Properties)
	}
	seen := make(map[string]bool, len(keys))
	for _, k := range keys {
		seen[k] = true
	}
	for _, k := range sortedKeys(sc.Properties) {
		if !seen[k] {
			keys = append(keys, k)
		}
	}
	sort.SliceStable(keys, func(i, j int) bool {
		return reqSet[keys[i]] && !reqSet[keys[j]]
	})
	for _, k := range keys {
		v := sc.Properties[k]
		typeStr := typeHTML(&v)
		req := ""
		if reqSet[k] {
			req = `<span class="prop-req">required</span>`
		}
		desc := strings.TrimSpace(v.Desc)
		if len(desc) > 200 {
			desc = desc[:200] + "…"
		}
		descHTML := html.EscapeString(desc)

		enumHTML := ""
		if len(v.Enum) > 0 {
			var eb strings.Builder
			eb.WriteString(`<div class="enum-list">`)
			for _, ev := range v.Enum {
				eb.WriteString(`<span class="enum-val">`)
				eb.WriteString(html.EscapeString(ev))
				eb.WriteString(`</span>`)
			}
			eb.WriteString(`</div>`)
			enumHTML = eb.String()
		}
		itemEnumHTML := ""
		if v.Items != nil && len(v.Items.Enum) > 0 {
			var eb strings.Builder
			eb.WriteString(`<div style="margin-top:4px;font-size:.72rem;color:var(--muted)">values:</div><div class="enum-list">`)
			for _, ev := range v.Items.Enum {
				eb.WriteString(`<span class="enum-val">`)
				eb.WriteString(html.EscapeString(ev))
				eb.WriteString(`</span>`)
			}
			eb.WriteString(`</div>`)
			itemEnumHTML = eb.String()
		}

		exampleHTML := ""
		if v.Example != nil {
			if b, err := json.MarshalIndent(v.Example, "", "  "); err == nil {
				exampleHTML = `<pre style="background:#0d0d0d;border:1px solid rgba(132,38,176,.25);padding:6px 8px;border-radius:6px;font-size:.72rem;margin-top:6px;color:#e879f9;overflow-x:auto;white-space:pre-wrap">` +
					html.EscapeString(string(b)) + `</pre>`
			}
		}

		rowID := ""
		if schemaID != "" {
			rowID = fmt.Sprintf(` id="field--%s--%s"`, html.EscapeString(schemaID), html.EscapeString(k))
		}
		fmt.Fprintf(w, `<tr%s>
<td onclick="copyText('%s')" title="Click to copy" style="cursor:pointer"><span class="prop-name">%s</span></td>
<td>%s</td>
<td>%s</td>
<td><span class="prop-desc">%s</span>%s%s%s</td>
</tr>`, rowID, html.EscapeString(k), html.EscapeString(k), typeStr, req, descHTML, enumHTML, itemEnumHTML, exampleHTML)
	}

	w.WriteString(`</tbody></table>`)
}

func sortedKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func resolveSchema(sc *asyncSchema, comps *asyncComponents) *asyncSchema {
	if len(sc.AllOf) == 0 {
		if sc.Ref != "" {
			name := strings.TrimPrefix(sc.Ref, "#/components/schemas/")
			if sub, ok := comps.Schemas[name]; ok {
				return resolveSchema(&sub, comps)
			}
		}
		return sc
	}
	merged := &asyncSchema{Properties: make(map[string]asyncProp)}
	seen := make(map[string]bool)
	for _, sub := range sc.AllOf {
		resolved := resolveSchema(&sub, comps)
		if resolved == nil {
			continue
		}
		merged.Required = append(merged.Required, resolved.Required...)
		for _, k := range resolved.PropertyOrder {
			if !seen[k] {
				seen[k] = true
				merged.PropertyOrder = append(merged.PropertyOrder, k)
			}
		}
		for k, v := range resolved.Properties {
			merged.Properties[k] = v
		}
	}
	return merged
}

func propType(p *asyncProp) string {
	if p.Ref != "" {
		return strings.TrimPrefix(p.Ref, "#/components/schemas/")
	}
	t := p.Type
	if p.Format != "" {
		t += "(" + p.Format + ")"
	}
	if p.Type == "array" && p.Items != nil {
		if p.Items.Type != "" {
			t = "array<" + p.Items.Type + ">"
		} else if p.Items.Ref != "" {
			t = "array<" + strings.TrimPrefix(p.Items.Ref, "#/components/schemas/") + ">"
		}
	}
	return t
}

func typeHTML(p *asyncProp) string {
	if p.Ref != "" {
		name := strings.TrimPrefix(p.Ref, "#/components/schemas/")
		return fmt.Sprintf(`<a href="#schema-%s" class="prop-type schema-link">%s</a>`,
			html.EscapeString(name), html.EscapeString(name))
	}
	if p.Type == "array" && p.Items != nil && p.Items.Ref != "" {
		name := strings.TrimPrefix(p.Items.Ref, "#/components/schemas/")
		return fmt.Sprintf(`<span class="prop-type">array&lt;<a href="#schema-%s" class="prop-type schema-link">%s</a>&gt;</span>`,
			html.EscapeString(name), html.EscapeString(name))
	}
	return `<span class="prop-type">` + html.EscapeString(propType(p)) + `</span>`
}

func snsEventType(node *yaml.Node) string {
	if node == nil || node.Kind == 0 {
		return ""
	}
	return walkYAML(node, "sns", "messageAttributes", "event_type", "value")
}

func walkYAML(node *yaml.Node, keys ...string) string {
	if node == nil || len(keys) == 0 {
		return ""
	}
	target := keys[0]
	rest := keys[1:]

	n := node
	if n.Kind == yaml.DocumentNode && len(n.Content) > 0 {
		n = n.Content[0]
	}
	if n.Kind == yaml.MappingNode {
		for i := 0; i+1 < len(n.Content); i += 2 {
			if n.Content[i].Value == target {
				if len(rest) == 0 {
					return n.Content[i+1].Value
				}
				return walkYAML(n.Content[i+1], rest...)
			}
		}
	}
	return ""
}
