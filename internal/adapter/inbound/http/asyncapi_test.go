package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"bytes"

	"github.com/gin-gonic/gin"
	"gopkg.in/yaml.v3"
)

// repoRoot locates the module root from this test file's own path so the
// real api/asyncapi.yaml (loaded relative to the process's working
// directory, per asyncAPISpecPath) can be found regardless of go test's
// per-package working directory.
func repoRoot(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed to resolve this test file's path")
	}
	// this file: <root>/internal/adapter/inbound/http/asyncapi_test.go
	root, err := filepath.Abs(filepath.Join(filepath.Dir(thisFile), "..", "..", "..", ".."))
	if err != nil {
		t.Fatalf("resolve repo root: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, asyncAPISpecPath)); err != nil {
		t.Fatalf("computed repo root %q does not contain %s: %v", root, asyncAPISpecPath, err)
	}
	return root
}

func loadRealSpec(t *testing.T) *asyncSpec {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), asyncAPISpecPath))
	if err != nil {
		t.Fatalf("read real spec: %v", err)
	}
	var spec asyncSpec
	if err := yaml.Unmarshal(raw, &spec); err != nil {
		t.Fatalf("unmarshal real spec: %v", err)
	}
	return &spec
}

func mustSpec(t *testing.T, yamlText string) *asyncSpec {
	t.Helper()
	var spec asyncSpec
	if err := yaml.Unmarshal([]byte(yamlText), &spec); err != nil {
		t.Fatalf("unmarshal synthetic spec: %v\n---\n%s", err, yamlText)
	}
	return &spec
}

// ---------------------------------------------------------------------
// AsyncAPIHandler — real HTTP request, real spec file, real error paths.
// ---------------------------------------------------------------------

func TestAsyncAPIHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("success renders the real spec as HTML", func(t *testing.T) {
		t.Chdir(repoRoot(t))
		t.Setenv("ENV", "staging")

		r := gin.New()
		r.GET("/asyncapi", AsyncAPIHandler)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/asyncapi", nil))

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
		}
		if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
			t.Fatalf("Content-Type = %q, want text/html prefix", ct)
		}
		body := w.Body.String()
		for _, want := range []string{
			"workflow-execution-svc events", // info.title
			"wf-workflow-events",            // static SNS topic reference
			`id="msg-WorkflowInstanceStarted"`,
			`id="schema-EventEnvelope"`,
			"STAGING", // ENV env var plumbed through
			"#f59e0b", // staging badge color
		} {
			if !strings.Contains(body, want) {
				t.Errorf("response body missing %q", want)
			}
		}
	})

	t.Run("missing spec file returns 500", func(t *testing.T) {
		t.Chdir(t.TempDir())

		r := gin.New()
		r.GET("/asyncapi", AsyncAPIHandler)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/asyncapi", nil))

		if w.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want 500", w.Code)
		}
		var resp map[string]string
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode error body: %v", err)
		}
		if !strings.Contains(resp["error"], "could not read AsyncAPI spec") {
			t.Errorf("error = %q, want ReadFile-failure message", resp["error"])
		}
	})

	t.Run("unparseable spec file returns 500", func(t *testing.T) {
		tmp := t.TempDir()
		specDir := filepath.Join(tmp, filepath.Dir(asyncAPISpecPath))
		if err := os.MkdirAll(specDir, 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(filepath.Join(tmp, asyncAPISpecPath), []byte("asyncapi: [1, 2\n"), 0o644); err != nil {
			t.Fatalf("write bad spec: %v", err)
		}
		t.Chdir(tmp)

		r := gin.New()
		r.GET("/asyncapi", AsyncAPIHandler)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/asyncapi", nil))

		if w.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want 500", w.Code)
		}
		var resp map[string]string
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode error body: %v", err)
		}
		if !strings.Contains(resp["error"], "could not parse AsyncAPI spec") {
			t.Errorf("error = %q, want Unmarshal-failure message", resp["error"])
		}
	})
}

// ---------------------------------------------------------------------
// asyncSchema.UnmarshalYAML
// ---------------------------------------------------------------------

func TestAsyncSchemaUnmarshalYAML(t *testing.T) {
	t.Run("preserves YAML property declaration order, not alphabetical", func(t *testing.T) {
		var sc asyncSchema
		err := yaml.Unmarshal([]byte(`
type: object
required: [alpha, gamma]
properties:
  zeta: { type: string }
  alpha: { type: string }
  beta: { type: string }
  gamma: { type: string }
`), &sc)
		if err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		want := []string{"zeta", "alpha", "beta", "gamma"}
		if strings.Join(sc.PropertyOrder, ",") != strings.Join(want, ",") {
			t.Errorf("PropertyOrder = %v, want %v", sc.PropertyOrder, want)
		}
		if len(sc.Properties) != 4 {
			t.Errorf("len(Properties) = %d, want 4", len(sc.Properties))
		}
	})

	t.Run("$ref schema decodes Ref without properties", func(t *testing.T) {
		var sc asyncSchema
		if err := yaml.Unmarshal([]byte(`$ref: '#/components/schemas/Other'`), &sc); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if sc.Ref != "#/components/schemas/Other" {
			t.Errorf("Ref = %q", sc.Ref)
		}
		if len(sc.Properties) != 0 {
			t.Errorf("Properties = %v, want empty", sc.Properties)
		}
	})

	t.Run("allOf composition decodes each branch", func(t *testing.T) {
		var sc asyncSchema
		err := yaml.Unmarshal([]byte(`
allOf:
  - $ref: '#/components/schemas/Base'
  - type: object
    required: [data]
    properties:
      data: { $ref: '#/components/schemas/Payload' }
`), &sc)
		if err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if len(sc.AllOf) != 2 {
			t.Fatalf("len(AllOf) = %d, want 2", len(sc.AllOf))
		}
		if sc.AllOf[0].Ref != "#/components/schemas/Base" {
			t.Errorf("AllOf[0].Ref = %q", sc.AllOf[0].Ref)
		}
		if _, ok := sc.AllOf[1].Properties["data"]; !ok {
			t.Errorf("AllOf[1].Properties missing 'data': %v", sc.AllOf[1].Properties)
		}
	})

	t.Run("schema without a properties key leaves PropertyOrder nil", func(t *testing.T) {
		var sc asyncSchema
		if err := yaml.Unmarshal([]byte(`type: object`), &sc); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if sc.PropertyOrder != nil {
			t.Errorf("PropertyOrder = %v, want nil", sc.PropertyOrder)
		}
	})

	t.Run("type mismatch surfaces a wrapped decode error", func(t *testing.T) {
		var sc asyncSchema
		err := yaml.Unmarshal([]byte("type: object\nrequired: 5\n"), &sc)
		if err == nil {
			t.Fatal("expected an error")
		}
		if !strings.Contains(err.Error(), "decode asyncapi schema node") {
			t.Errorf("error = %q, want to mention 'decode asyncapi schema node'", err.Error())
		}
	})
}

// ---------------------------------------------------------------------
// asyncProp.UnmarshalYAML — including the documented multi-type
// (promoted_from_version_id-shaped) nullable field bug.
// ---------------------------------------------------------------------

func TestAsyncPropUnmarshalYAML(t *testing.T) {
	t.Run("scalar type", func(t *testing.T) {
		var p asyncProp
		if err := yaml.Unmarshal([]byte(`{type: string, format: uuid}`), &p); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if p.Type != "string" || p.Format != "uuid" {
			t.Errorf("got Type=%q Format=%q", p.Type, p.Format)
		}
	})

	t.Run("nullable multi-type field joins and drops null (promoted_from_version_id shape)", func(t *testing.T) {
		var p asyncProp
		if err := yaml.Unmarshal([]byte(`{type: ["string", "null"], format: uuid}`), &p); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if p.Type != "string" {
			t.Errorf("Type = %q, want %q", p.Type, "string")
		}
	})

	t.Run("multi-type field with two non-null members joins with a pipe", func(t *testing.T) {
		var p asyncProp
		if err := yaml.Unmarshal([]byte(`{type: ["string", "integer"]}`), &p); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if p.Type != "string|integer" {
			t.Errorf("Type = %q, want %q", p.Type, "string|integer")
		}
	})

	t.Run("$ref prop", func(t *testing.T) {
		var p asyncProp
		if err := yaml.Unmarshal([]byte(`{$ref: '#/components/schemas/Foo'}`), &p); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if p.Ref != "#/components/schemas/Foo" {
			t.Errorf("Ref = %q", p.Ref)
		}
	})

	t.Run("array with nested items schema", func(t *testing.T) {
		var p asyncProp
		if err := yaml.Unmarshal([]byte(`{type: array, items: {type: string, format: uuid}}`), &p); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if p.Type != "array" || p.Items == nil || p.Items.Type != "string" || p.Items.Format != "uuid" {
			t.Errorf("got %+v, items=%+v", p, p.Items)
		}
	})

	t.Run("enum values", func(t *testing.T) {
		var p asyncProp
		if err := yaml.Unmarshal([]byte(`{type: string, enum: [a, b, c]}`), &p); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if strings.Join(p.Enum, ",") != "a,b,c" {
			t.Errorf("Enum = %v", p.Enum)
		}
	})

	t.Run("type mismatch surfaces a wrapped decode error", func(t *testing.T) {
		var p asyncProp
		err := yaml.Unmarshal([]byte("type: string\nformat: [1,2]\n"), &p)
		if err == nil {
			t.Fatal("expected an error")
		}
		if !strings.Contains(err.Error(), "decode asyncapi prop node") {
			t.Errorf("error = %q, want to mention 'decode asyncapi prop node'", err.Error())
		}
	})
}

// ---------------------------------------------------------------------
// flattenTypeNode
// ---------------------------------------------------------------------

func TestFlattenTypeNode(t *testing.T) {
	cases := []struct {
		name string
		node *yaml.Node
		want string
	}{
		{"scalar", &yaml.Node{Kind: yaml.ScalarNode, Value: "string"}, "string"},
		{
			"sequence drops null", &yaml.Node{Kind: yaml.SequenceNode, Content: []*yaml.Node{
				{Kind: yaml.ScalarNode, Value: "string"},
				{Kind: yaml.ScalarNode, Value: "null"},
			}}, "string",
		},
		{
			"sequence with two non-null members joins with pipe", &yaml.Node{Kind: yaml.SequenceNode, Content: []*yaml.Node{
				{Kind: yaml.ScalarNode, Value: "string"},
				{Kind: yaml.ScalarNode, Value: "integer"},
			}}, "string|integer",
		},
		{"empty sequence", &yaml.Node{Kind: yaml.SequenceNode}, ""},
		{"mapping node falls back to empty", &yaml.Node{Kind: yaml.MappingNode}, ""},
		{"document node falls back to empty", &yaml.Node{Kind: yaml.DocumentNode}, ""},
		{"alias node falls back to empty", &yaml.Node{Kind: yaml.AliasNode}, ""},
		{"unknown kind falls back to empty", &yaml.Node{Kind: 0}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := flattenTypeNode(tc.node); got != tc.want {
				t.Errorf("flattenTypeNode() = %q, want %q", got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------
// sortedKeys
// ---------------------------------------------------------------------

func TestSortedKeys(t *testing.T) {
	t.Run("empty map", func(t *testing.T) {
		if got := sortedKeys(map[string]int{}); len(got) != 0 {
			t.Errorf("got %v, want empty", got)
		}
	})
	t.Run("nil map", func(t *testing.T) {
		if got := sortedKeys[int](nil); len(got) != 0 {
			t.Errorf("got %v, want empty", got)
		}
	})
	t.Run("sorts lexically regardless of insertion order", func(t *testing.T) {
		got := sortedKeys(map[string]int{"c": 1, "a": 2, "b": 3})
		if strings.Join(got, ",") != "a,b,c" {
			t.Errorf("got %v", got)
		}
	})
}

// ---------------------------------------------------------------------
// isInboundMessage
// ---------------------------------------------------------------------

func TestIsInboundMessage(t *testing.T) {
	cases := map[string]bool{
		"DelegationStartedInbound": true,
		"WorkflowInstanceStarted":  false,
		"":                         false,
		"InboundFoo":               false, // prefix, not suffix
	}
	for name, want := range cases {
		if got := isInboundMessage(name); got != want {
			t.Errorf("isInboundMessage(%q) = %v, want %v", name, got, want)
		}
	}
}

// ---------------------------------------------------------------------
// propType / typeHTML
// ---------------------------------------------------------------------

func TestPropType(t *testing.T) {
	cases := []struct {
		name string
		p    *asyncProp
		want string
	}{
		{"ref", &asyncProp{Ref: "#/components/schemas/Foo"}, "Foo"},
		{"plain scalar", &asyncProp{Type: "string"}, "string"},
		{"scalar with format", &asyncProp{Type: "string", Format: "uuid"}, "string(uuid)"},
		{"array of scalar items", &asyncProp{Type: "array", Items: &asyncProp{Type: "string"}}, "array<string>"},
		{"array of ref items", &asyncProp{Type: "array", Items: &asyncProp{Ref: "#/components/schemas/Bar"}}, "array<Bar>"},
		{"array without items falls back to bare type", &asyncProp{Type: "array"}, "array"},
		{"array with empty items falls back to bare type", &asyncProp{Type: "array", Items: &asyncProp{}}, "array"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := propType(tc.p); got != tc.want {
				t.Errorf("propType() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestTypeHTML(t *testing.T) {
	t.Run("ref renders an anchor to the schema section", func(t *testing.T) {
		got := typeHTML(&asyncProp{Ref: "#/components/schemas/Foo"})
		if !strings.Contains(got, `href="#schema-Foo"`) || !strings.Contains(got, ">Foo<") {
			t.Errorf("got %q", got)
		}
	})

	t.Run("array of refs renders a nested anchor", func(t *testing.T) {
		got := typeHTML(&asyncProp{Type: "array", Items: &asyncProp{Ref: "#/components/schemas/Bar"}})
		if !strings.Contains(got, "array&lt;") || !strings.Contains(got, `href="#schema-Bar"`) {
			t.Errorf("got %q", got)
		}
	})

	t.Run("plain type renders a bare span", func(t *testing.T) {
		got := typeHTML(&asyncProp{Type: "string", Format: "uuid"})
		if got != `<span class="prop-type">string(uuid)</span>` {
			t.Errorf("got %q", got)
		}
	})

	t.Run("schema names are HTML-escaped, never injected raw", func(t *testing.T) {
		got := typeHTML(&asyncProp{Ref: "#/components/schemas/A<script>B"})
		if strings.Contains(got, "<script>") {
			t.Errorf("unescaped script tag leaked into HTML: %q", got)
		}
		if !strings.Contains(got, "&lt;script&gt;") {
			t.Errorf("expected escaped tag, got %q", got)
		}
	})
}

// ---------------------------------------------------------------------
// snsEventType / walkYAML
// ---------------------------------------------------------------------

type bindingsHolder struct {
	Bindings yaml.Node `yaml:"bindings"`
}

func TestSnsEventType(t *testing.T) {
	t.Run("nil node", func(t *testing.T) {
		if got := snsEventType(nil); got != "" {
			t.Errorf("got %q", got)
		}
	})

	t.Run("zero-value node", func(t *testing.T) {
		if got := snsEventType(&yaml.Node{}); got != "" {
			t.Errorf("got %q", got)
		}
	})

	t.Run("present, via a real message-shaped decode", func(t *testing.T) {
		var h bindingsHolder
		err := yaml.Unmarshal([]byte(`
bindings:
  sns:
    messageAttributes:
      event_type:
        value: workflow.instance.started
`), &h)
		if err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if got := snsEventType(&h.Bindings); got != "workflow.instance.started" {
			t.Errorf("got %q", got)
		}
	})

	t.Run("missing path returns empty", func(t *testing.T) {
		var h bindingsHolder
		if err := yaml.Unmarshal([]byte("bindings:\n  sns:\n    name: some-topic\n"), &h); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if got := snsEventType(&h.Bindings); got != "" {
			t.Errorf("got %q", got)
		}
	})
}

func TestWalkYAML(t *testing.T) {
	t.Run("nil node", func(t *testing.T) {
		if got := walkYAML(nil, "a"); got != "" {
			t.Errorf("got %q", got)
		}
	})

	t.Run("no keys", func(t *testing.T) {
		var n yaml.Node
		if err := yaml.Unmarshal([]byte("a: 1\n"), &n); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if got := walkYAML(&n); got != "" {
			t.Errorf("got %q", got)
		}
	})

	t.Run("unwraps a top-level document node", func(t *testing.T) {
		var n yaml.Node
		if err := yaml.Unmarshal([]byte("a:\n  b:\n    c: hi\n"), &n); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if n.Kind != yaml.DocumentNode {
			t.Fatalf("test assumption broken: top-level Unmarshal into *yaml.Node did not yield a DocumentNode")
		}
		if got := walkYAML(&n, "a", "b", "c"); got != "hi" {
			t.Errorf("got %q", got)
		}
	})

	t.Run("missing key at an intermediate level returns empty", func(t *testing.T) {
		var n yaml.Node
		if err := yaml.Unmarshal([]byte("a:\n  x: 1\n"), &n); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if got := walkYAML(&n, "a", "b"); got != "" {
			t.Errorf("got %q", got)
		}
	})

	t.Run("scalar encountered before keys are exhausted returns empty", func(t *testing.T) {
		var n yaml.Node
		if err := yaml.Unmarshal([]byte("a: hi\n"), &n); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if got := walkYAML(&n, "a", "b"); got != "" {
			t.Errorf("got %q", got)
		}
	})
}

// ---------------------------------------------------------------------
// resolveSchema
// ---------------------------------------------------------------------

const resolveSchemaFixture = `
components:
  schemas:
    Direct:
      type: object
      required: [a]
      properties:
        a: { type: string }

    ChainA: { $ref: '#/components/schemas/ChainB' }
    ChainB: { $ref: '#/components/schemas/ChainC' }
    ChainC:
      type: object
      required: [z]
      properties:
        z: { type: string, description: "chain end" }

    Dangling: { $ref: '#/components/schemas/DoesNotExist' }

    Base:
      type: object
      required: [a]
      properties:
        a: { type: string }
        shared: { type: string, description: "from base" }
    Extra:
      type: object
      required: [b]
      properties:
        b: { type: string }
        shared: { type: string, description: "from extra, wins" }
    Merged:
      allOf:
        - $ref: '#/components/schemas/Base'
        - $ref: '#/components/schemas/Extra'
`

func TestResolveSchema(t *testing.T) {
	spec := mustSpec(t, resolveSchemaFixture)
	comps := &spec.Comps

	t.Run("a schema with neither $ref nor allOf resolves to itself", func(t *testing.T) {
		sc := comps.Schemas["Direct"]
		got := resolveSchema(&sc, comps)
		if got != &sc {
			t.Errorf("expected identity, got a different pointer")
		}
	})

	t.Run("follows a multi-hop $ref chain to the terminal schema", func(t *testing.T) {
		sc := comps.Schemas["ChainA"]
		got := resolveSchema(&sc, comps)
		if len(got.PropertyOrder) != 1 || got.PropertyOrder[0] != "z" {
			t.Fatalf("PropertyOrder = %v", got.PropertyOrder)
		}
		if got.Properties["z"].Desc != "chain end" {
			t.Errorf("Properties[z].Desc = %q", got.Properties["z"].Desc)
		}
	})

	t.Run("a $ref to a schema not in components resolves to itself unexpanded", func(t *testing.T) {
		sc := comps.Schemas["Dangling"]
		got := resolveSchema(&sc, comps)
		if len(got.Properties) != 0 {
			t.Errorf("Properties = %v, want empty", got.Properties)
		}
		if got.Ref != "#/components/schemas/DoesNotExist" {
			t.Errorf("Ref = %q", got.Ref)
		}
	})

	t.Run("allOf merges properties, later branch wins on key collision", func(t *testing.T) {
		sc := comps.Schemas["Merged"]
		got := resolveSchema(&sc, comps)

		wantOrder := []string{"a", "shared", "b"}
		if strings.Join(got.PropertyOrder, ",") != strings.Join(wantOrder, ",") {
			t.Errorf("PropertyOrder = %v, want %v", got.PropertyOrder, wantOrder)
		}
		if got.Properties["shared"].Desc != "from extra, wins" {
			t.Errorf("Properties[shared].Desc = %q, want last-branch-wins", got.Properties["shared"].Desc)
		}
		if _, ok := got.Properties["a"]; !ok {
			t.Error("missing property 'a' from first allOf branch")
		}
		if _, ok := got.Properties["b"]; !ok {
			t.Error("missing property 'b' from second allOf branch")
		}
		wantRequired := map[string]bool{"a": true, "b": true}
		for _, r := range got.Required {
			delete(wantRequired, r)
		}
		if len(wantRequired) != 0 {
			t.Errorf("Required = %v missing entries", got.Required)
		}
	})
}

// ---------------------------------------------------------------------
// renderServers
// ---------------------------------------------------------------------

func TestRenderServers(t *testing.T) {
	t.Run("nil node renders nothing", func(t *testing.T) {
		var buf bytes.Buffer
		renderServers(&buf, nil)
		if buf.Len() != 0 {
			t.Errorf("got %q, want empty", buf.String())
		}
	})

	t.Run("zero-value node renders nothing", func(t *testing.T) {
		var buf bytes.Buffer
		renderServers(&buf, &yaml.Node{})
		if buf.Len() != 0 {
			t.Errorf("got %q, want empty", buf.String())
		}
	})

	t.Run("renders host/protocol/description and truncates a long description", func(t *testing.T) {
		type serversHolder struct {
			Servers yaml.Node `yaml:"servers"`
		}
		longDesc := strings.Repeat("d", 250)
		var h serversHolder
		err := yaml.Unmarshal([]byte(`
servers:
  primary:
    host: sns.example.com
    protocol: sns
    description: "`+longDesc+`"
`), &h)
		if err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		var buf bytes.Buffer
		renderServers(&buf, &h.Servers)
		got := buf.String()

		if !strings.Contains(got, `<span class="server-name">primary</span>`) {
			t.Errorf("missing server name, got %s", got)
		}
		if !strings.Contains(got, "SNS") { // protocol upper-cased
			t.Errorf("missing upper-cased protocol, got %s", got)
		}
		if !strings.Contains(got, "sns.example.com") {
			t.Errorf("missing host, got %s", got)
		}
		if strings.Contains(got, longDesc) {
			t.Error("full-length description present, expected truncation")
		}
		if !strings.Contains(got, strings.Repeat("d", 200)+"…") {
			t.Error("expected description truncated to 200 chars with ellipsis")
		}
	})
}

// ---------------------------------------------------------------------
// renderMessage
// ---------------------------------------------------------------------

const renderMessageFixture = `
components:
  messages:
    OutboundMsg:
      title: Outbound Sample
      description: Outbound description text
      payload: { $ref: '#/components/schemas/OutboundSchema' }
    NoTitleMsg:
      description: fallback title uses the message key
      payload: {}
    EventTypedInbound:
      summary: Explicit summary text
      description: unused because summary wins
      bindings:
        sns:
          messageAttributes:
            event_type:
              value: workflow.instance.started
      payload: { $ref: '#/components/schemas/EventTypedInbound' }
    DanglingPayloadMsg:
      title: Dangling Payload
      payload: { $ref: '#/components/schemas/NotRegistered' }
  schemas:
    OutboundSchema:
      type: object
      required: [id]
      properties:
        id: { type: string, format: uuid }
    EventTypedInbound:
      type: object
      properties:
        x: { type: string }
`

func TestRenderMessage(t *testing.T) {
	spec := mustSpec(t, renderMessageFixture)
	comps := &spec.Comps

	t.Run("outbound message gets the SEND badge and renders its payload table", func(t *testing.T) {
		msg := comps.Messages["OutboundMsg"]
		var buf bytes.Buffer
		renderMessage(&buf, "OutboundMsg", &msg, comps)
		got := buf.String()
		if !strings.Contains(got, `class="badge m-send"`) || !strings.Contains(got, ">SEND<") {
			t.Errorf("missing SEND badge: %s", got)
		}
		if !strings.Contains(got, "Outbound Sample") {
			t.Errorf("missing title: %s", got)
		}
		if !strings.Contains(got, "Outbound description text") {
			t.Errorf("summary should fall back to description: %s", got)
		}
		if !strings.Contains(got, "Payload:") || !strings.Contains(got, "OutboundSchema") {
			t.Errorf("missing payload schema label: %s", got)
		}
		if !strings.Contains(got, `prop-name">id<`) {
			t.Errorf("missing rendered payload property: %s", got)
		}
	})

	t.Run("missing title falls back to the message key", func(t *testing.T) {
		msg := comps.Messages["NoTitleMsg"]
		var buf bytes.Buffer
		renderMessage(&buf, "NoTitleMsg", &msg, comps)
		if !strings.Contains(buf.String(), `card-title">NoTitleMsg<`) {
			t.Errorf("expected fallback title, got %s", buf.String())
		}
	})

	t.Run("inbound message gets RECEIVE badge, explicit summary wins, and SNS event badge renders", func(t *testing.T) {
		msg := comps.Messages["EventTypedInbound"]
		var buf bytes.Buffer
		renderMessage(&buf, "EventTypedInbound", &msg, comps)
		got := buf.String()
		if !strings.Contains(got, `class="badge m-recv"`) || !strings.Contains(got, ">RECEIVE<") {
			t.Errorf("missing RECEIVE badge: %s", got)
		}
		if !strings.Contains(got, "Explicit summary text") {
			t.Errorf("expected explicit summary to win over description: %s", got)
		}
		if strings.Contains(got, "unused because summary wins") {
			t.Errorf("description should not render when summary is set: %s", got)
		}
		if !strings.Contains(got, "event_type: workflow.instance.started") {
			t.Errorf("missing SNS event_type badge: %s", got)
		}
	})

	t.Run("payload with no $ref renders no payload section", func(t *testing.T) {
		msg := comps.Messages["NoTitleMsg"] // payload: {}
		var buf bytes.Buffer
		renderMessage(&buf, "NoTitleMsg", &msg, comps)
		if strings.Contains(buf.String(), "Payload:") {
			t.Errorf("expected no payload section, got %s", buf.String())
		}
	})

	t.Run("payload $ref to a schema absent from components renders no payload section", func(t *testing.T) {
		msg := comps.Messages["DanglingPayloadMsg"]
		var buf bytes.Buffer
		renderMessage(&buf, "DanglingPayloadMsg", &msg, comps)
		got := buf.String()
		if strings.Contains(got, "Payload:") {
			t.Errorf("expected no payload section for an unregistered schema, got %s", got)
		}
		if !strings.Contains(got, "Dangling Payload") {
			t.Errorf("card should still render its title: %s", got)
		}
	})

	t.Run("real spec: inbound message with a nullable formatted field renders through the full pipeline", func(t *testing.T) {
		real := loadRealSpec(t)
		msg := real.Comps.Messages["DelegationStartedInbound"]
		var buf bytes.Buffer
		renderMessage(&buf, "DelegationStartedInbound", &msg, &real.Comps)
		got := buf.String()
		if !strings.Contains(got, `class="badge m-recv"`) {
			t.Errorf("expected RECEIVE badge for an Inbound-suffixed message: %s", got)
		}
		if !strings.Contains(got, "string(uuid)") {
			t.Errorf("expected scope_id's nullable-uuid type to render as string(uuid): %s", got)
		}
	})
}

// ---------------------------------------------------------------------
// renderSchema
// ---------------------------------------------------------------------

func TestRenderSchema(t *testing.T) {
	real := loadRealSpec(t)
	sc := real.Comps.Schemas["EventEnvelope"]

	var buf bytes.Buffer
	renderSchema(&buf, "EventEnvelope", &sc, &real.Comps)
	got := buf.String()

	if !strings.Contains(got, `id="schema-EventEnvelope"`) {
		t.Errorf("missing schema anchor: %s", got)
	}
	if !strings.Contains(got, `card-title" style="font-family:monospace">EventEnvelope<`) {
		t.Errorf("missing schema title: %s", got)
	}
	for _, field := range []string{"id", "source", "tenant_id", "specversion"} {
		if !strings.Contains(got, `prop-name">`+field+`<`) {
			t.Errorf("missing field %q in rendered schema: %s", field, got)
		}
	}
}

// ---------------------------------------------------------------------
// renderPropsTable
// ---------------------------------------------------------------------

func TestRenderPropsTable(t *testing.T) {
	t.Run("nil schema renders a no-properties message", func(t *testing.T) {
		var buf bytes.Buffer
		renderPropsTable(&buf, nil, "")
		if !strings.Contains(buf.String(), "No properties.") {
			t.Errorf("got %s", buf.String())
		}
	})

	t.Run("schema with an empty properties map renders a no-properties message", func(t *testing.T) {
		var buf bytes.Buffer
		renderPropsTable(&buf, &asyncSchema{}, "")
		if !strings.Contains(buf.String(), "No properties.") {
			t.Errorf("got %s", buf.String())
		}
	})

	t.Run("required fields sort before optional ones, each group keeping its relative order", func(t *testing.T) {
		var sc asyncSchema
		err := yaml.Unmarshal([]byte(`
type: object
required: [alpha, gamma]
properties:
  zeta: { type: string }
  alpha: { type: string }
  beta: { type: string }
  gamma: { type: string }
`), &sc)
		if err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		var buf bytes.Buffer
		renderPropsTable(&buf, &sc, "")
		got := buf.String()

		idx := func(name string) int { return strings.Index(got, `prop-name">`+name+`<`) }
		alpha, gamma, zeta, beta := idx("alpha"), idx("gamma"), idx("zeta"), idx("beta")
		for name, i := range map[string]int{"alpha": alpha, "gamma": gamma, "zeta": zeta, "beta": beta} {
			if i == -1 {
				t.Fatalf("field %q not found in output", name)
			}
		}
		if alpha >= zeta || gamma >= zeta || alpha >= beta || gamma >= beta {
			t.Errorf("required fields (alpha, gamma) did not sort before optional fields (zeta, beta): alpha=%d gamma=%d zeta=%d beta=%d", alpha, gamma, zeta, beta)
		}
		if zeta > beta {
			t.Errorf("optional fields lost their relative order: zeta=%d should come before beta=%d", zeta, beta)
		}
		if alpha > gamma {
			t.Errorf("required fields lost their relative order: alpha=%d should come before gamma=%d", alpha, gamma)
		}
		if !strings.Contains(got, `<span class="prop-req">required</span>`) {
			t.Error("missing required marker")
		}
	})

	t.Run("enum, item-level enum, example, and long-description truncation all render", func(t *testing.T) {
		var sc asyncSchema
		longDesc := strings.Repeat("x", 250)
		err := yaml.Unmarshal([]byte(`
type: object
properties:
  status: { type: string, enum: [a, b, c], description: "`+longDesc+`" }
  tags:
    type: array
    items: { type: string, enum: [x, "y"] }
  sample: { type: object, example: {foo: bar, n: 1} }
`), &sc)
		if err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		var buf bytes.Buffer
		renderPropsTable(&buf, &sc, "SchemaID")
		got := buf.String()

		if !strings.Contains(got, `<span class="enum-val">a</span>`) {
			t.Errorf("missing enum values: %s", got)
		}
		if !strings.Contains(got, "values:") || !strings.Contains(got, `<span class="enum-val">x</span>`) {
			t.Errorf("missing item-level enum: %s", got)
		}
		if !strings.Contains(got, `&#34;foo&#34;: &#34;bar&#34;`) {
			t.Errorf("missing rendered JSON example: %s", got)
		}
		if strings.Contains(got, longDesc) {
			t.Error("full-length description present, expected truncation")
		}
		if !strings.Contains(got, strings.Repeat("x", 200)+"…") {
			t.Error("expected description truncated to 200 chars with ellipsis")
		}
		if !strings.Contains(got, `id="field--SchemaID--status"`) {
			t.Errorf("expected a deep-link row id when schemaID is set: %s", got)
		}
	})

	t.Run("empty schemaID omits the deep-link row id", func(t *testing.T) {
		var sc asyncSchema
		if err := yaml.Unmarshal([]byte(`{type: object, properties: {a: {type: string}}}`), &sc); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		var buf bytes.Buffer
		renderPropsTable(&buf, &sc, "")
		if strings.Contains(buf.String(), "field--") {
			t.Errorf("did not expect a field id, got %s", buf.String())
		}
	})

	t.Run("no PropertyOrder at all falls back to alphabetical key order", func(t *testing.T) {
		// Same rationale as the fallback test below: a hand-built value
		// exercising renderPropsTable's own defensive branch for a schema
		// carrying Properties but no PropertyOrder whatsoever, a shape real
		// YAML decoding never produces (UnmarshalYAML always populates
		// PropertyOrder alongside a non-empty Properties map).
		sc := &asyncSchema{
			Properties: map[string]asyncProp{
				"zeta":  {Type: "string"},
				"alpha": {Type: "string"},
			},
		}
		var buf bytes.Buffer
		renderPropsTable(&buf, sc, "")
		got := buf.String()
		alphaIdx := strings.Index(got, `prop-name">alpha<`)
		zetaIdx := strings.Index(got, `prop-name">zeta<`)
		if alphaIdx == -1 || zetaIdx == -1 {
			t.Fatalf("expected both properties rendered, got %s", got)
		}
		if alphaIdx > zetaIdx {
			t.Errorf("expected alphabetical fallback order (alpha before zeta), got alpha=%d zeta=%d", alphaIdx, zetaIdx)
		}
	})

	t.Run("a Properties key missing from PropertyOrder still renders, appended", func(t *testing.T) {
		// Exercises renderPropsTable's own defensive fallback (a stale/partial
		// PropertyOrder next to a fuller Properties map) directly on a
		// hand-built value — not a shape asyncSchema's real YAML decoding
		// path would ever produce, since UnmarshalYAML always derives
		// PropertyOrder from the same properties node it decodes into Properties.
		sc := &asyncSchema{
			PropertyOrder: []string{"a"},
			Properties: map[string]asyncProp{
				"a": {Type: "string"},
				"b": {Type: "string"},
			},
		}
		var buf bytes.Buffer
		renderPropsTable(&buf, sc, "")
		got := buf.String()
		if !strings.Contains(got, `prop-name">a<`) || !strings.Contains(got, `prop-name">b<`) {
			t.Errorf("expected both properties rendered, got %s", got)
		}
	})
}

// ---------------------------------------------------------------------
// renderPage
// ---------------------------------------------------------------------

func TestRenderPage(t *testing.T) {
	t.Run("real spec renders title, sidebar links, and info description", func(t *testing.T) {
		real := loadRealSpec(t)
		var buf bytes.Buffer
		renderPage(&buf, real, "")
		got := buf.String()

		if !strings.Contains(got, "workflow-execution-svc events") {
			t.Errorf("missing title")
		}
		if !strings.Contains(got, `href="#msg-WorkflowInstanceStarted"`) {
			t.Errorf("missing sidebar message link")
		}
		if !strings.Contains(got, `href="#schema-EventEnvelope"`) {
			t.Errorf("missing sidebar schema link")
		}
		if !strings.Contains(got, "Event contract for the Workflow Execution Service") {
			t.Errorf("missing info description content")
		}
		if !strings.Contains(got, "DEV") || !strings.Contains(got, "#22c55e") {
			t.Errorf("empty env should default to green DEV badge")
		}
	})

	t.Run("env badge reflects production", func(t *testing.T) {
		real := loadRealSpec(t)
		var buf bytes.Buffer
		renderPage(&buf, real, "production")
		got := buf.String()
		if !strings.Contains(got, "PRODUCTION") || !strings.Contains(got, "#ec4b3c") {
			t.Errorf("expected red PRODUCTION badge, got relevant snippet absent")
		}
	})

	t.Run("env badge reflects staging via the abbreviated spelling", func(t *testing.T) {
		real := loadRealSpec(t)
		var buf bytes.Buffer
		renderPage(&buf, real, "stage")
		got := buf.String()
		if !strings.Contains(got, "STAGE") || !strings.Contains(got, "#f59e0b") {
			t.Errorf("expected yellow STAGE badge, got relevant snippet absent")
		}
	})

	t.Run("empty info description renders no info-desc block", func(t *testing.T) {
		spec := mustSpec(t, `
asyncapi: "3.0.0"
info:
  title: Empty Spec
  version: "0.0.1"
  description: ""
`)
		var buf bytes.Buffer
		renderPage(&buf, spec, "")
		if strings.Contains(buf.String(), `class="info-desc"`) {
			t.Errorf("expected no info-desc block for an empty description")
		}
		// No servers/messages/schemas at all must not panic and must render
		// empty (but present) Messages/Schemas sections.
		if !strings.Contains(buf.String(), `<div class="section" id="messages">`) {
			t.Errorf("messages section should still render its header")
		}
		if strings.Contains(buf.String(), `id="servers"`) {
			t.Errorf("absent servers node should skip the servers section entirely")
		}
	})

	t.Run("long info description is truncated", func(t *testing.T) {
		longDesc := strings.Repeat("a", 900)
		spec := mustSpec(t, `
asyncapi: "3.0.0"
info:
  title: Long Desc Spec
  version: "0.0.1"
  description: "`+longDesc+`"
`)
		var buf bytes.Buffer
		renderPage(&buf, spec, "")
		got := buf.String()
		if strings.Contains(got, longDesc) {
			t.Error("full-length description present, expected truncation")
		}
		if !strings.Contains(got, "[truncated — see api/asyncapi.yaml for full description]") {
			t.Error("expected truncation marker")
		}
	})
}
