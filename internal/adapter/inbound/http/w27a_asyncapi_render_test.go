package http

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

const w27aMinimalSpec = `
asyncapi: "3.0.0"
info:
  title: Badge Spec
  version: "0.0.1"
`

func w27aServe(t *testing.T, h gin.HandlerFunc) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/asyncapi", h)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/asyncapi", nil))
	return w
}

func w27aServersNode(t *testing.T, doc string) *yaml.Node {
	t.Helper()
	var holder struct {
		Servers yaml.Node `yaml:"servers"`
	}
	require.NoError(t, yaml.Unmarshal([]byte(doc), &holder))
	return &holder.Servers
}

func TestRenderPage_EnvBadge_TakesItsColourAndUpperCasedLabelFromTheEnvironmentName(t *testing.T) {
	const (
		red   = "#ec4b3c"
		amber = "#f59e0b"
		green = "#22c55e"
	)
	cases := []struct {
		name       string
		env        string
		wantLabel  string
		wantColour string
	}{
		{name: "prod", env: "prod", wantLabel: "PROD", wantColour: red},
		{name: "production", env: "production", wantLabel: "PRODUCTION", wantColour: red},
		{name: "mixed case prod", env: "Prod", wantLabel: "PROD", wantColour: red},
		{name: "upper case production", env: "PRODUCTION", wantLabel: "PRODUCTION", wantColour: red},
		{name: "staging", env: "staging", wantLabel: "STAGING", wantColour: amber},
		{name: "stage", env: "stage", wantLabel: "STAGE", wantColour: amber},
		{name: "mixed case staging", env: "Staging", wantLabel: "STAGING", wantColour: amber},
		{name: "local falls back to green", env: "local", wantLabel: "LOCAL", wantColour: green},
		{name: "test falls back to green", env: "test", wantLabel: "TEST", wantColour: green},
		{name: "an unknown name falls back to green", env: "qa-eu", wantLabel: "QA-EU", wantColour: green},
		{name: "empty shows DEV in green", env: "", wantLabel: "DEV", wantColour: green},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer

			renderPage(&buf, mustSpec(t, w27aMinimalSpec), tc.env)

			want := fmt.Sprintf(`<span class="badge" style="background:%s;color:#fff;margin-left:auto">%s</span>`, tc.wantColour, tc.wantLabel)
			assert.Contains(t, buf.String(), want, "the environment badge is the one right-aligned span")
		})
	}
}

func TestAsyncAPIHandler_RealSpecInDevEnvironment_ServesAFullPageListingEveryMessageAndSchema(t *testing.T) {
	spec := loadRealSpec(t)
	require.NotEmpty(t, spec.Info.Title)
	require.NotEmpty(t, spec.Info.Version)
	require.NotEmpty(t, spec.Comps.Messages)
	require.NotEmpty(t, spec.Comps.Schemas)

	w := w27aServe(t, AsyncAPIHandler("dev"))

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "text/html; charset=utf-8", w.Header().Get("Content-Type"))
	page := w.Body.String()
	assert.True(t, strings.HasPrefix(page, "<!DOCTYPE html>"), "the page starts with the doctype")
	assert.Contains(t, page, "<title>"+spec.Info.Title+" — AsyncAPI</title>", "the tab title joins the spec title and AsyncAPI with an em dash")
	assert.Contains(t, page, `<span class="badge badge-version">v`+spec.Info.Version+`</span>`)
	for _, name := range sortedKeys(spec.Comps.Messages) {
		assert.Contains(t, page, `id="msg-`+name+`"`, "message card for "+name)
		assert.Contains(t, page, `href="#msg-`+name+`"`, "sidebar link for message "+name)
	}
	for _, name := range sortedKeys(spec.Comps.Schemas) {
		assert.Contains(t, page, `id="schema-`+name+`"`, "schema card for "+name)
		assert.Contains(t, page, `href="#schema-`+name+`"`, "sidebar link for schema "+name)
	}
}

func TestAsyncSchemaUnmarshalYAML_PropertiesNodeThatIsNotAMapping_LeavesTheOrderUnset(t *testing.T) {
	cases := []struct {
		name         string
		doc          string
		wantErr      string
		wantNoOrder  bool
		wantTypeKept string
	}{
		{name: "null properties", doc: "type: object\nproperties: null\n", wantNoOrder: true, wantTypeKept: "object"},
		{name: "empty flow mapping", doc: "type: object\nproperties: {}\n", wantNoOrder: false, wantTypeKept: "object"},
		{name: "scalar properties is a decode error", doc: "type: object\nproperties: nope\n", wantErr: "decode asyncapi schema node"},
		{name: "sequence properties is a decode error", doc: "type: object\nproperties: [a, b]\n", wantErr: "decode asyncapi schema node"},
		{name: "a repeated properties key is a decode error, so the first match is the only one", doc: "type: object\nproperties:\n  a: {type: string}\nproperties:\n  b: {type: string}\n", wantErr: "decode asyncapi schema node"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var sc asyncSchema

			err := yaml.Unmarshal([]byte(tc.doc), &sc)

			if tc.wantErr != "" {
				require.Error(t, err)
				assert.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantTypeKept, sc.Type)
			assert.Empty(t, sc.Properties)
			if tc.wantNoOrder {
				assert.Nil(t, sc.PropertyOrder)
			} else {
				assert.NotNil(t, sc.PropertyOrder, "a mapping node, even an empty one, records an order list")
				assert.Empty(t, sc.PropertyOrder)
			}
		})
	}
}

const w27aAllOfFixture = `
components:
  schemas:
    First:
      type: object
      required: [id, kind]
      properties:
        id: { type: string }
        kind: { type: string }
    Second:
      type: object
      required: [kind, tenant]
      properties:
        kind: { type: string }
        tenant: { type: string }
    Target:
      type: object
      properties:
        from_target: { type: string }
    Both:
      $ref: '#/components/schemas/Target'
      allOf:
        - $ref: '#/components/schemas/First'
    Merged:
      allOf:
        - $ref: '#/components/schemas/First'
        - $ref: '#/components/schemas/Second'
`

func TestResolveSchema_AllOfMerge_RequiresEveryNameFromEitherPartAndShowsEachNameOnce(t *testing.T) {
	comps := &mustSpec(t, w27aAllOfFixture).Comps
	merged := comps.Schemas["Merged"]

	got := resolveSchema(&merged, comps)

	assert.ElementsMatch(t, []string{"id", "kind", "tenant"}, slices.Compact(slices.Sorted(slices.Values(got.Required))), "every name required by either part is required")
	assert.Equal(t, []string{"id", "kind", "tenant"}, got.PropertyOrder, "property order is first-seen and de-duplicated")
	var buf bytes.Buffer
	renderPropsTable(&buf, got, "Merged")
	assert.Equal(t, 3, strings.Count(buf.String(), `<span class="prop-req">required</span>`), "one marker per required field, not one per listing")
}

func TestResolveSchema_RefBesideAllOf_FollowsOnlyTheAllOf(t *testing.T) {
	comps := &mustSpec(t, w27aAllOfFixture).Comps
	both := comps.Schemas["Both"]

	got := resolveSchema(&both, comps)

	assert.Equal(t, []string{"id", "kind"}, got.PropertyOrder)
	assert.Contains(t, got.Properties, "id")
	assert.NotContains(t, got.Properties, "from_target", "the sibling $ref is not followed when allOf is present")
}

func TestRenderPage_NoServersNode_StillLinksTheServersAnchorButWritesNoSection(t *testing.T) {
	var buf bytes.Buffer

	renderPage(&buf, mustSpec(t, w27aMinimalSpec), "")

	page := buf.String()
	assert.Contains(t, page, `<a href="#servers">Servers</a>`, "the sidebar link is written whether or not a section exists")
	assert.NotContains(t, page, `id="servers"`)
}

func TestRenderServers_NodeThatIsNotAMapping_RendersAnEmptyCard(t *testing.T) {
	cases := []struct {
		name string
		doc  string
	}{
		{name: "sequence", doc: "servers:\n  - host: sns.example.com\n    protocol: sns\n"},
		{name: "scalar", doc: "servers: unavailable\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer

			renderServers(&buf, w27aServersNode(t, tc.doc))

			assert.Equal(t, `<div class="section" id="servers"><div class="section-title">Servers</div><div class="card"></div></div>`, buf.String())
		})
	}
}

func TestRenderServers_Mapping_RendersOneItemPerServerInFileOrder(t *testing.T) {
	var buf bytes.Buffer

	renderServers(&buf, w27aServersNode(t, `
servers:
  zeta:
    host: z.example.com
    protocol: sns
  alpha:
    host: a.example.com
    protocol: sqs
`))

	got := buf.String()
	assert.Equal(t, 2, strings.Count(got, `<div class="server-item">`))
	assert.Contains(t, got, `<span class="server-proto">SNS</span> <span class="server-host">z.example.com</span>`)
	assert.Contains(t, got, `<span class="server-proto">SQS</span> <span class="server-host">a.example.com</span>`)
	assert.Less(t, strings.Index(got, "zeta"), strings.Index(got, "alpha"), "servers keep the order of the file, not alphabetical order")
}

func TestDescriptionLimits_AtTheBoundary_ShowWholeAtTheLimitAndCutOneByteOver(t *testing.T) {
	t.Run("property description of 200 bytes is whole and 201 is cut to 200 plus an ellipsis", func(t *testing.T) {
		cases := []struct {
			name      string
			length    int
			wantShown string
		}{
			{name: "200 bytes", length: 200, wantShown: strings.Repeat("p", 200)},
			{name: "201 bytes", length: 201, wantShown: strings.Repeat("p", 200) + "…"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				var sc asyncSchema
				require.NoError(t, yaml.Unmarshal([]byte("type: object\nproperties:\n  f: { type: string, description: \""+strings.Repeat("p", tc.length)+"\" }\n"), &sc))
				var buf bytes.Buffer

				renderPropsTable(&buf, &sc, "S")

				assert.Contains(t, buf.String(), `<span class="prop-desc">`+tc.wantShown+`</span>`)
			})
		}
	})

	t.Run("server description of 200 bytes is whole and 201 is cut to 200 plus an ellipsis", func(t *testing.T) {
		cases := []struct {
			name      string
			length    int
			wantShown string
		}{
			{name: "200 bytes", length: 200, wantShown: strings.Repeat("s", 200)},
			{name: "201 bytes", length: 201, wantShown: strings.Repeat("s", 200) + "…"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				var buf bytes.Buffer

				renderServers(&buf, w27aServersNode(t, "servers:\n  primary:\n    host: h\n    protocol: sns\n    description: \""+strings.Repeat("s", tc.length)+"\"\n"))

				assert.Contains(t, buf.String(), `<div class="server-desc">`+tc.wantShown+`</div>`)
			})
		}
	})

	t.Run("info description of 800 bytes is whole and 801 is cut with the truncation notice", func(t *testing.T) {
		const notice = "[truncated — see api/asyncapi.yaml for full description]"
		cases := []struct {
			name       string
			length     int
			wantShown  string
			wantNotice bool
		}{
			{name: "800 bytes", length: 800, wantShown: strings.Repeat("i", 800)},
			{name: "801 bytes", length: 801, wantShown: strings.Repeat("i", 800) + "\n\n" + notice, wantNotice: true},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				spec := mustSpec(t, "asyncapi: \"3.0.0\"\ninfo:\n  title: T\n  version: \"1\"\n  description: \""+strings.Repeat("i", tc.length)+"\"\n")
				var buf bytes.Buffer

				renderPage(&buf, spec, "")

				page := buf.String()
				assert.Contains(t, page, `<div class="info-desc">`+tc.wantShown+`</div>`)
				assert.Equal(t, tc.wantNotice, strings.Contains(page, notice))
			})
		}
	})
}
