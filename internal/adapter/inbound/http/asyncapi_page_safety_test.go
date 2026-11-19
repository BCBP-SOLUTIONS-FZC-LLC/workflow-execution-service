package http

import (
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const pageSafetySpec = `
asyncapi: 3.0.0
info: {title: T, version: "1"}
components:
  messages:
    'Evil"One':
      payload: {$ref: '#/components/schemas/Evil"Shape'}
    Missing:
      payload: {$ref: '#/components/schemas/Nowhere'}
    Shared:
      payload: {$ref: '#/components/schemas/Shared'}
  schemas:
    'Evil"Shape':
      type: object
      properties:
        "it's":
          type: string
    Shared:
      type: object
      properties:
        status:
          type: string
`

func servePageSafety(t *testing.T, env string) string {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/asyncapi", asyncAPIHandlerFor([]byte(pageSafetySpec), env))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/asyncapi", nil))
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	return w.Body.String()
}

func TestAsyncAPIHandler_TheEnvironmentBadgeFollowsTheConfiguredEnvironmentNotTheENVVariable(t *testing.T) {
	t.Setenv("ENV", "production")

	page := servePageSafety(t, "staging")

	assert.Contains(t, page, `margin-left:auto">STAGING</span>`)
	assert.NotContains(t, page, "PRODUCTION")
}

func TestAsyncAPIHandler_NamesInIdsAndHrefsAreEscaped(t *testing.T) {
	page := servePageSafety(t, "dev")

	assert.NotContains(t, page, `id="msg-Evil"One"`)
	assert.NotContains(t, page, `href="#msg-Evil"One"`)
	assert.NotContains(t, page, `id="schema-Evil"Shape"`)
	assert.NotContains(t, page, `href="#schema-Evil"Shape"`)
	assert.Contains(t, page, `id="msg-Evil&#34;One"`)
	assert.Contains(t, page, `href="#msg-Evil&#34;One"`)
	assert.Contains(t, page, `id="schema-Evil&#34;Shape"`)
	assert.Contains(t, page, `href="#schema-Evil&#34;Shape"`)
}

func TestAsyncAPIHandler_TheCopyArgumentIsAJSONStringSoAnApostropheCannotBreakOutOfIt(t *testing.T) {
	page := servePageSafety(t, "dev")

	assert.Contains(t, page, `onclick="copyText(&#34;it&#39;s&#34;)"`)
	assert.NotContains(t, page, `copyText('it`)
}

func TestAsyncAPIHandler_NoIdAppearsTwiceOnThePage(t *testing.T) {
	page := servePageSafety(t, "dev")

	seen := map[string]int{}
	for _, m := range regexp.MustCompile(` id="([^"]*)"`).FindAllStringSubmatch(page, -1) {
		seen[m[1]]++
	}
	for id, n := range seen {
		assert.Equal(t, 1, n, "id %q appears %d times", id, n)
	}
	assert.Contains(t, page, `id="field--Shared--Shared--status"`, "the message card prefixes its rows with the message name")
	assert.Contains(t, page, `id="field--Shared--status"`, "the schema card keeps the row id the deep link reads")
}

func TestAsyncAPIHandler_AnUnresolvedPayloadRefIsShownAsNotFound(t *testing.T) {
	page := servePageSafety(t, "dev")

	assert.True(t, strings.Contains(page, "Payload: <span") && strings.Contains(page, ">Nowhere</span> (not found)"),
		"the card names the dangling payload and says it was not found")
}
