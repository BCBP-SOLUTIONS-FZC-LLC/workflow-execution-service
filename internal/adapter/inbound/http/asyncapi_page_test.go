package http_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func hrfBetween(t *testing.T, s, open, closing string) string {
	t.Helper()
	start := strings.Index(s, open)
	require.GreaterOrEqual(t, start, 0, "page has no %q", open)
	rest := s[start+len(open):]
	end := strings.Index(rest, closing)
	require.GreaterOrEqual(t, end, 0, "page has no %q after %q", closing, open)
	return rest[:end]
}

func TestAsyncAPIPage_Served_EmbedsStyleAndScriptInlineAndLinksGoogleFonts(t *testing.T) {
	rig := hrfNewRig(t, "dev")

	w := rig.do(http.MethodGet, "/asyncapi", nil)

	require.Equal(t, http.StatusOK, w.Code)
	page := w.Body.String()
	css := hrfBetween(t, page, "<style>", "</style>")
	js := hrfBetween(t, page, "<script>", "</script>")
	assert.Contains(t, css, "--accent:", "the stylesheet rules must be inlined")
	assert.Contains(t, js, "function toggleTheme", "the script must be inlined")
	assert.Contains(t, page, `href="https://fonts.googleapis.com/css2?family=Poppins`, "the font comes from Google Fonts over the network")
	assert.NotContains(t, page, "<script src=", "no script is loaded from a separate URL")
	assert.NotRegexp(t, `<link[^>]*href="/`, page, "no stylesheet is loaded from this server, whatever the attribute order")
}
