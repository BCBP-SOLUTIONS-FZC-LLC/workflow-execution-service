package http_test

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func hrfWriteSpecIn(t *testing.T, dir, title string) {
	t.Helper()
	specDir := filepath.Join(dir, "api")
	require.NoError(t, os.MkdirAll(specDir, 0o755))
	spec := "asyncapi: \"3.0.0\"\ninfo:\n  title: " + title + "\n  version: \"0.0.1\"\n  description: \"\"\n"
	require.NoError(t, os.WriteFile(filepath.Join(specDir, "asyncapi.yaml"), []byte(spec), 0o644))
}

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
	dir := t.TempDir()
	hrfWriteSpecIn(t, dir, "hrf inline assets")
	t.Chdir(dir)
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

func TestAsyncAPIPage_SpecEditedBetweenRequests_SecondResponseReflectsTheEdit(t *testing.T) {
	dir := t.TempDir()
	hrfWriteSpecIn(t, dir, "hrf first title")
	t.Chdir(dir)
	rig := hrfNewRig(t, "dev")

	first := rig.do(http.MethodGet, "/asyncapi", nil)
	hrfWriteSpecIn(t, dir, "hrf second title")
	second := rig.do(http.MethodGet, "/asyncapi", nil)

	require.Equal(t, http.StatusOK, first.Code)
	require.Equal(t, http.StatusOK, second.Code)
	assert.Contains(t, first.Body.String(), "hrf first title")
	assert.Contains(t, second.Body.String(), "hrf second title")
	assert.NotContains(t, second.Body.String(), "hrf first title", "nothing is cached between requests")
}
