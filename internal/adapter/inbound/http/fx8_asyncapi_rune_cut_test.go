package http

import (
	"bytes"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestDescriptionLimits_AMultiByteCharacterAtTheLimit_IsNeverSplit(t *testing.T) {
	const notice = "[truncated — see api/asyncapi.yaml for full description]"

	t.Run("property description cuts before the character that straddles byte 200", func(t *testing.T) {
		desc := strings.Repeat("p", 199) + "é" + "tail"
		var sc asyncSchema
		require.NoError(t, yaml.Unmarshal([]byte("type: object\nproperties:\n  f: { type: string, description: \""+desc+"\" }\n"), &sc))
		var buf bytes.Buffer

		renderPropsTable(&buf, &sc, "S")

		page := buf.String()
		assert.True(t, utf8.ValidString(page), "the page is valid UTF-8")
		assert.Contains(t, page, `<span class="prop-desc">`+strings.Repeat("p", 199)+`…</span>`)
	})

	t.Run("server description cuts before the character that straddles byte 200", func(t *testing.T) {
		desc := strings.Repeat("s", 198) + "€" + "tail"
		var buf bytes.Buffer

		renderServers(&buf, w27aServersNode(t, "servers:\n  primary:\n    host: h\n    protocol: sns\n    description: \""+desc+"\"\n"))

		page := buf.String()
		assert.True(t, utf8.ValidString(page), "the page is valid UTF-8")
		assert.Contains(t, page, `<div class="server-desc">`+strings.Repeat("s", 198)+`…</div>`)
	})

	t.Run("a character that ends exactly at byte 200 is kept", func(t *testing.T) {
		desc := strings.Repeat("s", 198) + "é" + "tail"
		var buf bytes.Buffer

		renderServers(&buf, w27aServersNode(t, "servers:\n  primary:\n    host: h\n    protocol: sns\n    description: \""+desc+"\"\n"))

		assert.Contains(t, buf.String(), `<div class="server-desc">`+strings.Repeat("s", 198)+`é…</div>`)
	})

	t.Run("info description cuts before the character that straddles byte 800", func(t *testing.T) {
		desc := strings.Repeat("i", 799) + "é" + "tail"
		spec := mustSpec(t, "asyncapi: \"3.0.0\"\ninfo:\n  title: T\n  version: \"1\"\n  description: \""+desc+"\"\n")
		var buf bytes.Buffer

		renderPage(&buf, spec, "")

		page := buf.String()
		assert.True(t, utf8.ValidString(page), "the page is valid UTF-8")
		assert.Contains(t, page, `<div class="info-desc">`+strings.Repeat("i", 799)+"\n\n"+notice+`</div>`)
	})
}
