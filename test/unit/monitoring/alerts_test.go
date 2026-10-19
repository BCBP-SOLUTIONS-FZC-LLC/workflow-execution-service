package monitoring_test

import (
	"os"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	alertName   = regexp.MustCompile(`(?m)^\s*- alert: (\S+)\s*$`)
	runbookSlug = regexp.MustCompile(`runbook: "(workflow-execution-service/[a-z0-9-]+)"`)
)

func read(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(raw)
}

func alertBlocks(t *testing.T, path string) map[string]string {
	t.Helper()
	text := read(t, path)
	idx := alertName.FindAllStringSubmatchIndex(text, -1)
	require.NotEmpty(t, idx)
	blocks := make(map[string]string, len(idx))
	for i, m := range idx {
		end := len(text)
		if i+1 < len(idx) {
			end = idx[i+1][0]
		}
		blocks[text[m[2]:m[3]]] = text[m[0]:end]
	}
	return blocks
}

func TestEveryAlertCarriesARunbookThatIsDocumented(t *testing.T) {
	runbooks := read(t, "../../../docs/observability/runbooks.md")
	for _, file := range []string{"../../../deploy/monitoring/app-alerts.yml", "../../../deploy/helm/templates/prometheusrule.yaml"} {
		for name, block := range alertBlocks(t, file) {
			m := runbookSlug.FindStringSubmatch(block)
			if !assert.Len(t, m, 2, "%s: alert %s has no runbook annotation", file, name) {
				continue
			}
			assert.Contains(t, runbooks, "`runbook: "+m[1]+"`", "%s: runbook %s is not documented", file, m[1])
			assert.Contains(t, runbooks, name, "%s: alert %s is not named in the runbooks", file, name)
		}
	}
}

func TestStaticAndChartAlertsAreTheSameSet(t *testing.T) {
	static := alertBlocks(t, "../../../deploy/monitoring/app-alerts.yml")
	chart := alertBlocks(t, "../../../deploy/helm/templates/prometheusrule.yaml")
	for name := range static {
		assert.Contains(t, chart, name)
	}
	for name := range chart {
		assert.Contains(t, static, name)
	}
}
