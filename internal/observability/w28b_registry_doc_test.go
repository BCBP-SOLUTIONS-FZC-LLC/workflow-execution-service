package observability_test

import (
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

type w28bDocMetric struct {
	kind   string
	labels []string
}

// w28bReadRegistryDoc returns the service-specific rows of the metric inventory.
func w28bReadRegistryDoc(t *testing.T) map[string]w28bDocMetric {
	t.Helper()
	raw, err := os.ReadFile("../../docs/observability/metric-registry.md")
	require.NoError(t, err)
	rows := map[string]w28bDocMetric{}
	for _, line := range strings.Split(string(raw), "\n") {
		if !strings.HasPrefix(line, "| `workflow_execution_service_") {
			continue
		}
		cells := strings.SplitN(line, "|", 6)
		require.GreaterOrEqual(t, len(cells), 5, line)
		name := strings.Trim(strings.TrimSpace(cells[1]), "`")
		doc := w28bDocMetric{kind: strings.TrimSpace(cells[2])}
		if cell := strings.TrimSpace(cells[3]); cell != "-" {
			for _, l := range strings.Split(cell, ",") {
				doc.labels = append(doc.labels, strings.Trim(strings.TrimSpace(l), "`"))
			}
		}
		sort.Strings(doc.labels)
		rows[name] = doc
	}
	return rows
}

// w28bTouchEveryVec gives each labelled collector one series so it shows in a scrape.
func w28bTouchEveryVec() {
	observability.RecordVersionConflictTotal.WithLabelValues("w28b")
	observability.InternalEventsIngestTotal.WithLabelValues("w28b", "ok")
	observability.DelegationScopeUnhandledTotal.WithLabelValues("w28b")
	observability.RetentionRowsDeletedTotal.WithLabelValues("w28b")
	observability.ConnectorDispatchDeadLetteredTotal.WithLabelValues("w28b")
	observability.ConnectorTaskFailuresTotal.WithLabelValues("w28b", "w28b", "unknown", "false")
	observability.WorkflowActivityDurationSeconds.WithLabelValues("w28b", "success")
	observability.InternalEventsLastReceivedTimestamp.WithLabelValues("w28b")
	observability.WorkerQueueLastPollTimestamp.WithLabelValues("w28b")
	observability.RecordOperationalStats(0, 0, 0, map[string]int64{"w28b": 1})
}

func TestRegister_FreshProcess_CollectorsMatchTheMetricInventoryAndTheLabelsAlertsUse(t *testing.T) {
	w28bReadFiles(t, append([]string{"../../docs/observability/metric-registry.md"}, w28bAlertFiles...)...)
	if !w28bInFreshProcess(t) {
		return
	}
	obstest.Init(t)
	w28bTouchEveryVec()
	doc := w28bReadRegistryDoc(t)
	require.NotEmpty(t, doc)

	live := map[string]*dto.MetricFamily{}
	for name, mf := range w28bFamilies(t) {
		if strings.HasPrefix(name, "workflow_execution_service_") {
			live[name] = mf
		}
	}
	liveNames := make([]string, 0, len(live))
	for name := range live {
		liveNames = append(liveNames, name)
	}
	docNames := make([]string, 0, len(doc))
	for name := range doc {
		docNames = append(docNames, name)
	}
	sort.Strings(liveNames)
	sort.Strings(docNames)
	require.Equal(t, docNames, liveNames, "the registered collectors and the inventory must list the same metrics")

	constLabels := map[string]bool{"domain": true, "service": true, "environment": true}
	liveLabels := map[string][]string{}
	for name, mf := range live {
		want := doc[name]
		assert.Equalf(t, want.kind, strings.ToLower(mf.GetType().String()), "%s type", name)
		own := map[string]bool{}
		for _, s := range w28bSamplesOf(mf) {
			for label := range s.labels {
				if !constLabels[label] {
					own[label] = true
				}
			}
		}
		var labels []string
		for label := range own {
			labels = append(labels, label)
		}
		sort.Strings(labels)
		liveLabels[name] = labels
		assert.Equalf(t, want.labels, labels, "%s label names", name)
	}

	selector := regexp.MustCompile(`(workflow_execution_service_[a-z_]+)\{([^}]*)\}`)
	matcher := regexp.MustCompile(`([a-z_][a-z0-9_]*)\s*(?:=~|!~|!=|=)`)
	grouping := regexp.MustCompile(`by \(([a-z_, ]+)\)\s*\(\s*(?:[a-z_]+\()?(workflow_execution_service_[a-z_]+)`)
	bareToken := regexp.MustCompile(`workflow_execution_service_[a-z_]+`)
	checked, bareTokens := 0, 0
	for _, rules := range w28bAlertFiles {
		raw, err := os.ReadFile(rules)
		require.NoError(t, err)
		for _, line := range strings.Split(string(raw), "\n") {
			if !strings.Contains(line, "name: workflow_execution_service_") {
				for _, token := range bareToken.FindAllString(line, -1) {
					bareTokens++
					assert.Containsf(t, live, w28bBaseMetric(token), "%s references %s, which the service does not register", rules, token)
				}
			}
			for _, m := range selector.FindAllStringSubmatch(line, -1) {
				family := w28bBaseMetric(m[1])
				for _, lm := range matcher.FindAllStringSubmatch(m[2], -1) {
					checked++
					assert.Containsf(t, liveLabels[family], lm[1], "%s selects on %q, which %s does not carry", rules, lm[1], family)
				}
			}
			for _, m := range grouping.FindAllStringSubmatch(line, -1) {
				family := w28bBaseMetric(m[2])
				for _, label := range strings.Split(m[1], ",") {
					if label = strings.TrimSpace(label); label != "le" {
						checked++
						assert.Containsf(t, liveLabels[family], label, "%s groups %s by %q, which it does not carry", rules, family, label)
					}
				}
			}
		}
	}
	assert.GreaterOrEqual(t, checked, 5, "label uses checked in the alert rules")
	assert.GreaterOrEqual(t, bareTokens, 10, "metric names checked in the alert rules")
}

var w28bAlertFiles = []string{"../../deploy/monitoring/app-alerts.yml", "../../deploy/helm/templates/prometheusrule.yaml"}

// w28bReadFiles reads each file so that `go test` caches the result against them: the parent run of a
// fresh-process test would otherwise read none of them and keep a stale pass.
func w28bReadFiles(t *testing.T, paths ...string) {
	t.Helper()
	for _, p := range paths {
		_, err := os.ReadFile(p)
		require.NoError(t, err)
	}
}

func w28bBaseMetric(token string) string {
	for _, suffix := range []string{"_bucket", "_count", "_sum"} {
		if strings.HasSuffix(token, suffix) {
			return strings.TrimSuffix(token, suffix)
		}
	}
	return token
}
