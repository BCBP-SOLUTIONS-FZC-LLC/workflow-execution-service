package observability_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

func gatheredSeries(t *testing.T) map[string]map[string]bool {
	t.Helper()
	initIdentity(t)
	gathered, err := gincommon.MetricsGatherer().Gather()
	require.NoError(t, err)
	series := map[string]map[string]bool{}
	for _, mf := range gathered {
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if series[mf.GetName()] == nil {
					series[mf.GetName()] = map[string]bool{}
				}
				series[mf.GetName()][lp.GetName()+"="+lp.GetValue()] = true
			}
		}
	}
	return series
}

func TestRegister_ThePagedReplayFailureCounterExistsBeforeItsFirstFailure(t *testing.T) {
	series := gatheredSeries(t)

	require.True(t, series["workflow_execution_service_workflow_replay_failures_total"]["workflow_type=Execute"])
}

func TestRegister_TheUnhandledScopeCounterExistsBeforeItsFirstEvent(t *testing.T) {
	series := gatheredSeries(t)

	require.True(t, series["workflow_execution_service_delegation_scope_unhandled_total"]["scope=unknown"])
}

func TestPreCreateConnectorDeadLetterSeries_EveryTypeAndTheUnknownLabelExistBeforeTheFirstDeadLetter(t *testing.T) {
	initIdentity(t)

	observability.PreCreateConnectorDeadLetterSeries("zero-seeded-type-a", "zero-seeded-type-b")

	series := gatheredSeries(t)["workflow_execution_service_connector_dispatch_dead_lettered_total"]
	for _, want := range []string{"connector_type=zero-seeded-type-a", "connector_type=zero-seeded-type-b", "connector_type=unknown"} {
		require.Truef(t, series[want], "expected series {%s}", want)
	}
}
