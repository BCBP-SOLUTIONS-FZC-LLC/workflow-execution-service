package port_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/logger"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

func TestTheWorkflowTypeTheWorkerRunsHasAZeroSeededReplayFailureSeries(t *testing.T) {
	log, err := logger.NewLogger("dev")
	require.NoError(t, err)
	_ = gincommon.ObservabilityMiddlewares(gincommon.Config{
		Logger: log, ServiceName: "workflow-execution-service", BuildVersion: "test", Domain: observability.Domain, Environment: "test",
	})
	observability.Register()

	gathered, err := gincommon.MetricsGatherer().Gather()
	require.NoError(t, err)

	var found bool
	for _, mf := range gathered {
		if mf.GetName() != "workflow_execution_service_workflow_replay_failures_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				found = found || (lp.GetName() == "workflow_type" && lp.GetValue() == port.WorkflowTypeExecute)
			}
		}
	}
	require.True(t, found)
}
