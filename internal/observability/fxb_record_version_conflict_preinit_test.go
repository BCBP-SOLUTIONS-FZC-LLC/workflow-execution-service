package observability_test

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRegister_TheRecordVersionConflictCounterHasBothTableSeriesBeforeTheFirstConflict(t *testing.T) {
	series := gatheredSeries(t)["workflow_execution_service_record_version_conflict_total"]

	require.True(t, series["table=workflow_instance"], "workflow_instance series missing from a scrape")
	require.True(t, series["table=workflow_task"], "workflow_task series missing from a scrape")
}
