package observability_test

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

func TestRecordingHelpers_LiveCollectors_RecordTheObservedValue(t *testing.T) {
	obstest.Init(t)

	t.Run("ObserveHistogram adds one sample of the given size", func(t *testing.T) {
		const family = "workflow_execution_service_instance_start_duration_seconds"
		before := w28bOnly(t, family, nil)

		observability.ObserveHistogram(observability.InstanceStartDurationSeconds, 0.25)

		after := w28bOnly(t, family, nil)
		assert.Equal(t, before.count+1, after.count)
		assert.InDelta(t, before.sum+0.25, after.sum, 1e-9)
	})

	t.Run("ObserveHistogramVec adds one sample to the labelled series only", func(t *testing.T) {
		const family = "workflow_execution_service_workflow_activity_duration_seconds"
		labels := map[string]string{"activity_name": "w28bVecProbe", "outcome": "success"}
		var before w28bSample
		if found := w28bFind(t, family, labels); len(found) == 1 {
			before = found[0]
		}
		observability.ObserveHistogramVec(observability.WorkflowActivityDurationSeconds, 0.5, "w28bVecProbe", "success")
		observability.ObserveHistogramVec(observability.WorkflowActivityDurationSeconds, 0.25, "w28bVecProbe", "success")

		hit := w28bOnly(t, family, labels)
		assert.Equal(t, before.count+2, hit.count)
		assert.InDelta(t, before.sum+0.75, hit.sum, 1e-9)
		other := w28bFind(t, family, map[string]string{"activity_name": "w28bVecProbe", "outcome": "error"})
		assert.Empty(t, other, "the other outcome's series must not be touched")
	})

	t.Run("SetGauge stores the value and a later call replaces it", func(t *testing.T) {
		observability.SetGauge(observability.ConnectorAliasRegistryAgeSeconds, 7.5)
		assert.Equal(t, 7.5, testutil.ToFloat64(observability.ConnectorAliasRegistryAgeSeconds))

		observability.SetGauge(observability.ConnectorAliasRegistryAgeSeconds, 2)
		assert.Equal(t, 2.0, testutil.ToFloat64(observability.ConnectorAliasRegistryAgeSeconds))
	})
}

func TestIncCounterVec_LiveCollector_IncrementsOnlyTheLabelledSeries(t *testing.T) {
	obstest.Init(t)
	const family = "workflow_execution_service_delegation_scope_unhandled_total"
	valueOf := func(scope string) float64 {
		if found := w28bFind(t, family, map[string]string{"scope": scope}); len(found) == 1 {
			return found[0].value
		}
		return 0
	}
	beforeA, beforeB := valueOf("w28b-a"), valueOf("w28b-b")

	observability.IncCounterVec(observability.DelegationScopeUnhandledTotal, "w28b-a")
	observability.IncCounterVec(observability.DelegationScopeUnhandledTotal, "w28b-a")
	observability.IncCounterVec(observability.DelegationScopeUnhandledTotal, "w28b-b")

	assert.Equal(t, beforeA+2, valueOf("w28b-a"))
	assert.Equal(t, beforeB+1, valueOf("w28b-b"))
}

func TestRegister_CalledAgain_KeepsTheFirstCollectors(t *testing.T) {
	obstest.Init(t)
	counterVec := observability.RecordVersionConflictTotal
	histogramVec := observability.TaskSignalDurationSeconds
	gauge := observability.InstanceDegradedCurrent
	counter := observability.WFCacheHitsTotal
	startHistogram := observability.InstanceStartDurationSeconds

	observability.Register()
	obstest.Init(t)

	assert.Same(t, counterVec, observability.RecordVersionConflictTotal)
	assert.Same(t, histogramVec, observability.TaskSignalDurationSeconds)
	assert.Same(t, gauge, observability.InstanceDegradedCurrent)
	assert.Same(t, counter, observability.WFCacheHitsTotal)
	assert.Same(t, startHistogram, observability.InstanceStartDurationSeconds)
}

func TestRecordOperationalStats_NoQueuesInTheSample_DeletesEverySeries(t *testing.T) {
	cases := []struct {
		name   string
		sample map[string]int64
	}{
		{name: "nil map", sample: nil},
		{name: "empty map", sample: map[string]int64{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obstest.Init(t)
			observability.RecordOperationalStats(0, 0, 0, map[string]int64{"w28b-q1": 2, "w28b-q2": 5})
			require.Equal(t, 2, testutil.CollectAndCount(observability.InstancesByQueue))

			observability.RecordOperationalStats(0, 0, 0, tc.sample)

			assert.Equal(t, 0, testutil.CollectAndCount(observability.InstancesByQueue))
		})
	}
}

func TestRecordOperationalStats_QueueReturnsAfterRemoval_GetsItsSeriesBack(t *testing.T) {
	obstest.Init(t)
	observability.RecordOperationalStats(0, 0, 0, map[string]int64{"w28b-back": 4, "w28b-stay": 1})
	observability.RecordOperationalStats(0, 0, 0, map[string]int64{"w28b-stay": 1})
	require.Equal(t, 1, testutil.CollectAndCount(observability.InstancesByQueue))

	observability.RecordOperationalStats(0, 0, 0, map[string]int64{"w28b-back": 9, "w28b-stay": 1})

	assert.Equal(t, 2, testutil.CollectAndCount(observability.InstancesByQueue))
	assert.Equal(t, 9.0, w28bOnly(t, "workflow_execution_service_instances_by_queue", map[string]string{"queue": "w28b-back"}).value)
	assert.Equal(t, 1.0, w28bOnly(t, "workflow_execution_service_instances_by_queue", map[string]string{"queue": "w28b-stay"}).value)
	observability.RecordOperationalStats(0, 0, 0, nil)
}

func TestRegister_FreshProcess_ClosedLabelSetsAndSingleChildrenStartAtZero(t *testing.T) {
	if !w28bInFreshProcess(t) {
		return
	}
	obstest.Init(t)

	zeroCounters := map[string][]map[string]string{
		"workflow_execution_service_rls_violations_total": {
			{"violation_type": "missing_or_invalid_guc"}, {"violation_type": "cross_tenant_access"},
		},
		"workflow_execution_service_user_pause_failures_total": {
			{"stage": "read_task"}, {"stage": "read_instance"}, {"stage": "signal"},
		},
		"workflow_execution_service_internal_auth_rejected_total": {
			{"reason": "missing_user"}, {"reason": "missing_role"},
		},
		"workflow_execution_service_entitlements_call_errors_total": {
			{"reason": "transport"}, {"reason": "status"}, {"reason": "response"},
		},
		"workflow_execution_service_upstream_dependency_errors_total": {
			{"dependency": "definition_service"}, {"dependency": "iam"}, {"dependency": "token_service"},
		},
	}
	for family, sets := range zeroCounters {
		for _, labels := range sets {
			assert.Equalf(t, 0.0, w28bOnly(t, family, labels).value, "%s%v must exist at 0", family, labels)
		}
	}

	zeroHistograms := map[string][]map[string]string{
		"workflow_execution_service_task_signal_duration_seconds": {
			{"operation": "claim"}, {"operation": "complete"}, {"operation": "defer"},
		},
		"workflow_execution_service_entitlements_call_duration_seconds": {
			{"outcome": "success"}, {"outcome": "error"},
		},
	}
	for family, sets := range zeroHistograms {
		for _, labels := range sets {
			assert.Equalf(t, uint64(0), w28bOnly(t, family, labels).count, "%s%v must exist with no sample", family, labels)
		}
	}

	for _, family := range []string{
		"workflow_execution_service_instance_start_duration_seconds",
		"workflow_execution_service_delegation_reroute_duration_seconds",
	} {
		sample := w28bOnly(t, family, nil)
		assert.Equal(t, uint64(0), sample.count, "%s is visible with count 0 before any observation", family)
		for name := range sample.labels {
			assert.Containsf(t, []string{"domain", "service", "environment"}, name, "%s must carry no label of its own", family)
		}
	}

	const rls = "workflow_execution_service_rls_violations_total"
	require.Empty(t, w28bFind(t, rls, map[string]string{"violation_type": "w28b_unknown_type"}))
	observability.AddRLSViolations("w28b_unknown_type", 2)
	assert.Equal(t, 2.0, w28bOnly(t, rls, map[string]string{"violation_type": "w28b_unknown_type"}).value)
	assert.Equal(t, 0.0, w28bOnly(t, rls, map[string]string{"violation_type": "cross_tenant_access"}).value)
}

func TestAddRetentionRowsDeleted_FreshProcess_ZeroRowSweepCreatesTheSeriesAtZero(t *testing.T) {
	if !w28bInFreshProcess(t) {
		return
	}
	obstest.Init(t)
	const family = "workflow_execution_service_retention_rows_deleted_total"
	require.Empty(t, w28bFind(t, family, nil), "no series exists before a sweep reports")

	observability.AddRetentionRowsDeleted("outbox_events", 0)
	assert.Equal(t, 0.0, w28bOnly(t, family, map[string]string{"table": "outbox_events"}).value)

	observability.AddRetentionRowsDeleted("processed_events", 5)
	observability.AddRetentionRowsDeleted("processed_events", 3)
	assert.Equal(t, 8.0, w28bOnly(t, family, map[string]string{"table": "processed_events"}).value)
	assert.Equal(t, 0.0, w28bOnly(t, family, map[string]string{"table": "outbox_events"}).value)
}

func TestRecordingHelpers_FreshProcessBeforeRegister_AreNoOpsAndRecordNothingLater(t *testing.T) {
	if !w28bInFreshProcess(t) {
		return
	}
	require.Nil(t, observability.InstancesByQueue)
	require.Nil(t, observability.RetentionRowsDeletedTotal)
	require.Nil(t, observability.RLSViolationsTotal)
	require.Nil(t, observability.DelegationRerouteDurationSeconds, "single-child histograms are nil before Register")
	require.Nil(t, observability.InstanceStartDurationSeconds, "single-child histograms are nil before Register")

	require.NotPanics(t, func() {
		observability.RecordOperationalStats(3, 4, 5, map[string]int64{"w28b-early": 1})
		observability.AddRetentionRowsDeleted("outbox_events", 4)
		observability.AddRLSViolations("cross_tenant_access", 1)
		observability.IncCounterVec(observability.RecordVersionConflictTotal, "workflow_instance")
		observability.ObserveHistogram(observability.DelegationRerouteDurationSeconds, 1)
		observability.ObserveHistogram(observability.InstanceStartDurationSeconds, 1)
		observability.ObserveHistogramVec(observability.TaskSignalDurationSeconds, 1, "claim")
		observability.SetGauge(observability.InstanceDegradedCurrent, 1)
		observability.PreCreateConnectorDeadLetterSeries("w28b-early")
	})
	require.Nil(t, observability.InstancesByQueue, "a call before Register must not build a collector")
	require.Nil(t, observability.DelegationRerouteDurationSeconds)
	require.Nil(t, observability.InstanceStartDurationSeconds)

	obstest.Init(t)

	assert.Equal(t, 0.0, w28bOnly(t, "workflow_execution_service_rls_violations_total", map[string]string{"violation_type": "cross_tenant_access"}).value)
	assert.Empty(t, w28bFind(t, "workflow_execution_service_retention_rows_deleted_total", nil))
	assert.Equal(t, 0, testutil.CollectAndCount(observability.InstancesByQueue))
	assert.Equal(t, 0.0, testutil.ToFloat64(observability.InstanceDegradedCurrent))
	assert.Equal(t, uint64(0), w28bOnly(t, "workflow_execution_service_delegation_reroute_duration_seconds", nil).count)
	assert.Equal(t, uint64(0), w28bOnly(t, "workflow_execution_service_instance_start_duration_seconds", nil).count)
}
