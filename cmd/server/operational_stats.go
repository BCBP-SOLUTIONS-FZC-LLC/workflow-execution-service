package main

import (
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

func recordOperationalStats(defaultQueue string) func(domain.OperationalStats) {
	return func(s domain.OperationalStats) {
		observability.RecordOperationalStats(
			s.DegradedInstances, s.OldestDegradedAgeSeconds, s.OldestReadyTaskAgeSeconds,
			observability.InstancesByQueueClass(s.InstancesByQueue, defaultQueue),
		)
		observability.SetGauge(
			observability.OldestReadyConnectorTaskAgeSeconds, s.OldestReadyConnectorTaskAgeSeconds,
		)
	}
}
