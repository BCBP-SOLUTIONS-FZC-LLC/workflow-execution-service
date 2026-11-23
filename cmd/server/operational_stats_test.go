package main

import (
	"testing"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

func TestRecordOperationalStats_InstancesAreLabelledByQueueClassNeverByQueueName(t *testing.T) {
	obstest.Init(t)
	tenantQueue := domain.TenantTaskQueueName(uuid.New())

	recordOperationalStats("wf-queue-default")(domain.OperationalStats{
		InstancesByQueue: map[string]int64{"wf-queue-default": 4, tenantQueue: 3},
	})

	assert.Equal(t, 2, testutil.CollectAndCount(observability.InstancesByQueue))
	assert.Equal(t, 4.0, testutil.ToFloat64(observability.InstancesByQueue.WithLabelValues("default")))
	assert.Equal(t, 3.0, testutil.ToFloat64(observability.InstancesByQueue.WithLabelValues("dedicated")))
}
