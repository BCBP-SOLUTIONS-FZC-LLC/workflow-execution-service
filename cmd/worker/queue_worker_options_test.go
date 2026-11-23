package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDefaultQueueWorkerOptions_StopTimeoutAndMetricsInterceptorWithTheSDKConcurrencyDefaults(t *testing.T) {
	assert.Equal(t, 25*time.Second, defaultQueueWorkerOptions.WorkerStopTimeout)
	assert.Len(t, defaultQueueWorkerOptions.Interceptors, 1, "the metrics interceptor")
	assert.Zero(t, defaultQueueWorkerOptions.MaxConcurrentActivityExecutionSize, "the SDK default applies")
	assert.Zero(t, defaultQueueWorkerOptions.MaxConcurrentWorkflowTaskExecutionSize, "the SDK default applies")
}

func TestDynamicQueueWorkerOptions_CapConcurrencyAtTwoHundredWhereTheDefaultQueueDoesNot(t *testing.T) {
	assert.Equal(t, 200, dynamicQueueWorkerOptions.MaxConcurrentActivityExecutionSize)
	assert.Equal(t, 200, dynamicQueueWorkerOptions.MaxConcurrentWorkflowTaskExecutionSize)
	assert.Equal(t, defaultQueueWorkerOptions.WorkerStopTimeout, dynamicQueueWorkerOptions.WorkerStopTimeout)
}
