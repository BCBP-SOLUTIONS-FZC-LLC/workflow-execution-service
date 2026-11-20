package observability_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

func TestQueueClass_OnlyTheDefaultQueueIsDefault(t *testing.T) {
	cases := []struct {
		queue string
		want  string
	}{
		{"wf-queue-default", "default"},
		{"wf-queue-0192f4de-7b1c-7a3e-9a52-3f4f0b8d2c11", "dedicated"},
		{"anything-else", "dedicated"},
		{"", "dedicated"},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.want, observability.QueueClass(tc.queue, "wf-queue-default"), tc.queue)
	}
}

func TestInstancesByQueueClass_SumsEveryDedicatedQueueUnderOneLabel(t *testing.T) {
	byQueue := map[string]int64{
		"wf-queue-default": 4,
		"wf-queue-0192f4de-7b1c-7a3e-9a52-3f4f0b8d2c11": 3,
		"wf-queue-0192f4de-7b1c-7a3e-9a52-3f4f0b8d2c12": 2,
	}

	got := observability.InstancesByQueueClass(byQueue, "wf-queue-default")

	assert.Equal(t, map[string]int64{"default": 4, "dedicated": 5}, got)
}

func TestInstancesByQueueClass_ALabelWithNoInstancesIsAbsent(t *testing.T) {
	got := observability.InstancesByQueueClass(map[string]int64{"wf-queue-default": 1}, "wf-queue-default")

	assert.Equal(t, map[string]int64{"default": 1}, got)
	assert.Empty(t, observability.InstancesByQueueClass(nil, "wf-queue-default"))
}
