package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMessageName(t *testing.T) {
	tests := []struct {
		eventType string
		want      string
	}{
		{EventWorkflowInstanceStarted, "WorkflowInstanceStarted"},
		{EventWorkflowTaskSLAWarning, "WorkflowTaskSlaWarning"},
		{"AnyOtherType", "AnyOtherType"}, // unknown/removed event: passes through unchanged
	}

	for _, tt := range tests {
		t.Run(tt.eventType, func(t *testing.T) {
			assert.Equal(t, tt.want, MessageName(tt.eventType))
		})
	}
}
