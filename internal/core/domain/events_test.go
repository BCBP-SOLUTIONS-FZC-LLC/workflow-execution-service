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
		{EventWorkflowInstancePaused, "WorkflowInstancePaused"},
		{EventWorkflowInstanceResumed, "WorkflowInstanceResumed"},
		{EventWorkflowInstanceCancelled, "WorkflowInstanceCancelled"},
		{EventWorkflowInstanceTerminated, "WorkflowInstanceTerminated"},
		{EventWorkflowInstanceDegraded, "WorkflowInstanceDegraded"},
		{EventWorkflowInstanceFailed, "WorkflowInstanceFailed"},
		{EventWorkflowInstanceFinished, "WorkflowInstanceFinished"},
		{EventWorkflowTaskCreated, "WorkflowTaskCreated"},
		{EventWorkflowTaskClaimed, "WorkflowTaskClaimed"},
		{EventWorkflowTaskCompleted, "WorkflowTaskCompleted"},
		{EventWorkflowTaskDeferred, "WorkflowTaskDeferred"},
		{EventWorkflowTaskReassigned, "WorkflowTaskReassigned"},
		{EventWorkflowTaskSuperseded, "WorkflowTaskSuperseded"},
		{EventWorkflowTaskFailed, "WorkflowTaskFailed"},
		{EventWorkflowInstanceForceRouted, "WorkflowInstanceForceRouted"},
		{EventWorkflowTaskSLAWarning, "WorkflowTaskSlaWarning"},
		{EventWorkflowTaskSLABreached, "WorkflowTaskSlaBreached"},
		{"workflow.task.message-sent", "workflow.task.message-sent"}, // removed event: passes through unchanged
	}

	for _, tt := range tests {
		t.Run(tt.eventType, func(t *testing.T) {
			assert.Equal(t, tt.want, MessageName(tt.eventType))
		})
	}
}
