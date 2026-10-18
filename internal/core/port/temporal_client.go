package port

import (
	"context"

	"github.com/google/uuid"
)

const (
	SignalInstancePause     = "instance-pause"
	SignalInstanceResume    = "instance-resume"
	SignalInstanceCancel    = "instance-cancel"
	SignalInstanceForceFwd  = "instance-force-forward"
	SignalInstanceForceBack = "instance-force-back"
	SignalInstanceReassign  = "instance-reassign"

	SignalInstanceExternalAnswer = "instance-external-answer"
)

const QueryWaitingExternal = "waiting-external"

const WorkflowTypeExecute = "Execute"

type StartWorkflowInput struct {
	TemporalWorkflowID string
	TaskQueue          string
	TenantID           uuid.UUID
	InstanceID         uuid.UUID
	WorkflowVersionID  uuid.UUID
	ContextJSON        string
	OverrideMap        map[string]string
}

type StartWorkflowOutput struct {
	TemporalWorkflowID string
	TemporalRunID      string
}

type TemporalClient interface {
	StartWorkflow(ctx context.Context, in StartWorkflowInput) (StartWorkflowOutput, error)

	SignalWorkflow(ctx context.Context, temporalWorkflowID string, instanceID uuid.UUID, signalName string, payload any) error

	TerminateWorkflow(ctx context.Context, temporalWorkflowID, runID, reason string) error

	QueryWorkflow(ctx context.Context, temporalWorkflowID, queryType string, out any) error
}
