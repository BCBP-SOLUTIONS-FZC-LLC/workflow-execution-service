package port

import (
	"context"

	"github.com/google/uuid"
)

// Signal name constants, byte-for-byte mirroring internal/workflow/signals.go's
// own constants (SignalInstancePause etc.) — duplicated here, not imported,
// since internal/core/service may not depend on internal/workflow (arch-lint's
// peer-package rule, LLD §1.7). A future change to either copy without the
// other silently breaks signal delivery with no compile error; the
// testsuite.WorkflowTestSuite round-trip test in test/workflow/ exists
// specifically to catch that drift.
const (
	SignalInstancePause     = "instance-pause"
	SignalInstanceResume    = "instance-resume"
	SignalInstanceCancel    = "instance-cancel"
	SignalInstanceForceFwd  = "instance-force-forward"
	SignalInstanceForceBack = "instance-force-back"
	SignalInstanceReassign  = "instance-reassign"
)

// WorkflowTypeExecute is the Temporal workflow type name internal/workflow.Execute
// registers under (cmd/worker's RegisterWorkflowWithOptions call) — duplicated
// here rather than imported, since internal/adapter/outbound/temporalclient
// may not depend on internal/workflow either.
const WorkflowTypeExecute = "Execute"

// StartWorkflowInput is TemporalClient.StartWorkflow's input: the exact
// instantiation contract LLD §3.7 defines — the identifiers
// internal/workflow's own ExecuteInput needs, plus the two
// StartWorkflowOptions fields (TemporalWorkflowID, TaskQueue) that never
// cross into the workflow's own input argument.
type StartWorkflowInput struct {
	// TemporalWorkflowID is {tenantID}:{businessKey} — becomes
	// client.StartWorkflowOptions.ID, the identity StartWorkflow/
	// SignalWorkflow/TerminateWorkflow all route by.
	TemporalWorkflowID string
	TaskQueue          string
	TenantID           uuid.UUID
	InstanceID         uuid.UUID
	WorkflowVersionID  uuid.UUID
	ContextJSON        string
	// OverrideMap is node_key -> user_id, both already stringified — matches
	// internal/workflow.ExecuteInput.OverrideMap's own map[string]string shape
	// exactly, so the adapter can pass it straight through.
	OverrideMap map[string]string
}

type StartWorkflowOutput struct {
	TemporalWorkflowID string
	TemporalRunID      string
}

// TemporalClient is the API-process-side contract for driving a running
// workflow execution: start it, signal it, terminate it, query it. Its real
// implementation is internal/adapter/outbound/temporalclient — a peer of
// internal/adapter/outbound/temporal (the Worker-side Activity package);
// these two packages serve opposite processes despite what design/LLD/
// execution_service.md §1.7's prose currently says about outbound/temporal's
// role — see that package's own doc comment, which predates this port.
type TemporalClient interface {
	StartWorkflow(ctx context.Context, in StartWorkflowInput) (StartWorkflowOutput, error)

	// SignalWorkflow delivers payload on the channel internal/workflow's
	// interpreter actually listens on: signalName + ":" + instanceID
	// (internal/workflow/signals.go's own channel-naming convention).
	// temporalWorkflowID and instanceID are deliberately two separate
	// parameters, not the same value under two names: temporalWorkflowID
	// (Instance.TemporalWorkflowID, "{tenantID}:{businessKey}") is what
	// routes the SDK's SignalWorkflow call to the right execution;
	// instanceID (Instance.ID) is what the interpreter's own signal router
	// keys its channel name on. Conflating the two silently no-ops the
	// signal — Temporal buffers delivery to a channel name nothing is
	// listening on, with no error surfaced anywhere.
	SignalWorkflow(ctx context.Context, temporalWorkflowID string, instanceID uuid.UUID, signalName string, payload any) error

	// TerminateWorkflow is the one direct (non-signal) client call (LLD
	// §3.1) — the caller writes terminal DB state itself, before or around
	// this call; unlike every signal-forwarded method, this one carries no
	// record_version and isn't signal-validated.
	TerminateWorkflow(ctx context.Context, temporalWorkflowID, reason string) error

	// QueryWorkflow is internal reconciliation/test-tooling only (LLD
	// §5.6) — never HTTP-exposed. out must be a non-nil pointer; queryType
	// is "get-workflow-status" today (internal/workflow/query.go), the one
	// Query this schema defines.
	QueryWorkflow(ctx context.Context, temporalWorkflowID, queryType string, out any) error
}
