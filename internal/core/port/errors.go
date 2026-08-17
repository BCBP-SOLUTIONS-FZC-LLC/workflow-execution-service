package port

import "errors"

// Sentinel errors TaskService methods return, mapped to HTTP status/codes by
// the handler layer (LLD §5.10).
var (
	ErrTaskNotFound          = errors.New("task not found")
	ErrNotAssignee           = errors.New("caller is not the task's active assignee")
	ErrRecordVersionConflict = errors.New("record version conflict")
	ErrInvalidTaskState      = errors.New("action not valid for task's current status")
	ErrTaskAlreadyClaimed    = errors.New("task already claimed by another assignee")
	ErrClaimNotApplicable    = errors.New("claim only applies to multi-assignee tasks")
	ErrNodeAlreadyResolved   = errors.New("node has already progressed")
	ErrOverrideNoOp          = errors.New("new assignee is already the current assignee")
	ErrNotAuthorizedForRead  = errors.New("caller is not authorized to read this resource")

	// ErrTenantMismatch is the port.WorkflowClient family's own sentinel
	// (LLD §5.7/§5.8): a supplied delegation_id belongs to a different
	// tenant than the request's own tenant_id.
	ErrTenantMismatch = errors.New("delegation_id belongs to a different tenant than tenant_id")

	// ErrIdempotencyKeyReplay signals an Idempotency-Key reused with a
	// different request body (LLD §5.9).
	ErrIdempotencyKeyReplay = errors.New("idempotency key replay with different payload")

	// The sentinels below are InstanceService's own failure modes (LLD §5.10).
	ErrInstanceNotFound        = errors.New("instance not found")
	ErrTargetNodeNotFound      = errors.New("target node not found in the compiled plan")
	ErrDuplicateBusinessKey    = errors.New("business key already active for this tenant")
	ErrTenantNotActive         = errors.New("tenant is not currently active or trial")
	ErrVersionNotPublished     = errors.New("workflow version is not published")
	ErrVersionInvalid          = errors.New("workflow version is not valid")
	ErrInstanceAlreadyTerminal = errors.New("instance is already in a terminal state")
	ErrInvalidInstanceState    = errors.New("action not valid for instance's current status")
	ErrForceBackNoSavedBranch  = errors.New("no saved branch to restore")
	ErrOverrideMapInvalid      = errors.New("override_map references an unknown node key or invalid value")

	// ErrAssigneeIneligible is Start's own §5.5 bulk eligibility re-check
	// failure; maps to the existing CodeAssigneeIneligible.
	ErrAssigneeIneligible = errors.New("one or more default assignees no longer satisfy their node's eligibility requirement")

	// ErrTaskNotHumanActionable rejects a human-action method (Claim/
	// Complete/Defer/Reassign/OverrideAssignee) called against a
	// connector-typed task — one with zero assignments, ever, whose only
	// legitimate completion path is the stage-transition/stage-fail signal
	// path cmd/connector-worker will eventually use.
	ErrTaskNotHumanActionable = errors.New("task is connector-typed and has no human assignee to act on")

	// ErrAssigneeUnavailable closes LLD Appendix B's own documented gap
	// ("Task actions never re-verify the assignee's live OOO/delegation/
	// deleted status at the moment of the action") — a live IAM status
	// check confirming the assignee is deleted or currently OOO, not merely
	// an inability to check (which fails open, see IAMClient's own doc
	// comment).
	ErrAssigneeUnavailable = errors.New("assignee is no longer available (deleted or out-of-office)")
)

// AssigneeIneligibleError is ErrAssigneeIneligible's node-carrying form (LLD
// §5.5 step 3: "naming every offending node in one payload so the caller
// resolves all of them before a single retry"). errors.Is(err,
// ErrAssigneeIneligible) still matches, via Unwrap.
type AssigneeIneligibleError struct {
	Nodes []string
}

func (e *AssigneeIneligibleError) Error() string {
	return ErrAssigneeIneligible.Error()
}

func (e *AssigneeIneligibleError) Unwrap() error {
	return ErrAssigneeIneligible
}
