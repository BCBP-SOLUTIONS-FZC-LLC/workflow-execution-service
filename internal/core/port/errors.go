package port

import "errors"

var (
	ErrTaskNotFound          = errors.New("task not found")
	ErrNotAssignee           = errors.New("caller is not the task's active assignee")
	ErrRecordVersionConflict = errors.New("record version conflict")
	ErrInvalidTaskState      = errors.New("action not valid for task's current status")
	ErrUpdateRefused         = errors.New("the workflow refused the request")
	ErrTaskAlreadyClaimed    = errors.New("task already claimed by another assignee")
	ErrClaimNotApplicable    = errors.New("claim only applies to multi-assignee tasks")
	ErrNodeAlreadyResolved   = errors.New("node has already progressed")
	ErrOverrideNoOp          = errors.New("new assignee is already the current assignee")
	ErrNotAuthorizedForRead  = errors.New("caller is not authorized to read this resource")

	ErrTenantMismatch = errors.New("delegation_id belongs to a different tenant than tenant_id")

	ErrIdempotencyKeyReplay = errors.New("idempotency key replay with different payload")

	ErrInstanceNotFound        = errors.New("instance not found")
	ErrWorkflowNotFound        = errors.New("workflow execution not found")
	ErrTargetNodeNotFound      = errors.New("target node not found in the compiled plan")
	ErrDuplicateBusinessKey    = errors.New("business key already active for this subject")
	ErrTenantNotActive         = errors.New("the tenant's status does not allow starting an instance")
	ErrVersionNotPublished     = errors.New("workflow version is not published")
	ErrVersionInvalid          = errors.New("workflow version is not valid")
	ErrInstanceAlreadyTerminal = errors.New("instance is already in a terminal state")
	ErrInvalidInstanceState    = errors.New("action not valid for instance's current status")
	ErrForceBackNoSavedBranch  = errors.New("no completed step to go back to")
	ErrWaitNotFound            = errors.New("not an outside wait of the instance's plan")
	ErrNotWaiting              = errors.New("instance is not waiting there")
	ErrOverrideMapInvalid      = errors.New("override_map references an unknown node key or invalid value")

	ErrAssigneeIneligible = errors.New("the user does not satisfy the task's department and role level")

	ErrTaskNotHumanActionable = errors.New("task is connector-typed and has no human assignee to act on")

	ErrTaskNotConnectorTyped = errors.New("task is not connector-typed")

	ErrNoAutomationPrincipal = errors.New("tenant has no automation principal")
)

type AssigneeIneligibleError struct {
	Nodes []string
}

func (e *AssigneeIneligibleError) Error() string {
	return "the assignee of one or more nodes does not satisfy the node's eligibility requirement"
}

func (e *AssigneeIneligibleError) Unwrap() error {
	return ErrAssigneeIneligible
}
