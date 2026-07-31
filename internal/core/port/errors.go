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
)
