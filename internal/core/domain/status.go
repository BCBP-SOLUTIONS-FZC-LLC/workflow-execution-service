// Package domain holds sentinel types shared across core/port, core/service,
// and internal/workflow. It has no dependencies beyond the standard library.
package domain

// InstanceStatus is the lifecycle status of a workflow instance.
// See design/LLD/execution_service.md §3.4 for the full state machine.
type InstanceStatus string

const (
	InstanceStatusRunning    InstanceStatus = "RUNNING"
	InstanceStatusPaused     InstanceStatus = "PAUSED"
	InstanceStatusCompleted  InstanceStatus = "COMPLETED"
	InstanceStatusTerminated InstanceStatus = "TERMINATED"
	InstanceStatusFailed     InstanceStatus = "FAILED"
	InstanceStatusDegraded   InstanceStatus = "DEGRADED"
)

// TaskStatus is the lifecycle status of a workflow_task row.
//
// There is no independent per-task failure mode: TaskStatusFailed means "swept
// up in the parent instance's terminal transition," never "this task failed on
// its own" (LLD §3.4).
type TaskStatus string

const (
	TaskStatusReady      TaskStatus = "READY"
	TaskStatusInProgress TaskStatus = "IN_PROGRESS"
	TaskStatusCompleted  TaskStatus = "COMPLETED"
	TaskStatusDeferred   TaskStatus = "DEFERRED"
	TaskStatusFailed     TaskStatus = "FAILED"
	TaskStatusSuperseded TaskStatus = "SUPERSEDED"
)

// AssignmentStatus is the lifecycle status of a workflow_task_assignment row.
//
// Completed means the assignee acted (including choosing to defer); Vacated
// means something external acted instead (LLD §3.4).
type AssignmentStatus string

const (
	AssignmentStatusCreated   AssignmentStatus = "CREATED"
	AssignmentStatusClaimed   AssignmentStatus = "CLAIMED"
	AssignmentStatusCompleted AssignmentStatus = "COMPLETED"
	AssignmentStatusVacated   AssignmentStatus = "VACATED"
)
