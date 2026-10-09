package domain

type InstanceStatus string

const (
	InstanceStatusRunning    InstanceStatus = "RUNNING"
	InstanceStatusPaused     InstanceStatus = "PAUSED"
	InstanceStatusCompleted  InstanceStatus = "COMPLETED"
	InstanceStatusTerminated InstanceStatus = "TERMINATED"
	InstanceStatusFailed     InstanceStatus = "FAILED"
	InstanceStatusDegraded   InstanceStatus = "DEGRADED"
)

type TaskStatus string

const (
	TaskStatusReady      TaskStatus = "READY"
	TaskStatusInProgress TaskStatus = "IN_PROGRESS"
	TaskStatusCompleted  TaskStatus = "COMPLETED"
	TaskStatusDeferred   TaskStatus = "DEFERRED"
	TaskStatusFailed     TaskStatus = "FAILED"
	TaskStatusSuperseded TaskStatus = "SUPERSEDED"
)

type AssignmentStatus string

const (
	AssignmentStatusCreated   AssignmentStatus = "CREATED"
	AssignmentStatusClaimed   AssignmentStatus = "CLAIMED"
	AssignmentStatusCompleted AssignmentStatus = "COMPLETED"
	AssignmentStatusVacated   AssignmentStatus = "VACATED"
)
