package port

import (
	"encoding/json"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

const (
	ActivityGetCompiledPlan      = "GetCompiledPlanActivity"
	ActivityCreateTask           = "CreateTaskActivity"
	ActivityUpdateInstanceNodes  = "UpdateInstanceNodesActivity"
	ActivityClaimAssignment      = "ClaimAssignmentActivity"
	ActivityCompleteAssignment   = "CompleteAssignmentActivity"
	ActivityDeferTask            = "DeferTaskActivity"
	ActivityUpdateInstanceStatus = "UpdateInstanceStatusActivity"
	ActivityRecordForceRoute     = "RecordForceRouteActivity"
	ActivityRecordSLAWarning     = "RecordSLAWarningActivity"
	ActivityRecordSLABreach      = "RecordSLABreachActivity"
	ActivityPauseInstance        = "PauseInstanceActivity"
	ActivityResumeInstance       = "ResumeInstanceActivity"
	ActivityCancelInstance       = "CancelInstanceActivity"
	ActivityReassignAssignment   = "ReassignAssignmentActivity"
	ActivityUpdateTaskStatus     = "UpdateTaskStatusActivity"
)

type GetCompiledPlanInput struct {
	TenantID  string
	VersionID string
}

type GetCompiledPlanOutput struct {
	Collaboration dsl.CompiledCollaboration
}

type CreateTaskInput struct {
	InstanceID      string
	TenantID        string
	NodeKey         domain.NodeKey
	CompiledNode    json.RawMessage
	ContextJSON     string
	OverrideMap     map[string]string
	VisitCount      int64
	IAMDepartmentID string
}

type CreateTaskOutput struct {
	TaskID string
}

type UpdateInstanceNodesInput struct {
	InstanceID string
	TenantID   string
	NodeKeys   []domain.NodeKey
}

type ClaimAssignmentInput struct {
	AssignmentID  string
	TenantID      string
	UserID        string
	RecordVersion int64
}

type CompleteAssignmentInput struct {
	TaskID        string
	UserID        string
	TenantID      string
	ResultJSON    string
	RecordVersion int64
}

type CompleteAssignmentOutput struct {
	AllDone bool
}

type DeferTaskInput struct {
	TaskID            string
	TenantID          string
	UserID            string
	AssignmentID      string
	Reason            string
	DeferredToNodeKey string
	RecordVersion     int64
}

type UpdateInstanceStatusInput struct {
	InstanceID     string
	TenantID       string
	Status         domain.InstanceStatus
	CompletedAt    *time.Time
	FailedBranches []domain.FailedBranch
}

type RecordForceRouteInput struct {
	InstanceID    string
	TenantID      string
	OldNodeKeys   []domain.NodeKey
	TargetNodeID  string
	Direction     string
	AdminUserID   string
	RecordVersion int64
}

type RecordSLAWarningInput struct {
	InstanceID string
	TenantID   string
	TaskID     string
	NodeKey    domain.NodeKey
}

type RecordSLABreachInput struct {
	InstanceID string
	TenantID   string
	TaskID     string
	NodeKey    domain.NodeKey
}

type PauseInstanceInput struct {
	InstanceID    string
	TenantID      string
	AdminUserID   string
	Initiator     string
	RecordVersion int64
}

type ResumeInstanceInput struct {
	InstanceID    string
	TenantID      string
	AdminUserID   string
	Initiator     string
	RecordVersion int64
}

type CancelInstanceInput struct {
	InstanceID    string
	TenantID      string
	AdminUserID   string
	Reason        *string
	RecordVersion int64
}

type ReassignAssignmentInput struct {
	TaskID        string
	TenantID      string
	OldUserID     string
	NewUserID     string
	AdminUserID   string
	RecordVersion int64
}

type UpdateTaskStatusInput struct {
	TaskID        string
	TenantID      string
	Status        domain.TaskStatus
	RecordVersion int64
}
