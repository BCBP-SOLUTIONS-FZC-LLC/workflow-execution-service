// Package port defines the narrow contract internal/workflow calls through
// workflow.ExecuteActivity: Activity name constants and their input/output
// shapes. It never defines Activity bodies — those are a separate,
// persistence-layer task (see .claude/CLAUDE.md's Clean Architecture rules).
//
// These shapes are a soft coordination point with whichever task lands
// internal/core/service in parallel: state them explicitly in review rather
// than treating them as an already-fixed contract.
package port

import (
	"encoding/json"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

// Activity names, matching what cmd/worker registers via worker.RegisterActivity
// (a separate sibling task). internal/workflow references these constants only
// — it never imports the activity implementations themselves.
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
)

type GetCompiledPlanInput struct {
	TenantID  string
	VersionID string
}

// GetCompiledPlanOutput's Collaboration is held for the instance's entire
// lifetime — never re-fetched (LLD §2.5).
type GetCompiledPlanOutput struct {
	Collaboration dsl.CompiledCollaboration
}

type CreateTaskInput struct {
	InstanceID   string
	TenantID     string
	NodeKey      domain.NodeKey
	CompiledNode json.RawMessage
	ContextJSON  string
	OverrideMap  map[string]string
}

type CreateTaskOutput struct {
	TaskID string
}

type UpdateInstanceNodesInput struct {
	InstanceID string
	TenantID   string
	NodeKeys   []domain.NodeKey
}

// ClaimAssignmentInput only applies to multi-assignee (assignee_mode='all')
// tasks; single-assignee tasks skip claim entirely (LLD §3.1).
type ClaimAssignmentInput struct {
	AssignmentID  string
	TenantID      string
	UserID        string
	RecordVersion int64
}

type CompleteAssignmentInput struct {
	AssignmentID  string
	TenantID      string
	ResultJSON    string
	RecordVersion int64
}

// CompleteAssignmentOutput.AllDone reports whether every assignment on the
// task has now completed.
type CompleteAssignmentOutput struct {
	AllDone bool
}

type DeferTaskInput struct {
	TaskID        string
	TenantID      string
	UserID        string
	AssignmentID  string
	Reason        string
	RecordVersion int64
}

type DeferTaskOutput struct {
	NewTaskID string
}

// UpdateInstanceStatusInput.CompletedAt is nil for a non-terminal status.
type UpdateInstanceStatusInput struct {
	InstanceID  string
	TenantID    string
	Status      domain.InstanceStatus
	CompletedAt *time.Time
}

// RecordForceRouteInput.OldNodeKeys must be captured before
// UpdateInstanceNodesActivity overwrites them (LLD §3.1).
type RecordForceRouteInput struct {
	InstanceID    string
	TenantID      string
	OldNodeKeys   []domain.NodeKey
	TargetNodeID  string
	AdminUserID   string
	RecordVersion int64
}

// RecordSLAWarningInput/RecordSLABreachInput are audit-only — neither
// changes instance/task status (LLD §3.4).
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

// PauseInstanceInput/ResumeInstanceInput share the same shape (LLD §3.1's
// admin lifecycle signals): a version-checked status update, writing the
// matching event on success.
type PauseInstanceInput struct {
	InstanceID    string
	TenantID      string
	AdminUserID   string
	RecordVersion int64
}

type ResumeInstanceInput struct {
	InstanceID    string
	TenantID      string
	AdminUserID   string
	RecordVersion int64
}

// CancelInstanceInput drives instance-cancel (LLD §3.1): marks active tasks
// FAILED and vacates their assignments, then updates status to TERMINATED,
// writing both event classes.
type CancelInstanceInput struct {
	InstanceID    string
	TenantID      string
	AdminUserID   string
	RecordVersion int64
}

// ReassignAssignmentInput drives instance-reassign (LLD §3.1): vacates the
// old assignment and inserts a new one.
type ReassignAssignmentInput struct {
	TaskID        string
	TenantID      string
	OldUserID     string
	NewUserID     string
	AdminUserID   string
	RecordVersion int64
}
