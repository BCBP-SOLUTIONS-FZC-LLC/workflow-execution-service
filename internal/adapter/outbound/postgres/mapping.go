package postgres

import (
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres/db"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
)

func fromPgtypeText(t pgtype.Text) string {
	if !t.Valid {
		return ""
	}
	return t.String
}

func toNullableText(s string) pgtype.Text {
	if s == "" {
		return pgtype.Text{}
	}
	return pgtype.Text{String: s, Valid: true}
}

func fromPgtypeTimestamptz(t pgtype.Timestamptz) *time.Time {
	if !t.Valid {
		return nil
	}
	tt := t.Time
	return &tt
}

func toPgtypeTimestamptz(t *time.Time) pgtype.Timestamptz {
	if t == nil {
		return pgtype.Timestamptz{}
	}
	return pgtype.Timestamptz{Time: *t, Valid: true}
}

func fromPgtypeUUID(u pgtype.UUID) *uuid.UUID {
	if !u.Valid {
		return nil
	}
	id := uuid.UUID(u.Bytes)
	return &id
}

func toPgtypeUUID(id *uuid.UUID) pgtype.UUID {
	if id == nil {
		return pgtype.UUID{}
	}
	return pgtype.UUID{Bytes: *id, Valid: true}
}

func instanceFromDB(row db.WorkflowInstance) *domain.Instance {
	return &domain.Instance{
		ID:                 row.ID,
		TenantID:           row.TenantID,
		WorkflowID:         row.WorkflowID,
		WorkflowVersionID:  row.WorkflowVersionID,
		BusinessKey:        row.BusinessKey,
		TemporalWorkflowID: row.TemporalWorkflowID,
		TemporalRunID:      fromPgtypeText(row.TemporalRunID),
		Status:             domain.InstanceStatus(row.Status),
		CurrentNodeKeys:    row.CurrentNodeKeys,
		SavedNodeKeys:      row.SavedNodeKeys,
		ContextJSON:        row.ContextJson,
		OverrideMap:        row.OverrideMap,
		TaskQueue:          row.TaskQueue,
		StartedByUserID:    row.StartedByUserID,
		StartedAt:          fromPgtypeTimestamptz(row.StartedAt),
		CompletedAt:        fromPgtypeTimestamptz(row.CompletedAt),
		RecordVersion:      row.RecordVersion,
		CreatedAt:          row.CreatedAt,
		UpdatedAt:          row.UpdatedAt,
	}
}

func taskFromDB(row db.WorkflowTask) *domain.Task {
	return &domain.Task{
		ID:                 row.ID,
		TenantID:           row.TenantID,
		WorkflowInstanceID: row.WorkflowInstanceID,
		NodeKey:            row.NodeKey,
		DepartmentID:       row.DepartmentID,
		Status:             domain.TaskStatus(row.Status),
		RecordVersion:      row.RecordVersion,
		AssigneeMode:       row.AssigneeMode,
		ExtrasJSON:         row.ExtrasJson,
		DeferredFromTaskID: fromPgtypeUUID(row.DeferredFromTaskID),
		DueAt:              fromPgtypeTimestamptz(row.DueAt),
		FollowUpAt:         fromPgtypeTimestamptz(row.FollowUpAt),
		CreatedAt:          row.CreatedAt,
		UpdatedAt:          row.UpdatedAt,
		CompletedAt:        fromPgtypeTimestamptz(row.CompletedAt),
	}
}

func outboxEventRecordFromDB(row db.OutboxEvent) *domain.OutboxEventRecord {
	return &domain.OutboxEventRecord{
		ID:        row.ID,
		EventType: row.EventType,
		Payload:   row.Payload,
		CreatedAt: row.CreatedAt,
	}
}

func taskAssignmentFromDB(row db.WorkflowTaskAssignment) *domain.TaskAssignment {
	return &domain.TaskAssignment{
		ID:          row.ID,
		TenantID:    row.TenantID,
		TaskID:      row.TaskID,
		UserID:      row.UserID,
		AssignedBy:  fromPgtypeUUID(row.AssignedBy),
		Reason:      fromPgtypeText(row.Reason),
		IsLead:      row.IsLead,
		IsActive:    row.IsActive,
		AssignedAt:  fromPgtypeTimestamptz(row.AssignedAt),
		ClaimedAt:   fromPgtypeTimestamptz(row.ClaimedAt),
		CompletedAt: fromPgtypeTimestamptz(row.CompletedAt),
		ResultJSON:  row.ResultJson,
		VacatedAt:   fromPgtypeTimestamptz(row.VacatedAt),
		UpdatedAt:   row.UpdatedAt,
	}
}
