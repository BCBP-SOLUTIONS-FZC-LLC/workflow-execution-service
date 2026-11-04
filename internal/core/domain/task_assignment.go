package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

func DeterministicAssignmentID(taskID, userID uuid.UUID) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("execution_service:assignment:"+taskID.String()+"/"+userID.String()))
}

type TaskAssignment struct {
	ID          uuid.UUID
	TenantID    uuid.UUID
	TaskID      uuid.UUID
	UserID      uuid.UUID
	AssignedBy  *uuid.UUID
	Reason      string
	IsLead      bool
	IsActive    bool
	AssignedAt  *time.Time
	ClaimedAt   *time.Time
	CompletedAt *time.Time
	ResultJSON  json.RawMessage
	VacatedAt   *time.Time
	UpdatedAt   time.Time
}
