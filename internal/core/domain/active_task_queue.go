package domain

import (
	"time"

	"github.com/google/uuid"
)

type ActiveTaskQueue struct {
	ID           uuid.UUID
	TenantID     uuid.UUID
	QueueName    string
	Routable     bool
	RegisteredAt time.Time
	UpdatedAt    time.Time
}

func TenantTaskQueueName(tenantID uuid.UUID) string {
	return "wf-queue-" + tenantID.String()
}

// FeatureDedicatedWorkflowQueue is the Commercial catalog grant that gives a tenant its own task queue.
const FeatureDedicatedWorkflowQueue = "dedicated_workflow_queue"
