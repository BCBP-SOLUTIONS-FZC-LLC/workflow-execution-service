package port

import (
	"context"
	"time"

	"github.com/google/uuid"
)

const (
	ConsumerMembership = "membership-execution"
	ConsumerUser       = "user-execution"
	ConsumerConnector  = "connector-stream-execution"
)

var EventConsumers = []string{ConsumerMembership, ConsumerUser, ConsumerConnector}

type ProcessedEventRepository interface {
	IsProcessed(ctx context.Context, eventID uuid.UUID, consumer string) (bool, error)
	MarkProcessed(ctx context.Context, eventID uuid.UUID, consumer string) error
	Prune(ctx context.Context, olderThan time.Duration) (int64, error)
}
