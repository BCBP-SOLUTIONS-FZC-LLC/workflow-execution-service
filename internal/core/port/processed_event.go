package port

import (
	"context"
	"encoding/json"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"
)

const (
	ConsumerMembership = "membership-execution"
	ConsumerUser       = "user-execution"
	ConsumerConnector  = "connector-stream-execution"
)

var EventConsumers = []string{ConsumerMembership, ConsumerUser, ConsumerConnector}

type ProcessedEventRepository interface {
	// Process claims the event's id for the consumer and runs apply while the claim is held. A
	// delivery of an id already claimed, or being applied by another delivery, returns nil without
	// calling apply; a failed apply releases the claim so the delivery can be retried.
	Process(ctx context.Context, env events.Envelope[json.RawMessage], consumer string, apply func(ctx context.Context) error) error
	Prune(ctx context.Context, olderThan time.Duration) (int64, error)
}
