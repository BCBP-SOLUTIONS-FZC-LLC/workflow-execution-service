package port

import (
	"context"
	"encoding/json"
)

type EventValidator interface {
	Validate(ctx context.Context, eventType string, payload json.RawMessage) error
}
