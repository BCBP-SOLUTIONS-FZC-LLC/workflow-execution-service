package port

import (
	"context"
	"encoding/json"
)

// EventValidator validates an event payload against its registered JSON
// Schema before buildEnvelope embeds it in an outbound envelope
// (internal/core/service.buildEnvelope). Wire-format/schema-registry
// encoding (e.g. AWS Glue Schema Registry) is a separate concern, applied
// later at SNS-publish time via a platform-events events.Codec configured
// through events.WithCodec — not here.
type EventValidator interface {
	Validate(ctx context.Context, eventType string, payload json.RawMessage) error
}
