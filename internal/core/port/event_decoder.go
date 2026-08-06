package port

import (
	"context"
	"encoding/json"
)

// EventDecoder reverses a platform-events events.Codec's wire-format
// encoding on an inbound event's payload, given the SchemaID recorded on
// its envelope. Mirrors events.Codec's Decode method so the same adapter
// (internal/adapter/outbound/glue.Codec) can satisfy both.
type EventDecoder interface {
	Decode(ctx context.Context, schemaID string, encoded []byte) (json.RawMessage, error)
}
