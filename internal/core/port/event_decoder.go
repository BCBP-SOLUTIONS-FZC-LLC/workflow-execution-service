package port

import (
	"context"
	"encoding/json"
)

type EventDecoder interface {
	Decode(ctx context.Context, schemaID string, encoded []byte) (json.RawMessage, error)
}
