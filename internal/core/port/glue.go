package port

import "context"

// GlueCodec encodes an event payload before it is embedded in an outbound
// envelope. Implementations: the real adapter (internal/adapter/outbound/glue)
// backed by AWS Glue Schema Registry, and a no-op pass-through for dev/test.
//
// Encode's returned bytes are embedded directly as the envelope's JSON `data`
// field (internal/core/service.buildEnvelope) - implementations must always
// return valid JSON. See the real adapter's doc comment for why this rules
// out literally applying the AWS Glue wire-format binary header here.
type GlueCodec interface {
	Encode(ctx context.Context, schemaName string, payload []byte) ([]byte, error)
}
