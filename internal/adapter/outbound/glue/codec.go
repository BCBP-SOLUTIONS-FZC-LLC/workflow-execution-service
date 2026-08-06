// Package glue implements events.Codec against AWS Glue Schema Registry.
package glue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
)

var _ events.Codec = (*Codec)(nil)

// glueHeaderVersion, glueNoCompression, and glueHeaderSize describe the AWS
// Glue Schema Registry wire-format header: a 1-byte format version, a 1-byte
// compression flag, and the 16-byte schema-version UUID, prepended to every
// encoded payload.
const (
	glueHeaderVersion byte = 0x03
	glueNoCompression byte = 0x00
	glueHeaderSize         = 18 // 1 (version) + 1 (compression) + 16 (schema-version UUID)
)

type schemaVersionGetter interface {
	GetSchemaVersion(ctx context.Context, params *glue.GetSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaVersionOutput, error)
}

type cacheEntry struct {
	versionID string
	expiresAt time.Time
}

// Codec resolves an event type's latest registered schema version in AWS
// Glue Schema Registry and prepends the registry's wire-format header to the
// payload before it publishes - a fail-closed check that a payload's schema
// actually exists in the registry, and real framing for downstream
// Glue-aware consumers.
//
// This type is meant to be injected via events.WithCodec(...) into a real
// events.NewSNSPublisher(...) call once that composition-root wiring exists
// in cmd/server (it doesn't yet) - platform-events' publisher base64-wraps
// Encode's returned bytes before assigning them to Envelope.Payload, so the
// envelope's "data" field stays valid JSON regardless of what Codec.Encode
// returns.
type Codec struct {
	client       schemaVersionGetter
	registryName string
	useStub      bool
	cache        map[string]cacheEntry
	mu           sync.RWMutex
	cacheTTL     time.Duration
}

func NewCodec(cfg aws.Config, registryName string, useStub bool, customEndpoint string, cacheTTL time.Duration) *Codec {
	var client *glue.Client
	if !useStub {
		client = glue.NewFromConfig(cfg, func(o *glue.Options) {
			if customEndpoint != "" {
				o.BaseEndpoint = aws.String(customEndpoint)
			}
		})
	}
	if cacheTTL <= 0 {
		cacheTTL = 5 * time.Minute
	}
	return &Codec{
		client:       client,
		registryName: registryName,
		useStub:      useStub,
		cache:        make(map[string]cacheEntry),
		cacheTTL:     cacheTTL,
	}
}

func (c *Codec) Encode(ctx context.Context, schemaName string, payload json.RawMessage) ([]byte, string, error) {
	if c.useStub {
		return payload, "", nil
	}
	versionID, err := c.getSchemaVersionID(ctx, schemaName)
	if err != nil {
		return nil, "", fmt.Errorf("resolve schema version id: %w", err)
	}
	encoded, err := prependGlueHeader(versionID, payload)
	if err != nil {
		return nil, "", err
	}
	return encoded, versionID, nil
}

// Decode reverses Encode: the version UUID travels self-contained in the
// header bytes, so the schemaID recorded on the envelope (mirrored here to
// satisfy events.Codec/port.EventDecoder) isn't needed to strip it back off.
func (c *Codec) Decode(_ context.Context, _ string, encoded []byte) (json.RawMessage, error) {
	if len(encoded) < glueHeaderSize {
		return nil, fmt.Errorf("glue codec: encoded payload is %d bytes — shorter than the %d-byte Glue header", len(encoded), glueHeaderSize)
	}
	if encoded[0] != glueHeaderVersion {
		return nil, fmt.Errorf("glue codec: unexpected header version byte 0x%02x — want 0x%02x", encoded[0], glueHeaderVersion)
	}
	return json.RawMessage(encoded[glueHeaderSize:]), nil
}

func prependGlueHeader(versionID string, payload []byte) ([]byte, error) {
	id, err := uuid.Parse(versionID)
	if err != nil {
		return nil, fmt.Errorf("parse schema version uuid %q: %w", versionID, err)
	}
	out := make([]byte, glueHeaderSize+len(payload))
	out[0] = glueHeaderVersion
	out[1] = glueNoCompression
	copy(out[2:glueHeaderSize], id[:])
	copy(out[glueHeaderSize:], payload)
	return out, nil
}

func (c *Codec) getSchemaVersionID(ctx context.Context, schemaName string) (string, error) {
	c.mu.RLock()
	entry, ok := c.cache[schemaName]
	c.mu.RUnlock()
	if ok && time.Now().Before(entry.expiresAt) {
		return entry.versionID, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok = c.cache[schemaName]
	if ok && time.Now().Before(entry.expiresAt) {
		return entry.versionID, nil
	}

	input := &glue.GetSchemaVersionInput{
		SchemaId: &types.SchemaId{
			SchemaName:   aws.String(schemaName),
			RegistryName: aws.String(c.registryName),
		},
		SchemaVersionNumber: &types.SchemaVersionNumber{
			LatestVersion: true,
		},
	}

	result, err := c.client.GetSchemaVersion(ctx, input)
	if err != nil {
		return "", fmt.Errorf("glue GetSchemaVersion: %w", err)
	}
	if result.SchemaVersionId == nil {
		return "", errors.New("glue returned nil schema version id")
	}

	versionID := *result.SchemaVersionId
	c.cache[schemaName] = cacheEntry{versionID: versionID, expiresAt: time.Now().Add(c.cacheTTL)}
	return versionID, nil
}
