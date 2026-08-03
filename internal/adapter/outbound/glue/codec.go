// Package glue implements port.GlueCodec against AWS Glue Schema Registry.
package glue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var _ port.GlueCodec = (*Codec)(nil)

type schemaVersionGetter interface {
	GetSchemaVersion(ctx context.Context, params *glue.GetSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaVersionOutput, error)
}

type cacheEntry struct {
	versionID string
	expiresAt time.Time
}

// Codec resolves an event type's latest registered schema version in AWS
// Glue Schema Registry before allowing it to publish - a fail-closed check
// that a payload's schema actually exists in the registry.
//
// Deviation from definition_service/iam-user-profile's own GlueCodec: those
// implementations prepend the AWS Glue wire-format binary header (0x03 magic
// + 0x00 compression + 16-byte schema-version UUID) directly onto the
// returned bytes. That output is embedded straight into
// events.Envelope[json.RawMessage].Payload by buildEnvelope, and
// outbox.Enqueue's internal json.Marshal(env) errors the instant those bytes
// are non-JSON (confirmed via direct repro: "invalid character '\x03'
// looking for beginning of value") - a defect present, unfixed, and masked
// by AWS_USE_STUB=true in both reference repos today. Encode here performs
// the same registry lookup (so a missing/unregistered schema still fails
// closed) but always returns the payload unchanged, so the envelope stays
// valid JSON regardless of useStub. Real Glue wire-format framing for
// downstream Glue-aware consumers, if ever needed, has to be applied at
// actual SNS-publish time by a custom Publisher wrapper - platform-events'
// current Runner/Publisher has no such hook - which is out of this task's
// scope (see design/LLD/execution_service.md §6.8 deviation note).
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

func (c *Codec) Encode(ctx context.Context, schemaName string, payload []byte) ([]byte, error) {
	if c.useStub {
		return payload, nil
	}
	if _, err := c.getSchemaVersionID(ctx, schemaName); err != nil {
		return nil, fmt.Errorf("resolve schema version id: %w", err)
	}
	return payload, nil
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
