package glue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

var _ events.Codec = (*Codec)(nil)

const (
	glueHeaderVersion byte = 0x03
	glueNoCompression byte = 0x00
	glueHeaderSize         = 18
)

type schemaVersionGetter interface {
	GetSchemaVersion(ctx context.Context, params *glue.GetSchemaVersionInput, optFns ...func(*glue.Options)) (*glue.GetSchemaVersionOutput, error)
}

const staleRefreshBackoff = 30 * time.Second

type cacheEntry struct {
	versionID string
	expiresAt time.Time
	retryAt   time.Time
}

type Codec struct {
	client       schemaVersionGetter
	registryName string
	useStub      bool
	cache        map[string]cacheEntry
	flights      map[string]*schemaFlight
	mu           sync.Mutex
	cacheTTL     time.Duration
	MaxStaleness time.Duration
	logger       port.Logger
	now          func() time.Time
}

func (c *Codec) WithLogger(log port.Logger) *Codec {
	c.logger = log
	return c
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

func (c *Codec) Decode(_ context.Context, _ string, encoded []byte) (json.RawMessage, error) {
	if len(encoded) <= glueHeaderSize {
		return nil, fmt.Errorf("glue codec: encoded payload is %d bytes — no payload after the %d-byte Glue header", len(encoded), glueHeaderSize)
	}
	if encoded[0] != glueHeaderVersion {
		return nil, fmt.Errorf("glue codec: unexpected header version byte 0x%02x — want 0x%02x", encoded[0], glueHeaderVersion)
	}
	if encoded[1] != glueNoCompression {
		return nil, fmt.Errorf("glue codec: unsupported compression byte 0x%02x — want 0x%02x", encoded[1], glueNoCompression)
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

func registrySchemaName(eventType string) string {
	var b strings.Builder
	for i, r := range eventType {
		if unicode.IsUpper(r) {
			if i > 0 {
				b.WriteByte('_')
			}
			b.WriteRune(unicode.ToLower(r))
		} else {
			b.WriteRune(r)
		}
	}
	return b.String()
}

type schemaFlight struct {
	done               chan struct{}
	versionID          string
	stale              bool
	err                error
	leaderContextEnded bool
}

func (c *Codec) clock() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

func (c *Codec) getSchemaVersionID(ctx context.Context, schemaName string) (string, error) {
	versionID, stale, err := c.resolve(ctx, registrySchemaName(schemaName))
	if err == nil && stale {
		observability.IncCounter(observability.GlueSchemaStaleServedTotal)
	}
	return versionID, err
}

func (c *Codec) resolve(ctx context.Context, schemaName string) (versionID string, stale bool, err error) {
	for {
		c.mu.Lock()
		entry, known := c.cache[schemaName]
		if known && c.servable(entry) {
			c.mu.Unlock()
			return entry.versionID, !c.clock().Before(entry.expiresAt), nil
		}
		if flight, running := c.flights[schemaName]; running {
			c.mu.Unlock()
			versionID, retry, err := awaitFlight(ctx, flight)
			if retry {
				continue
			}
			return versionID, err == nil && flight.stale, err
		}
		flight := &schemaFlight{done: make(chan struct{})}
		if c.flights == nil {
			c.flights = make(map[string]*schemaFlight)
		}
		c.flights[schemaName] = flight
		c.mu.Unlock()

		c.lead(ctx, schemaName, flight, entry, known)
		return flight.versionID, flight.err == nil && flight.stale, flight.err
	}
}

func (c *Codec) lead(ctx context.Context, schemaName string, flight *schemaFlight, entry cacheEntry, known bool) {
	defer func() {
		c.mu.Lock()
		delete(c.flights, schemaName)
		c.mu.Unlock()
		close(flight.done)
	}()
	flight.err = errors.New("glue schema version lookup did not finish")
	flight.versionID, flight.stale, flight.err = c.refresh(ctx, schemaName, entry, known)
	flight.leaderContextEnded = ctx.Err() != nil
}

func awaitFlight(ctx context.Context, flight *schemaFlight) (versionID string, retry bool, err error) {
	select {
	case <-flight.done:
		if flight.err != nil && flight.leaderContextEnded && ctx.Err() == nil {
			return "", true, nil
		}
		return flight.versionID, false, flight.err
	case <-ctx.Done():
		return "", false, fmt.Errorf("%w: waiting for schema version id: %w", events.ErrRetryable, ctx.Err())
	}
}

func (c *Codec) servable(entry cacheEntry) bool {
	now := c.clock()
	return now.Before(entry.expiresAt) || (now.Before(entry.retryAt) && c.withinStaleness(entry))
}

func (c *Codec) refresh(ctx context.Context, schemaName string, stale cacheEntry, hadEntry bool) (versionID string, servedStale bool, err error) {
	versionID, err = c.lookup(ctx, schemaName)
	if err == nil {
		c.mu.Lock()
		c.cache[schemaName] = cacheEntry{versionID: versionID, expiresAt: c.clock().Add(c.cacheTTL)}
		c.mu.Unlock()
		return versionID, false, nil
	}
	if !hadEntry || ctx.Err() != nil || !errors.Is(err, events.ErrRetryable) || !c.withinStaleness(stale) {
		return "", false, err
	}
	c.serveStale(schemaName, stale, err)
	return stale.versionID, true, nil
}

func (c *Codec) withinStaleness(entry cacheEntry) bool {
	return c.MaxStaleness > 0 && c.clock().Sub(entry.expiresAt) <= c.MaxStaleness
}

func (c *Codec) serveStale(schemaName string, entry cacheEntry, cause error) {
	entry.retryAt = c.clock().Add(staleRefreshBackoff)
	c.mu.Lock()
	c.cache[schemaName] = entry
	c.mu.Unlock()
	if c.logger != nil {
		c.logger.Warn("glue codec: schema registry unreachable, serving the last known schema version id", map[string]any{
			"schema": schemaName, "version_id": entry.versionID, "error": cause.Error(),
			"stale_for": c.clock().Sub(entry.expiresAt).String(), "max_staleness": c.MaxStaleness.String(),
		})
	}
}

func (c *Codec) lookup(ctx context.Context, schemaName string) (string, error) {
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
		return "", markTransient(fmt.Errorf("glue GetSchemaVersion: %w", err))
	}
	if result.SchemaVersionId == nil {
		return "", errors.New("glue returned nil schema version id")
	}
	if _, err := uuid.Parse(*result.SchemaVersionId); err != nil {
		return "", fmt.Errorf("glue returned schema version id %q that is not a UUID: %w", *result.SchemaVersionId, err)
	}
	if result.Status != types.SchemaVersionStatusAvailable {
		return "", markUnavailable(schemaName, result.Status)
	}
	return *result.SchemaVersionId, nil
}

func markUnavailable(schemaName string, status types.SchemaVersionStatus) error {
	err := fmt.Errorf("glue schema %q latest version is %q, not AVAILABLE", schemaName, status)
	if status == types.SchemaVersionStatusPending {
		return fmt.Errorf("%w: %w", events.ErrRetryable, err)
	}
	return err
}

func markTransient(err error) error {
	if isPermanentRegistryError(err) {
		return err
	}
	return fmt.Errorf("%w: %w", events.ErrRetryable, err)
}

func isPermanentRegistryError(err error) bool {
	var notFound *types.EntityNotFoundException
	var denied *types.AccessDeniedException
	var invalid *types.InvalidInputException
	return errors.As(err, &notFound) || errors.As(err, &denied) || errors.As(err, &invalid)
}
