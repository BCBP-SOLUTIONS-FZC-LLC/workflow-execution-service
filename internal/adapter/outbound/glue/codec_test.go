package glue

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsglue "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeSchemaVersionGetter struct {
	versionID string
	err       error
}

func (f fakeSchemaVersionGetter) GetSchemaVersion(_ context.Context, _ *awsglue.GetSchemaVersionInput, _ ...func(*awsglue.Options)) (*awsglue.GetSchemaVersionOutput, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &awsglue.GetSchemaVersionOutput{SchemaVersionId: aws.String(f.versionID)}, nil
}

type nilSchemaVersionIDGetter struct{}

func (nilSchemaVersionIDGetter) GetSchemaVersion(_ context.Context, _ *awsglue.GetSchemaVersionInput, _ ...func(*awsglue.Options)) (*awsglue.GetSchemaVersionOutput, error) {
	return &awsglue.GetSchemaVersionOutput{SchemaVersionId: nil}, nil
}

func TestNewCodec_NonStub_ConstructsRealClientWithCustomEndpoint(t *testing.T) {
	c := NewCodec(aws.Config{}, "wf-workflow-events", false, "http://localhost:4566", 5*time.Minute)

	require.NotNil(t, c)
	assert.False(t, c.useStub)
	assert.NotNil(t, c.client, "a non-stub Codec must construct a real glue.Client")
}

func TestCodec_UseStub_PassesThroughWithoutRegistryCall(t *testing.T) {
	c := NewCodec(aws.Config{}, "wf-workflow-events", true, "", 0)

	payload := json.RawMessage(`{"workflow_instance_id":"b6e2b6f0-1c3e-4a2a-9c6d-2f0a7e9b5d11"}`)
	out, schemaID, err := c.Encode(context.Background(), "workflow.instance.started", payload)

	require.NoError(t, err)
	assert.Equal(t, []byte(payload), out)
	assert.Empty(t, schemaID)
}

// TestCodec_Encode_ProducesRealGlueWireFormat is the resolved-note regression
// test for what was previously a repo-specific deviation: with a real
// (non-stub) registry lookup succeeding, Encode must now prepend the AWS
// Glue wire-format binary header (0x03 magic + 0x00 compression + 16-byte
// schema-version UUID) and return the resolved version ID as schemaID.
// platform-events v1.4.0's SNS publisher base64-wraps these bytes before
// ever touching Envelope.Payload, so producing non-JSON bytes here is safe.
func TestCodec_Encode_ProducesRealGlueWireFormat(t *testing.T) {
	versionID := "8fa88cde-824c-47bc-836b-665cd42c2222"
	c := &Codec{
		client:       fakeSchemaVersionGetter{versionID: versionID},
		registryName: "wf-workflow-events",
		cache:        make(map[string]cacheEntry),
		cacheTTL:     0,
	}

	payload := json.RawMessage(`{"workflow_instance_id":"b6e2b6f0-1c3e-4a2a-9c6d-2f0a7e9b5d11"}`)
	out, schemaID, err := c.Encode(context.Background(), "workflow.instance.started", payload)
	require.NoError(t, err)

	require.Equal(t, versionID, schemaID)
	require.Len(t, out, glueHeaderSize+len(payload))
	assert.Equal(t, glueHeaderVersion, out[0], "output must start with the AWS Glue wire-format magic byte")
	assert.Equal(t, glueNoCompression, out[1])
	gotID, err := uuid.FromBytes(out[2:glueHeaderSize])
	require.NoError(t, err)
	assert.Equal(t, versionID, gotID.String(), "header must embed the resolved schema version UUID")
	assert.Equal(t, []byte(payload), out[glueHeaderSize:], "payload tail must be intact")
}

func TestCodec_EncodeDecode_RoundTrip(t *testing.T) {
	versionID := "8fa88cde-824c-47bc-836b-665cd42c2222"
	c := &Codec{
		client:       fakeSchemaVersionGetter{versionID: versionID},
		registryName: "wf-workflow-events",
		cache:        make(map[string]cacheEntry),
		cacheTTL:     0,
	}

	payload := json.RawMessage(`{"workflow_instance_id":"b6e2b6f0-1c3e-4a2a-9c6d-2f0a7e9b5d11"}`)
	encoded, schemaID, err := c.Encode(context.Background(), "workflow.instance.started", payload)
	require.NoError(t, err)

	decoded, err := c.Decode(context.Background(), schemaID, encoded)
	require.NoError(t, err)
	assert.JSONEq(t, string(payload), string(decoded))
}

func TestCodec_Decode_TooShort(t *testing.T) {
	c := NewCodec(aws.Config{}, "wf-workflow-events", true, "", 0)

	_, err := c.Decode(context.Background(), "any-schema-id", []byte("short"))
	require.Error(t, err)
}

func TestCodec_Decode_WrongMagicByte(t *testing.T) {
	c := NewCodec(aws.Config{}, "wf-workflow-events", true, "", 0)

	bad := make([]byte, glueHeaderSize+4)
	bad[0] = 0x01 // not glueHeaderVersion
	_, err := c.Decode(context.Background(), "any-schema-id", bad)
	require.Error(t, err)
}

func TestPrependGlueHeader_MalformedVersionID(t *testing.T) {
	_, err := prependGlueHeader("not-a-uuid", []byte(`{}`))
	require.Error(t, err)
}

// TestCodec_Encode_MalformedSchemaVersionIDFromRegistry_ReturnsError covers
// Encode's own prependGlueHeader error branch: the registry lookup itself
// succeeds, but returns something that isn't a parseable UUID.
func TestCodec_Encode_MalformedSchemaVersionIDFromRegistry_ReturnsError(t *testing.T) {
	c := &Codec{
		client:       fakeSchemaVersionGetter{versionID: "not-a-uuid"},
		registryName: "wf-workflow-events",
		cache:        make(map[string]cacheEntry),
		cacheTTL:     0,
	}

	_, _, err := c.Encode(context.Background(), "workflow.instance.started", []byte(`{}`))
	require.Error(t, err)
}

func TestCodec_GetSchemaVersionID_NilSchemaVersionID_ReturnsError(t *testing.T) {
	c := &Codec{
		client:       nilSchemaVersionIDGetter{},
		registryName: "wf-workflow-events",
		cache:        make(map[string]cacheEntry),
		cacheTTL:     0,
	}

	_, _, err := c.Encode(context.Background(), "workflow.instance.started", []byte(`{}`))
	require.Error(t, err)
}

// TestCodec_Encode_RequestsUnderscoredSchemaName is the regression test for
// this adapter's registry-name mismatch: platform-schemagov's register
// command has no name-override, and always registers schemas in Glue under
// the underscored JSON schema filename stem (e.g. "workflow_task_created"),
// never the dotted wire event type. Encode must convert before calling
// GetSchemaVersion, or every real (non-stub) publish 404s.
func TestCodec_Encode_RequestsUnderscoredSchemaName(t *testing.T) {
	getter := &nameCapturingGetter{versionID: "8fa88cde-824c-47bc-836b-665cd42c2222"}
	c := &Codec{
		client:       getter,
		registryName: "wf-workflow-events",
		cache:        make(map[string]cacheEntry),
		cacheTTL:     0,
	}

	_, _, err := c.Encode(context.Background(), "workflow.task.created", json.RawMessage(`{}`))

	require.NoError(t, err)
	require.NotNil(t, getter.gotSchemaName)
	assert.Equal(t, "workflow_task_created", *getter.gotSchemaName)
}

type nameCapturingGetter struct {
	versionID     string
	gotSchemaName *string
}

func (g *nameCapturingGetter) GetSchemaVersion(_ context.Context, params *awsglue.GetSchemaVersionInput, _ ...func(*awsglue.Options)) (*awsglue.GetSchemaVersionOutput, error) {
	g.gotSchemaName = params.SchemaId.SchemaName
	return &awsglue.GetSchemaVersionOutput{SchemaVersionId: aws.String(g.versionID)}, nil
}

func TestCodec_Encode_RegistryLookupError_FailsClosed(t *testing.T) {
	wantErr := errors.New("schema not found")
	c := &Codec{
		client:       fakeSchemaVersionGetter{err: wantErr},
		registryName: "wf-workflow-events",
		cache:        make(map[string]cacheEntry),
		cacheTTL:     0,
	}

	_, _, err := c.Encode(context.Background(), "workflow.instance.started", []byte(`{}`))

	require.Error(t, err)
	assert.ErrorIs(t, err, wantErr)
}

func TestCodec_Encode_CachesSchemaVersionID(t *testing.T) {
	calls := 0
	getter := &countingGetter{versionID: "8fa88cde-824c-47bc-836b-665cd42c2222", calls: &calls}
	c := &Codec{
		client:       getter,
		registryName: "wf-workflow-events",
		cache:        make(map[string]cacheEntry),
		cacheTTL:     0, // constructed directly, so no floor applied - use a real TTL below
	}
	c.cacheTTL = 5 * 60 * 1e9 // 5 minutes, expressed in time.Duration nanoseconds

	_, _, err := c.Encode(context.Background(), "workflow.instance.started", []byte(`{}`))
	require.NoError(t, err)
	_, _, err = c.Encode(context.Background(), "workflow.instance.started", []byte(`{}`))
	require.NoError(t, err)

	assert.Equal(t, 1, calls, "second Encode call for the same schema must hit the cache, not the registry again")
}

type countingGetter struct {
	versionID string
	calls     *int
}

func (g *countingGetter) GetSchemaVersion(_ context.Context, _ *awsglue.GetSchemaVersionInput, _ ...func(*awsglue.Options)) (*awsglue.GetSchemaVersionOutput, error) {
	*g.calls++
	return &awsglue.GetSchemaVersionOutput{SchemaVersionId: aws.String(g.versionID)}, nil
}
