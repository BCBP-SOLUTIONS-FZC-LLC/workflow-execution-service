package glue

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsglue "github.com/aws/aws-sdk-go-v2/service/glue"
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

func TestCodec_UseStub_PassesThroughWithoutRegistryCall(t *testing.T) {
	c := NewCodec(aws.Config{}, "wf-workflow-events", true, "", 0)

	payload := []byte(`{"workflow_instance_id":"b6e2b6f0-1c3e-4a2a-9c6d-2f0a7e9b5d11"}`)
	out, err := c.Encode(context.Background(), "workflow.instance.started", payload)

	require.NoError(t, err)
	assert.Equal(t, payload, out)
}

// TestCodec_Encode_NeverProducesInvalidJSON is the regression test for this
// adapter's deviation from definition_service/iam-user-profile: even with a
// real (non-stub) registry lookup succeeding, Encode must never prepend the
// AWS Glue wire-format binary header, since that would make the returned
// bytes invalid JSON once embedded in events.Envelope[json.RawMessage] and
// break outbox.Enqueue's json.Marshal.
func TestCodec_Encode_NeverProducesInvalidJSON(t *testing.T) {
	c := &Codec{
		client:       fakeSchemaVersionGetter{versionID: "8fa88cde-824c-47bc-836b-665cd42c2222"},
		registryName: "wf-workflow-events",
		cache:        make(map[string]cacheEntry),
		cacheTTL:     0,
	}

	payload := []byte(`{"workflow_instance_id":"b6e2b6f0-1c3e-4a2a-9c6d-2f0a7e9b5d11"}`)
	out, err := c.Encode(context.Background(), "workflow.instance.started", payload)
	require.NoError(t, err)

	assert.Equal(t, payload, out, "Encode must return the payload byte-for-byte unchanged, never header-prefixed")
	assert.NotEqual(t, byte(0x03), out[0], "output must not start with the AWS Glue wire-format magic byte")

	type envelope struct {
		Data json.RawMessage `json:"data"`
	}
	env := envelope{Data: json.RawMessage(out)}
	marshaled, err := json.Marshal(env)
	require.NoError(t, err, "embedding Encode's output as json.RawMessage must never break json.Marshal")

	var roundTrip envelope
	require.NoError(t, json.Unmarshal(marshaled, &roundTrip))
	assert.JSONEq(t, string(payload), string(roundTrip.Data))
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

	_, err := c.Encode(context.Background(), "workflow.task.created", []byte(`{}`))

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

	_, err := c.Encode(context.Background(), "workflow.instance.started", []byte(`{}`))

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

	_, err := c.Encode(context.Background(), "workflow.instance.started", []byte(`{}`))
	require.NoError(t, err)
	_, err = c.Encode(context.Background(), "workflow.instance.started", []byte(`{}`))
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
