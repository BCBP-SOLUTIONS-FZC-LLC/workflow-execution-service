package glue

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsglue "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegistrySchemaName_EdgeInputs_UnderscoreBeforeEveryCapitalExceptTheFirst(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		input string
		want  string
	}{
		{"pascal case", "WorkflowTaskCreated", "workflow_task_created"},
		{"already snake case is unchanged", "workflow_task_created", "workflow_task_created"},
		{"first letter gets no underscore", "Workflow", "workflow"},
		{"consecutive capitals each get an underscore", "ABC", "a_b_c"},
		{"capitals after a lower case word", "taskABCDone", "task_a_b_c_done"},
		{"empty input", "", ""},
		{"digit before a capital", "a1B", "a1_b"},
		{"digits alone are kept", "v2", "v2"},
		{"non-ASCII capital at the start", "Ünï", "ünï"},
		{"non-ASCII capital after a letter", "aÜb", "a_üb"},
		{"capital after a multi-byte letter", "éA", "é_a"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, registrySchemaName(tc.input))
		})
	}
}

func w30bStaticConfig() aws.Config {
	return aws.Config{
		Region: "us-east-1",
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "w30b", SecretAccessKey: "w30b"}, nil
		}),
		RetryMaxAttempts: 1,
	}
}

type w30bGlueRequest struct {
	target string
	body   string
}

type w30bGlueServer struct {
	mu       sync.Mutex
	requests []w30bGlueRequest
}

func (s *w30bGlueServer) handler(versionID string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		s.requests = append(s.requests, w30bGlueRequest{target: r.Header.Get("X-Amz-Target"), body: string(body)})
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		_ = json.NewEncoder(w).Encode(map[string]string{"SchemaVersionId": versionID, "Status": "AVAILABLE"})
	})
}

func (s *w30bGlueServer) seen() []w30bGlueRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]w30bGlueRequest(nil), s.requests...)
}

func TestNewCodec_StubMode_BuildsNoGlueClient(t *testing.T) {
	t.Parallel()

	c := NewCodec(w30bStaticConfig(), "w30b-registry", true, "http://127.0.0.1:1", 0)

	require.True(t, c.useStub)
	assert.Nil(t, c.client, "stub mode builds no Glue client, even when an endpoint is given")
	assert.Equal(t, 5*time.Minute, c.cacheTTL, "a zero cache TTL falls back to the default")
}

func TestNewCodec_NonStub_AppliesCustomEndpointOnlyWhenNotEmpty(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		endpoint string
		want     *string
	}{
		{"custom endpoint is the base endpoint", "http://127.0.0.1:4566", aws.String("http://127.0.0.1:4566")},
		{"empty endpoint leaves the default", "", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := NewCodec(w30bStaticConfig(), "w30b-registry", false, tc.endpoint, time.Minute)

			client, ok := c.client.(*awsglue.Client)
			require.True(t, ok, "a non-stub Codec holds a real Glue client")
			require.NotNil(t, client)
			assert.Equal(t, tc.want, client.Options().BaseEndpoint)
			assert.Equal(t, time.Minute, c.cacheTTL)
		})
	}
}

func TestNewCodec_NonStub_CustomEndpointReceivesTheSchemaLookup(t *testing.T) {
	t.Parallel()

	const versionID = "8fa88cde-824c-47bc-836b-665cd42c2222"
	recorder := &w30bGlueServer{}
	server := httptest.NewServer(recorder.handler(versionID))
	t.Cleanup(server.Close)
	c := NewCodec(w30bStaticConfig(), "w30b-registry", false, server.URL, time.Minute)

	_, schemaID, err := c.Encode(context.Background(), "WorkflowInstanceStarted", json.RawMessage(`{}`))

	require.NoError(t, err)
	assert.Equal(t, versionID, schemaID)
	requests := recorder.seen()
	require.Len(t, requests, 1)
	assert.Equal(t, "AWSGlue.GetSchemaVersion", requests[0].target)
	var sent struct {
		SchemaID struct {
			SchemaName   string
			RegistryName string
		} `json:"SchemaId"`
	}
	require.NoError(t, json.Unmarshal([]byte(requests[0].body), &sent))
	assert.Equal(t, "workflow_instance_started", sent.SchemaID.SchemaName)
	assert.Equal(t, "w30b-registry", sent.SchemaID.RegistryName)
}
