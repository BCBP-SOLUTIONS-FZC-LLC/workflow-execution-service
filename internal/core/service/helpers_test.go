package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
)

type stubPayload struct {
	WorkflowInstanceID uuid.UUID `json:"workflow_instance_id"`
}

type erroringValidator struct{ err error }

func (v erroringValidator) Validate(_ context.Context, _ string, _ json.RawMessage) error {
	return v.err
}

func TestBuildEnvelope_SetsCoreFields(t *testing.T) {
	instID := uuid.New()
	payload := stubPayload{WorkflowInstanceID: instID}

	env, err := buildEnvelope(context.Background(), noopValidator{}, domain.EventWorkflowInstanceStarted, "tenant-1", "instances/"+instID.String(), "user-1", payload)
	require.NoError(t, err)

	assert.Equal(t, domain.EventWorkflowInstanceStarted, env.Type)
	assert.Equal(t, domain.EventSource, env.Source)
	assert.Equal(t, "tenant-1", env.TenantID)
	assert.Equal(t, "1", env.SchemaVersion)
	assert.Equal(t, "instances/"+instID.String(), env.Subject)
	assert.Equal(t, "user-1", env.Actor)
	assert.Empty(t, env.TraceID)
}

func TestBuildEnvelope_OmitsEmptySubjectAndActor(t *testing.T) {
	env, err := buildEnvelope(context.Background(), noopValidator{}, domain.EventWorkflowInstanceStarted, "tenant-1", "", "", stubPayload{})
	require.NoError(t, err)

	assert.Empty(t, env.Subject)
	assert.Empty(t, env.Actor)
}

func TestBuildEnvelope_SetsTraceIDWhenSpanValid(t *testing.T) {
	traceID, err := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	require.NoError(t, err)
	spanID, err := trace.SpanIDFromHex("00f067aa0ba902b7")
	require.NoError(t, err)
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	env, err := buildEnvelope(ctx, noopValidator{}, domain.EventWorkflowInstanceStarted, "tenant-1", "", "", stubPayload{})
	require.NoError(t, err)

	assert.Equal(t, traceID.String(), env.TraceID)
}

// unmarshalablePayload has a chan field, which encoding/json can never
// marshal - used to exercise buildEnvelope's own json.Marshal error branch,
// distinct from a validator rejection.
type unmarshalablePayload struct {
	Ch chan int `json:"ch"`
}

func TestBuildEnvelope_MarshalError(t *testing.T) {
	_, err := buildEnvelope(context.Background(), noopValidator{}, domain.EventWorkflowInstanceStarted, "tenant-1", "", "", unmarshalablePayload{Ch: make(chan int)})

	require.Error(t, err)
}

func TestBuildEnvelope_ValidationError(t *testing.T) {
	wantErr := errors.New("payload violates schema")
	_, err := buildEnvelope(context.Background(), erroringValidator{err: wantErr}, domain.EventWorkflowInstanceStarted, "tenant-1", "", "", stubPayload{})

	require.Error(t, err)
	assert.ErrorIs(t, err, wantErr)
}

// TestBuildEnvelope_NestsPayloadUnderDataKey guards LLD §4.5/§4.9's JSONB
// expression indexes (payload -> 'data' ->> 'workflow_instance_id'): don't
// trust events.NewEnvelope's nesting implicitly, assert it directly.
func TestBuildEnvelope_NestsPayloadUnderDataKey(t *testing.T) {
	instID := uuid.New()
	env, err := buildEnvelope(context.Background(), noopValidator{}, domain.EventWorkflowInstanceStarted, "tenant-1", "", "", stubPayload{WorkflowInstanceID: instID})
	require.NoError(t, err)

	marshaled, err := json.Marshal(env)
	require.NoError(t, err)

	var asMap map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(marshaled, &asMap))
	require.Contains(t, asMap, "data")

	var data map[string]any
	require.NoError(t, json.Unmarshal(asMap["data"], &data))
	assert.Equal(t, instID.String(), data["workflow_instance_id"])
}

func TestNoopValidator_AlwaysAccepts(t *testing.T) {
	err := noopValidator{}.Validate(context.Background(), "any-schema", json.RawMessage(`{"a":1}`))
	require.NoError(t, err)
}
