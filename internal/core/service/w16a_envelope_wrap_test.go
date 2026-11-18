package service

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
)

var errW16aSchemaRefused = errors.New("w16a: payload violates schema")

type w16aValidatorCall struct {
	eventType string
	payload   string
}

type w16aRecordingValidator struct {
	err   error
	calls []w16aValidatorCall
}

func (v *w16aRecordingValidator) Validate(_ context.Context, eventType string, payload json.RawMessage) error {
	v.calls = append(v.calls, w16aValidatorCall{eventType: eventType, payload: string(payload)})
	return v.err
}

type w16aUnmarshalablePayload struct {
	Ch chan int `json:"ch"`
}

type w16aEnvelopePayload struct {
	Name string `json:"name"`
}

func TestBuildEnvelope_AFailureNamesTheStepThatFailedAndKeepsItsCause(t *testing.T) {
	t.Run("a payload that cannot be marshalled is wrapped and never reaches the validator", func(t *testing.T) {
		validator := &w16aRecordingValidator{}

		env, err := BuildEnvelope(context.Background(), validator, domain.EventWorkflowInstanceStarted, "tenant-1", "", "", w16aUnmarshalablePayload{Ch: make(chan int)})

		require.Error(t, err)
		var unsupported *json.UnsupportedTypeError
		require.ErrorAs(t, err, &unsupported, "the marshal failure stays reachable through the wrapper")
		assert.True(t, strings.HasPrefix(err.Error(), "marshal event payload: "), "the wrapper names the marshal step")
		assert.Empty(t, validator.calls, "a payload that cannot be marshalled is not validated")
		assert.Empty(t, env.Type, "no envelope is returned with the error")
	})

	t.Run("a payload the validator refuses is wrapped, and the validator saw the event type and the marshalled payload", func(t *testing.T) {
		validator := &w16aRecordingValidator{err: errW16aSchemaRefused}

		env, err := BuildEnvelope(context.Background(), validator, domain.EventWorkflowInstanceStarted, "tenant-1", "", "", w16aEnvelopePayload{Name: "x"})

		require.ErrorIs(t, err, errW16aSchemaRefused)
		assert.Equal(t, "validate event payload: "+errW16aSchemaRefused.Error(), err.Error(), "the wrapper names the validate step and carries the cause")
		assert.Equal(t, []w16aValidatorCall{{eventType: domain.EventWorkflowInstanceStarted, payload: `{"name":"x"}`}}, validator.calls)
		assert.Empty(t, env.Type, "no envelope is returned with the error")
	})
}
