package temporalclient_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporalclient"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func insfixTerminateWith(sdkErr error) error {
	c := temporalclient.New(&fakeSDKClient{
		terminateWorkflowFunc: func(context.Context, string, string, string, ...interface{}) error { return sdkErr },
	})
	return c.TerminateWorkflow(context.Background(), "tenant:subject:key", "run-1", "reason")
}

func TestClient_TerminateWorkflow_AFailureAnotherAttemptCanCureIsMarkedUnavailable(t *testing.T) {
	for name, sdkErr := range map[string]error{
		"unavailable":           serviceerror.NewUnavailable("frontend down"),
		"namespace unavailable": serviceerror.NewNamespaceUnavailable("ns is moving"),
		"resource exhausted":    serviceerror.NewResourceExhausted(0, "too many requests"),
		"deadline exceeded":     serviceerror.NewDeadlineExceeded("took too long"),
		"plain transport error": errors.New("connection refused"),
	} {
		t.Run(name, func(t *testing.T) {
			err := insfixTerminateWith(sdkErr)

			require.ErrorIs(t, err, port.ErrTemporalUnavailable)
			assert.ErrorIs(t, err, sdkErr, "the cause stays reachable for logs")
		})
	}
}

func TestClient_TerminateWorkflow_ARefusalOfTheRequestIsNotMarkedUnavailable(t *testing.T) {
	for name, sdkErr := range map[string]error{
		"invalid argument":  serviceerror.NewInvalidArgument("bad run id"),
		"permission denied": serviceerror.NewPermissionDenied("no", "no"),
	} {
		t.Run(name, func(t *testing.T) {
			err := insfixTerminateWith(sdkErr)

			require.Error(t, err)
			assert.NotErrorIs(t, err, port.ErrTemporalUnavailable)
			assert.ErrorIs(t, err, sdkErr)
		})
	}
}

func TestClient_SignalWorkflow_ANamespaceThatIsUnavailableIsMarkedUnavailable(t *testing.T) {
	sdkErr := serviceerror.NewNamespaceUnavailable("ns is moving")
	c := temporalclient.New(&fakeSDKClient{
		signalWorkflowFunc: func(context.Context, string, string, string, interface{}) error { return sdkErr },
	})

	err := c.SignalWorkflow(context.Background(), "tenant:subject:key", uuid.New(), port.SignalInstancePause, nil)

	require.ErrorIs(t, err, port.ErrTemporalUnavailable)
}
