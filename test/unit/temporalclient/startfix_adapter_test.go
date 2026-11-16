package temporalclient_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporalclient"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func startfixStartWith(t *testing.T, sdkErr error) (client.StartWorkflowOptions, error) {
	t.Helper()
	var got client.StartWorkflowOptions
	c := temporalclient.New(&fakeSDKClient{
		executeWorkflowFunc: func(_ context.Context, options client.StartWorkflowOptions, _ interface{}, _ ...interface{}) (client.WorkflowRun, error) {
			got = options
			return nil, sdkErr
		},
	})
	_, err := c.StartWorkflow(context.Background(), port.StartWorkflowInput{
		TemporalWorkflowID: "tenant:subject:key", TenantID: uuid.New(), InstanceID: uuid.New(), WorkflowVersionID: uuid.New(),
	})
	return got, err
}

func TestClient_StartWorkflow_AFailureOfTheClusterIsMarkedUnavailable(t *testing.T) {
	for name, sdkErr := range map[string]error{
		"unavailable":        serviceerror.NewUnavailable("frontend down"),
		"resource exhausted": serviceerror.NewResourceExhausted(0, "too many requests"),
		"plain error":        errors.New("connection refused"),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := startfixStartWith(t, sdkErr)

			require.ErrorIs(t, err, port.ErrTemporalUnavailable)
			assert.ErrorIs(t, err, sdkErr, "the cause stays reachable for logs")
		})
	}
}

func TestClient_SignalWorkflow_AFailureOtherThanMissingIsMarkedUnavailable(t *testing.T) {
	for name, sdkErr := range map[string]error{
		"unavailable":        serviceerror.NewUnavailable("frontend down"),
		"resource exhausted": serviceerror.NewResourceExhausted(0, "too many requests"),
	} {
		t.Run(name, func(t *testing.T) {
			c := temporalclient.New(&fakeSDKClient{
				signalWorkflowFunc: func(context.Context, string, string, string, interface{}) error { return sdkErr },
			})

			err := c.SignalWorkflow(context.Background(), "tenant:subject:key", uuid.New(), port.SignalInstancePause, struct{}{})

			require.ErrorIs(t, err, port.ErrTemporalUnavailable)
			assert.NotErrorIs(t, err, port.ErrWorkflowNotFound)
		})
	}
}

func TestClient_SignalWorkflow_AMissingExecutionIsNotUnavailable(t *testing.T) {
	c := temporalclient.New(&fakeSDKClient{
		signalWorkflowFunc: func(context.Context, string, string, string, interface{}) error {
			return serviceerror.NewNotFound("no such workflow")
		},
	})

	err := c.SignalWorkflow(context.Background(), "tenant:subject:key", uuid.New(), port.SignalInstancePause, struct{}{})

	require.ErrorIs(t, err, port.ErrWorkflowNotFound)
	assert.NotErrorIs(t, err, port.ErrTemporalUnavailable)
}

func startfixKinds() (retryable, permanent map[string]error) {
	retryable = map[string]error{
		"unavailable":        serviceerror.NewUnavailable("frontend down"),
		"deadline exceeded":  serviceerror.NewDeadlineExceeded("too slow"),
		"resource exhausted": serviceerror.NewResourceExhausted(0, "too many requests"),
		"aborted":            serviceerror.NewAborted("aborted"),
		"internal":           serviceerror.NewInternal("server fault"),
		"transport error":    errors.New("connection refused"),
		"caller deadline":    context.DeadlineExceeded,
	}
	permanent = map[string]error{
		"invalid argument":    serviceerror.NewInvalidArgument("bad id"),
		"permission denied":   serviceerror.NewPermissionDenied("no", "reason"),
		"namespace not found": serviceerror.NewNamespaceNotFound("ns"),
		"failed precondition": serviceerror.NewFailedPrecondition("not ready"),
		"unimplemented":       serviceerror.NewUnimplemented("no such call"),
	}
	return retryable, permanent
}

func TestClient_StartWorkflow_OnlyARetryableFailureIsMarkedUnavailable(t *testing.T) {
	retryable, permanent := startfixKinds()
	for name, sdkErr := range retryable {
		t.Run("retryable "+name, func(t *testing.T) {
			_, err := startfixStartWith(t, sdkErr)
			require.ErrorIs(t, err, port.ErrTemporalUnavailable)
		})
	}
	for name, sdkErr := range permanent {
		t.Run("permanent "+name, func(t *testing.T) {
			_, err := startfixStartWith(t, sdkErr)
			require.ErrorIs(t, err, sdkErr, "the cause stays reachable")
			assert.NotErrorIs(t, err, port.ErrTemporalUnavailable, "a failure a retry cannot cure is not a 503")
		})
	}
}

func TestClient_SignalWorkflow_OnlyARetryableFailureIsMarkedUnavailable(t *testing.T) {
	retryable, permanent := startfixKinds()
	signal := func(sdkErr error) error {
		c := temporalclient.New(&fakeSDKClient{
			signalWorkflowFunc: func(context.Context, string, string, string, interface{}) error { return sdkErr },
		})
		return c.SignalWorkflow(context.Background(), "tenant:subject:key", uuid.New(), port.SignalInstancePause, struct{}{})
	}
	for name, sdkErr := range retryable {
		t.Run("retryable "+name, func(t *testing.T) {
			require.ErrorIs(t, signal(sdkErr), port.ErrTemporalUnavailable)
		})
	}
	for name, sdkErr := range permanent {
		t.Run("permanent "+name, func(t *testing.T) {
			err := signal(sdkErr)
			require.ErrorIs(t, err, sdkErr)
			assert.NotErrorIs(t, err, port.ErrTemporalUnavailable)
			assert.NotErrorIs(t, err, port.ErrWorkflowNotFound)
		})
	}
}
