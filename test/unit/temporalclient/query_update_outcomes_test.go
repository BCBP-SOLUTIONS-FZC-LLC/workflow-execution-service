package temporalclient_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporalclient"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func queryFailingWith(sdkErr error) error {
	sdk := &fakeSDKClient{
		queryWorkflowFunc: func(context.Context, string, string, string, ...interface{}) (converter.EncodedValue, error) {
			return nil, sdkErr
		},
	}
	var out []string
	return temporalclient.New(sdk).QueryWorkflow(context.Background(), "tenant:biz", port.QueryWaitingExternal, &out)
}

func TestClient_QueryWorkflow_AnExecutionThatHasGoneIsReportedAsNotFound(t *testing.T) {
	err := queryFailingWith(serviceerror.NewNotFound("workflow execution already completed"))

	require.Error(t, err)
	assert.ErrorIs(t, err, port.ErrWorkflowNotFound)
	assert.NotErrorIs(t, err, port.ErrTemporalUnavailable)
}

func TestClient_QueryWorkflow_AFailureOfTheClusterIsMarkedUnavailable(t *testing.T) {
	cases := map[string]error{
		"unavailable":        serviceerror.NewUnavailable("frontend down"),
		"resource exhausted": serviceerror.NewResourceExhausted(0, "rate limited"),
		"transport":          errors.New("connection refused"),
		"deadline":           context.DeadlineExceeded,
	}
	for name, sdkErr := range cases {
		t.Run(name, func(t *testing.T) {
			err := queryFailingWith(sdkErr)

			require.Error(t, err)
			assert.ErrorIs(t, err, port.ErrTemporalUnavailable)
			assert.NotErrorIs(t, err, port.ErrWorkflowNotFound)
		})
	}
}

func TestClient_QueryWorkflow_ARefusalOfTheQueryItselfIsNeitherNotFoundNorUnavailable(t *testing.T) {
	err := queryFailingWith(serviceerror.NewQueryFailed("handler failed"))

	require.Error(t, err)
	assert.NotErrorIs(t, err, port.ErrWorkflowNotFound)
	assert.NotErrorIs(t, err, port.ErrTemporalUnavailable)
}

func updateFailingWith(callErr, handleErr error) error {
	sdk := &fakeSDKClient{updateCallErr: callErr, updateErr: handleErr}
	return temporalclient.New(sdk).UpdateWorkflow(context.Background(), "wf", uuid.New(), "stage-defer", struct{}{})
}

func TestClient_UpdateWorkflow_AFailureOfTheClusterIsMarkedUnavailableWhetherTheCallOrTheWaitFailed(t *testing.T) {
	cases := map[string]error{
		"unavailable":        serviceerror.NewUnavailable("frontend down"),
		"resource exhausted": serviceerror.NewResourceExhausted(0, "rate limited"),
		"transport":          errors.New("connection refused"),
	}
	for name, sdkErr := range cases {
		t.Run(name+" on the call", func(t *testing.T) {
			err := updateFailingWith(sdkErr, nil)

			require.Error(t, err)
			assert.ErrorIs(t, err, port.ErrTemporalUnavailable)
		})
		t.Run(name+" while waiting", func(t *testing.T) {
			err := updateFailingWith(nil, sdkErr)

			require.Error(t, err)
			assert.ErrorIs(t, err, port.ErrTemporalUnavailable)
		})
	}
}

func TestClient_UpdateWorkflow_AFailureInsideTheWorkflowIsNotMarkedUnavailable(t *testing.T) {
	err := updateFailingWith(nil, temporal.NewApplicationError("defer task: db down", "", nil))

	require.Error(t, err)
	assert.NotErrorIs(t, err, port.ErrTemporalUnavailable)
	assert.NotErrorIs(t, err, port.ErrUpdateRefused)
}

func TestClient_UpdateWorkflow_ARefusalByTheClusterForTheRequestIsNotMarkedUnavailable(t *testing.T) {
	err := updateFailingWith(serviceerror.NewInvalidArgument("bad update"), nil)

	require.Error(t, err)
	assert.NotErrorIs(t, err, port.ErrTemporalUnavailable)
}
