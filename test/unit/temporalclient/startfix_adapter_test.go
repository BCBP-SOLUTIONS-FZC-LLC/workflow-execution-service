package temporalclient_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

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
			assert.NotErrorIs(t, err, port.ErrExecutionHeld)
			assert.ErrorIs(t, err, sdkErr, "the cause stays reachable for logs")
		})
	}
}

// startfixSDK answers a start with "already started" and serves the history of the open execution.
type startfixSDK struct {
	*fakeSDKClient
	startedBy  string
	historyErr error
	events     []*historypb.HistoryEvent
	customised bool
	options    client.StartWorkflowOptions
	historyOf  string
}

type startfixHistory struct {
	events []*historypb.HistoryEvent
}

func (h *startfixHistory) HasNext() bool { return len(h.events) > 0 }
func (h *startfixHistory) Next() (*historypb.HistoryEvent, error) {
	e := h.events[0]
	h.events = h.events[1:]
	return e, nil
}

func (s *startfixSDK) ExecuteWorkflow(_ context.Context, options client.StartWorkflowOptions, _ interface{}, _ ...interface{}) (client.WorkflowRun, error) {
	s.options = options
	return nil, serviceerror.NewWorkflowExecutionAlreadyStarted("already running", "other-request", "run-7")
}

func (s *startfixSDK) GetWorkflowHistory(_ context.Context, workflowID, runID string, _ bool, filter enumspb.HistoryEventFilterType) client.HistoryEventIterator {
	s.historyOf = workflowID + "/" + runID
	if s.historyErr != nil {
		return &startfixErrHistory{err: s.historyErr}
	}
	if s.customised {
		return &startfixHistory{events: s.events}
	}
	payloads, err := converter.GetDefaultDataConverter().ToPayloads(map[string]string{"InstanceID": s.startedBy})
	if err != nil {
		panic(err)
	}
	return &startfixHistory{events: []*historypb.HistoryEvent{{
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{Input: payloads},
		},
	}}}
}

type startfixErrHistory struct{ err error }

func (h *startfixErrHistory) HasNext() bool                          { return true }
func (h *startfixErrHistory) Next() (*historypb.HistoryEvent, error) { return nil, h.err }

func TestClient_StartWorkflow_AnExecutionOfAnotherInstanceIsRefusedNotJoined(t *testing.T) {
	sdk := &startfixSDK{fakeSDKClient: &fakeSDKClient{}, startedBy: uuid.NewString()}

	out, err := temporalclient.New(sdk).StartWorkflow(context.Background(), port.StartWorkflowInput{
		TemporalWorkflowID: "tenant:a:b:c", TenantID: uuid.New(), InstanceID: uuid.New(), WorkflowVersionID: uuid.New(),
	})

	require.ErrorIs(t, err, port.ErrExecutionHeld)
	assert.NotErrorIs(t, err, port.ErrTemporalUnavailable)
	assert.Empty(t, out.TemporalRunID, "the refused start did not take the other run")
	assert.True(t, sdk.options.WorkflowExecutionErrorWhenAlreadyStarted, "the SDK must report an open execution instead of returning its run")
	assert.Equal(t, "tenant:a:b:c/run-7", sdk.historyOf, "the owner is read from the open run's own start")
}

func TestClient_StartWorkflow_ARepeatStartOfTheSameInstanceFindsItsOwnExecution(t *testing.T) {
	instanceID := uuid.New()
	sdk := &startfixSDK{fakeSDKClient: &fakeSDKClient{}, startedBy: instanceID.String()}

	out, err := temporalclient.New(sdk).StartWorkflow(context.Background(), port.StartWorkflowInput{
		TemporalWorkflowID: "tenant:subject:key", TenantID: uuid.New(), InstanceID: instanceID, WorkflowVersionID: uuid.New(),
	})

	require.NoError(t, err)
	assert.Equal(t, "run-7", out.TemporalRunID)
	assert.Equal(t, "tenant:subject:key", out.TemporalWorkflowID)
}

func TestClient_StartWorkflow_AnOwnerThatCannotBeReadIsAnUnavailableCluster(t *testing.T) {
	sdk := &startfixSDK{fakeSDKClient: &fakeSDKClient{}, historyErr: serviceerror.NewUnavailable("history down")}

	_, err := temporalclient.New(sdk).StartWorkflow(context.Background(), port.StartWorkflowInput{
		TemporalWorkflowID: "tenant:subject:key", TenantID: uuid.New(), InstanceID: uuid.New(), WorkflowVersionID: uuid.New(),
	})

	require.ErrorIs(t, err, port.ErrTemporalUnavailable)
	assert.NotErrorIs(t, err, port.ErrExecutionHeld)
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

func startfixStartedEvent(input []byte) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
			WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
				Input: &commonpb.Payloads{Payloads: []*commonpb.Payload{{
					Metadata: map[string][]byte{"encoding": []byte("json/plain")}, Data: input,
				}}},
			},
		},
	}
}

func TestClient_StartWorkflow_AnOpenExecutionWhoseStartCannotBeReadIsNotJoinedOrRefused(t *testing.T) {
	cases := map[string][]*historypb.HistoryEvent{
		"no history at all":                    nil,
		"a first event that is not the start":  {{EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED}},
		"an input that is not the start input": {startfixStartedEvent([]byte(`[1,2]`))},
	}
	for name, events := range cases {
		t.Run(name, func(t *testing.T) {
			sdk := &startfixSDK{fakeSDKClient: &fakeSDKClient{}, events: events, customised: true}

			out, err := temporalclient.New(sdk).StartWorkflow(context.Background(), port.StartWorkflowInput{
				TemporalWorkflowID: "tenant:subject:key", TenantID: uuid.New(), InstanceID: uuid.New(), WorkflowVersionID: uuid.New(),
			})

			require.ErrorIs(t, err, port.ErrTemporalUnavailable)
			assert.NotErrorIs(t, err, port.ErrExecutionHeld)
			assert.Empty(t, out.TemporalRunID)
		})
	}
}

func TestClient_StartWorkflow_AnOpenExecutionThatNamesNoInstanceIsNotTheCallers(t *testing.T) {
	sdk := &startfixSDK{fakeSDKClient: &fakeSDKClient{}, customised: true,
		events: []*historypb.HistoryEvent{startfixStartedEvent([]byte(`{"TenantID":"t"}`))}}

	out, err := temporalclient.New(sdk).StartWorkflow(context.Background(), port.StartWorkflowInput{
		TemporalWorkflowID: "tenant:subject:key", TenantID: uuid.New(), InstanceID: uuid.New(), WorkflowVersionID: uuid.New(),
	})

	require.ErrorIs(t, err, port.ErrExecutionHeld)
	assert.Empty(t, out.TemporalRunID)
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
