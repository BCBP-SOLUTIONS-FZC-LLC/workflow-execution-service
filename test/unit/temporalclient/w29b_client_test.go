package temporalclient_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporalclient"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// w29bSDK records every argument the adapter hands to the SDK, which the shared fake ignores for updates.
type w29bSDK struct {
	client.Client

	executeArgs   [][]any
	updateOpts    []client.UpdateWorkflowOptions
	updateCallErr error
	handleErr     error
	handleGets    int
	signals       []w29bSignal
	signalErr     error
	terminates    []w29bTerminate
	terminateErr  error
	queries       []w29bQuery
	queryErr      error
}

type w29bSignal struct{ workflowID, runID, name string }

type w29bTerminate struct {
	workflowID, runID, reason string
	details                   []any
}

type w29bQuery struct {
	workflowID, runID, queryType string
	args                         []any
}

type w29bHandle struct {
	client.WorkflowUpdateHandle
	sdk *w29bSDK
}

func (h *w29bHandle) Get(context.Context, any) error {
	h.sdk.handleGets++
	return h.sdk.handleErr
}

func (s *w29bSDK) ExecuteWorkflow(_ context.Context, _ client.StartWorkflowOptions, _ any, args ...any) (client.WorkflowRun, error) {
	s.executeArgs = append(s.executeArgs, args)
	return &fakeWorkflowRun{id: "wf", runID: "run"}, nil
}

func (s *w29bSDK) UpdateWorkflow(_ context.Context, opts client.UpdateWorkflowOptions) (client.WorkflowUpdateHandle, error) {
	s.updateOpts = append(s.updateOpts, opts)
	if s.updateCallErr != nil {
		return nil, s.updateCallErr
	}
	return &w29bHandle{sdk: s}, nil
}

func (s *w29bSDK) SignalWorkflow(_ context.Context, workflowID, runID, name string, _ any) error {
	s.signals = append(s.signals, w29bSignal{workflowID, runID, name})
	return s.signalErr
}

func (s *w29bSDK) TerminateWorkflow(_ context.Context, workflowID, runID, reason string, details ...any) error {
	s.terminates = append(s.terminates, w29bTerminate{workflowID, runID, reason, details})
	return s.terminateErr
}

func (s *w29bSDK) QueryWorkflow(_ context.Context, workflowID, runID, queryType string, args ...any) (converter.EncodedValue, error) {
	s.queries = append(s.queries, w29bQuery{workflowID, runID, queryType, args})
	return nil, s.queryErr
}

func TestClient_SignalWorkflow_NotFoundKeepsTheSDKErrorAndNamesTheChannel(t *testing.T) {
	instanceID := uuid.New()
	sdkErr := serviceerror.NewNotFound("workflow execution already completed")
	sdk := &w29bSDK{signalErr: sdkErr}

	err := temporalclient.New(sdk).SignalWorkflow(context.Background(), "tenant:biz", instanceID, port.SignalInstancePause, nil)

	require.ErrorIs(t, err, port.ErrWorkflowNotFound)
	assert.ErrorIs(t, err, sdkErr, "the original error stays reachable")
	notFound, ok := errors.AsType[*serviceerror.NotFound](err)
	require.True(t, ok)
	assert.Equal(t, "workflow execution already completed", notFound.Message)
	assert.NotErrorIs(t, err, port.ErrTemporalUnavailable)
	assert.Contains(t, err.Error(), port.SignalInstancePause+":"+instanceID.String(), "the channel name is in the message")
	assert.Equal(t, []w29bSignal{{"tenant:biz", "", port.SignalInstancePause + ":" + instanceID.String()}}, sdk.signals)
}

func TestClient_SignalWorkflow_NamespaceNotFoundIsNotReportedAsAMissingExecution(t *testing.T) {
	instanceID := uuid.New()
	sdkErr := serviceerror.NewNamespaceNotFound("workflows")
	sdk := &w29bSDK{signalErr: sdkErr}

	err := temporalclient.New(sdk).SignalWorkflow(context.Background(), "tenant:biz", instanceID, port.SignalInstancePause, nil)

	require.Error(t, err)
	assert.NotErrorIs(t, err, port.ErrWorkflowNotFound)
	assert.NotErrorIs(t, err, port.ErrTemporalUnavailable, "a refusal of the request is not curable by a retry")
	assert.ErrorIs(t, err, sdkErr)
	assert.Contains(t, err.Error(), port.SignalInstancePause+":"+instanceID.String())
}

func TestClient_StartWorkflow_OverrideMapIsPassedAsGiven(t *testing.T) {
	tenantID, instanceID, versionID := uuid.New(), uuid.New(), uuid.New()
	cases := []struct {
		name         string
		overrideMap  map[string]string
		wantOverride string
	}{
		{name: "nil stays nil", overrideMap: nil, wantOverride: `null`},
		{name: "empty stays empty", overrideMap: map[string]string{}, wantOverride: `{}`},
		{name: "populated is passed on", overrideMap: map[string]string{"finance/review": "user-1"}, wantOverride: `{"finance/review":"user-1"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sdk := &w29bSDK{}

			_, err := temporalclient.New(sdk).StartWorkflow(context.Background(), port.StartWorkflowInput{
				TemporalWorkflowID: "tenant:biz", TaskQueue: "q", TenantID: tenantID, InstanceID: instanceID,
				WorkflowVersionID: versionID, ContextJSON: " {\"a\" : [1, 2]}\n", OverrideMap: tc.overrideMap,
			})

			require.NoError(t, err)
			require.Len(t, sdk.executeArgs, 1)
			require.Len(t, sdk.executeArgs[0], 1, "one input argument")
			raw, err := json.Marshal(sdk.executeArgs[0][0])
			require.NoError(t, err)
			var fields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(raw, &fields))
			assert.Len(t, fields, 5)
			assert.JSONEq(t, tc.wantOverride, string(fields["OverrideMap"]))
			assert.JSONEq(t, `"`+tenantID.String()+`"`, string(fields["TenantID"]))
			assert.JSONEq(t, `"`+instanceID.String()+`"`, string(fields["InstanceID"]))
			assert.JSONEq(t, `"`+versionID.String()+`"`, string(fields["VersionID"]))
			var contextJSON string
			require.NoError(t, json.Unmarshal(fields["ContextJSON"], &contextJSON))
			assert.Equal(t, " {\"a\" : [1, 2]}\n", contextJSON, "the context text is not re-encoded")
		})
	}
}

func TestClient_UpdateWorkflow_SendsTheInstanceUpdateAndWaitsForCompletion(t *testing.T) {
	instanceID := uuid.New()
	sdk := &w29bSDK{}
	payload := map[string]string{"Reason": "waiting"}

	err := temporalclient.New(sdk).UpdateWorkflow(context.Background(), "tenant:biz", instanceID, "stage-defer", payload)

	require.NoError(t, err)
	require.Len(t, sdk.updateOpts, 1)
	got := sdk.updateOpts[0]
	assert.Equal(t, "stage-defer:"+instanceID.String(), got.UpdateName)
	assert.Equal(t, []any{payload}, got.Args, "the payload is the only argument")
	assert.Equal(t, client.WorkflowUpdateStageCompleted, got.WaitForStage)
	assert.Equal(t, "tenant:biz", got.WorkflowID)
	assert.Empty(t, got.RunID, "the update targets the latest run")
	assert.Equal(t, 1, sdk.handleGets, "the result of the update is awaited")
}

func TestClient_UpdateWorkflow_NotFoundFromEitherStepIsAMissingExecution(t *testing.T) {
	cases := []struct {
		name string
		sdk  func(sdkErr error) *w29bSDK
	}{
		{name: "from the update call", sdk: func(e error) *w29bSDK { return &w29bSDK{updateCallErr: e} }},
		{name: "from waiting for the result", sdk: func(e error) *w29bSDK { return &w29bSDK{handleErr: e} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			instanceID := uuid.New()
			sdkErr := serviceerror.NewNotFound("workflow execution already completed")

			err := temporalclient.New(tc.sdk(sdkErr)).UpdateWorkflow(context.Background(), "tenant:biz", instanceID, "stage-defer", struct{}{})

			require.ErrorIs(t, err, port.ErrWorkflowNotFound)
			assert.ErrorIs(t, err, sdkErr)
			_, ok := errors.AsType[*serviceerror.NotFound](err)
			assert.True(t, ok, "the original serviceerror.NotFound is reachable")
			assert.Contains(t, err.Error(), "stage-defer:"+instanceID.String())
			assert.NotErrorIs(t, err, port.ErrUpdateRefused)
			assert.NotErrorIs(t, err, port.ErrTemporalUnavailable)
		})
	}
}

func TestClient_UpdateWorkflow_NotFoundIsCheckedBeforeTheApplicationErrorType(t *testing.T) {
	refusal := temporal.NewNonRetryableApplicationError("defer refused: no step", port.UpdateRefusedErrorType, nil)
	sdk := &w29bSDK{handleErr: errors.Join(refusal, serviceerror.NewNotFound("gone"))}

	err := temporalclient.New(sdk).UpdateWorkflow(context.Background(), "tenant:biz", uuid.New(), "stage-defer", struct{}{})

	require.ErrorIs(t, err, port.ErrWorkflowNotFound)
	assert.NotErrorIs(t, err, port.ErrUpdateRefused)
}

func TestClient_UpdateWorkflow_RefusalCarriesOnlyTheRefusalMessage(t *testing.T) {
	instanceID := uuid.New()
	cause := errors.New("w29b internal cause")
	refusal := temporal.NewNonRetryableApplicationError("defer refused: no completed step", port.UpdateRefusedErrorType, cause)
	sdk := &w29bSDK{handleErr: refusal}

	err := temporalclient.New(sdk).UpdateWorkflow(context.Background(), "tenant:biz", instanceID, "stage-defer", struct{}{})

	require.ErrorIs(t, err, port.ErrUpdateRefused)
	assert.Contains(t, err.Error(), "defer refused: no completed step")
	assert.NotContains(t, err.Error(), "stage-defer", "the update name is dropped")
	assert.NotContains(t, err.Error(), instanceID.String())
	assert.NotContains(t, err.Error(), "w29b internal cause", "the original cause is dropped")
	_, stillThere := errors.AsType[*temporal.ApplicationError](err)
	assert.False(t, stillThere, "the original error is not wrapped")
	assert.NotErrorIs(t, err, cause)
}

func TestClient_UpdateWorkflow_AnActivityFailureKeepsTheUpdateNameAndTheOriginalError(t *testing.T) {
	instanceID := uuid.New()
	appErr := temporal.NewApplicationError("defer task: db down", "ActivityFailure", nil)
	sdk := &w29bSDK{handleErr: appErr}

	err := temporalclient.New(sdk).UpdateWorkflow(context.Background(), "tenant:biz", instanceID, "stage-defer", struct{}{})

	require.Error(t, err)
	assert.ErrorIs(t, err, appErr, "the original error stays reachable")
	assert.Contains(t, err.Error(), "stage-defer:"+instanceID.String())
	assert.Contains(t, err.Error(), "defer task: db down")
	assert.NotErrorIs(t, err, port.ErrUpdateRefused)
	assert.NotErrorIs(t, err, port.ErrWorkflowNotFound)
	assert.NotErrorIs(t, err, port.ErrTemporalUnavailable)
}

func TestClient_TerminateWorkflow_AnyErrorButNotFoundIsWrappedWithTheTerminatePrefix(t *testing.T) {
	cases := []struct {
		name            string
		sdkErr          error
		wantDeadline    bool
		wantUnavailable bool
	}{
		{name: "unavailable", sdkErr: serviceerror.NewUnavailable("frontend down"), wantUnavailable: true},
		{name: "cluster deadline exceeded", sdkErr: serviceerror.NewDeadlineExceeded("took too long"), wantUnavailable: true},
		{name: "caller deadline", sdkErr: context.DeadlineExceeded, wantDeadline: true, wantUnavailable: true},
		{name: "refused request", sdkErr: serviceerror.NewInvalidArgument("bad run id")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sdk := &w29bSDK{terminateErr: tc.sdkErr}

			err := temporalclient.New(sdk).TerminateWorkflow(context.Background(), "tenant:biz", "run-1", "reason")

			require.Error(t, err)
			assert.ErrorIs(t, err, tc.sdkErr, "the cause stays reachable")
			assert.Equal(t, tc.wantDeadline, errors.Is(err, context.DeadlineExceeded))
			assert.Equal(t, tc.wantUnavailable, errors.Is(err, port.ErrTemporalUnavailable))
			assert.NotErrorIs(t, err, port.ErrWorkflowNotFound)
			assert.Contains(t, err.Error(), "terminate workflow: ")
			assert.Contains(t, err.Error(), tc.sdkErr.Error())
		})
	}
}

func TestClient_TerminateWorkflow_PassesTheIdentifiersAndReasonUnchanged(t *testing.T) {
	cases := []struct {
		name  string
		runID string
	}{
		{name: "a named run", runID: "run-7"},
		{name: "no run id means the latest run", runID: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sdk := &w29bSDK{}

			err := temporalclient.New(sdk).TerminateWorkflow(context.Background(), "tenant:biz", tc.runID, "tenant offboarded")

			require.NoError(t, err)
			assert.Equal(t, []w29bTerminate{{"tenant:biz", tc.runID, "tenant offboarded", nil}}, sdk.terminates)
		})
	}
}

func TestClient_QueryWorkflow_TargetsTheLatestRunWithNoArguments(t *testing.T) {
	sdk := &w29bSDK{queryErr: serviceerror.NewQueryFailed("handler failed")}
	var out []string

	err := temporalclient.New(sdk).QueryWorkflow(context.Background(), "tenant:biz", port.QueryWaitingExternal, &out)

	require.Error(t, err)
	assert.Equal(t, []w29bQuery{{"tenant:biz", "", port.QueryWaitingExternal, nil}}, sdk.queries)
}

func TestClient_QueryWorkflow_ADeadlineDuringTheQueryKeepsTheDeadlineError(t *testing.T) {
	sdk := &w29bSDK{queryErr: context.DeadlineExceeded}
	var out []string

	err := temporalclient.New(sdk).QueryWorkflow(context.Background(), "tenant:biz", port.QueryWaitingExternal, &out)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.ErrorIs(t, err, port.ErrTemporalUnavailable)
	assert.Contains(t, err.Error(), "query workflow "+`"`+port.QueryWaitingExternal+`"`)
	assert.Empty(t, out)
}
