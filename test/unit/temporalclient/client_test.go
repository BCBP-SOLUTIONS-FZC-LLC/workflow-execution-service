// Package temporalclient_test exercises internal/adapter/outbound/temporalclient
// against a hand-rolled fake go.temporal.io/sdk/client.Client -- the SDK ships
// no mock of its own, and client.Client is too large to hand-implement in
// full, so the fake embeds the real (nil) interface and overrides only the
// four methods temporalclient.Client actually calls; any other call panics on
// the nil embedded value, which is fine since nothing under test reaches them.
package temporalclient_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporalclient"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type fakeSDKClient struct {
	client.Client

	executeWorkflowFunc   func(ctx context.Context, options client.StartWorkflowOptions, workflow interface{}, args ...interface{}) (client.WorkflowRun, error)
	signalWorkflowFunc    func(ctx context.Context, workflowID, runID, signalName string, arg interface{}) error
	terminateWorkflowFunc func(ctx context.Context, workflowID, runID, reason string, details ...interface{}) error
	queryWorkflowFunc     func(ctx context.Context, workflowID, runID, queryType string, args ...interface{}) (converter.EncodedValue, error)
}

func (f *fakeSDKClient) ExecuteWorkflow(ctx context.Context, options client.StartWorkflowOptions, workflow interface{}, args ...interface{}) (client.WorkflowRun, error) {
	return f.executeWorkflowFunc(ctx, options, workflow, args...)
}

func (f *fakeSDKClient) SignalWorkflow(ctx context.Context, workflowID, runID, signalName string, arg interface{}) error {
	return f.signalWorkflowFunc(ctx, workflowID, runID, signalName, arg)
}

func (f *fakeSDKClient) TerminateWorkflow(ctx context.Context, workflowID, runID, reason string, details ...interface{}) error {
	return f.terminateWorkflowFunc(ctx, workflowID, runID, reason, details...)
}

func (f *fakeSDKClient) QueryWorkflow(ctx context.Context, workflowID, runID, queryType string, args ...interface{}) (converter.EncodedValue, error) {
	return f.queryWorkflowFunc(ctx, workflowID, runID, queryType, args...)
}

type fakeWorkflowRun struct {
	client.WorkflowRun
	id, runID string
}

func (f *fakeWorkflowRun) GetID() string    { return f.id }
func (f *fakeWorkflowRun) GetRunID() string { return f.runID }

// fakeEncodedValue JSON round-trips val through Get, matching the real SDK's
// own JSON-based default converter closely enough for this adapter's tests.
type fakeEncodedValue struct {
	converter.EncodedValue
	val any
	err error
}

func (f *fakeEncodedValue) Get(valuePtr interface{}) error {
	if f.err != nil {
		return f.err
	}
	b, err := json.Marshal(f.val)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, valuePtr)
}

func TestClient_StartWorkflow(t *testing.T) {
	tenantID, instanceID, versionID := uuid.New(), uuid.New(), uuid.New()

	t.Run("success", func(t *testing.T) {
		var gotOptions client.StartWorkflowOptions
		var gotWorkflowType interface{}
		var gotArg interface{}
		sdk := &fakeSDKClient{
			executeWorkflowFunc: func(_ context.Context, options client.StartWorkflowOptions, workflow interface{}, args ...interface{}) (client.WorkflowRun, error) {
				gotOptions = options
				gotWorkflowType = workflow
				require.Len(t, args, 1)
				gotArg = args[0]
				return &fakeWorkflowRun{id: options.ID, runID: "run-1"}, nil
			},
		}
		c := temporalclient.New(sdk)

		out, err := c.StartWorkflow(context.Background(), port.StartWorkflowInput{
			TemporalWorkflowID: "tenant:biz-key",
			TaskQueue:          "wf-queue-default",
			TenantID:           tenantID,
			InstanceID:         instanceID,
			WorkflowVersionID:  versionID,
			ContextJSON:        `{"a":1}`,
			OverrideMap:        map[string]string{"node": "user-id"},
		})
		require.NoError(t, err)
		assert.Equal(t, "tenant:biz-key", out.TemporalWorkflowID)
		assert.Equal(t, "run-1", out.TemporalRunID)

		assert.Equal(t, "tenant:biz-key", gotOptions.ID)
		assert.Equal(t, "wf-queue-default", gotOptions.TaskQueue)
		assert.Equal(t, enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE, gotOptions.WorkflowIDReusePolicy)
		assert.Equal(t, port.WorkflowTypeExecute, gotWorkflowType)

		// gotArg is the adapter's own unexported executeWorkflowInput mirror
		// struct -- assert its JSON shape (what Temporal actually transmits)
		// rather than its Go type, matching internal/workflow.ExecuteInput
		// field-for-field.
		b, err := json.Marshal(gotArg)
		require.NoError(t, err)
		assert.JSONEq(t, `{
			"TenantID": "`+tenantID.String()+`",
			"InstanceID": "`+instanceID.String()+`",
			"VersionID": "`+versionID.String()+`",
			"ContextJSON": "{\"a\":1}",
			"OverrideMap": {"node": "user-id"}
		}`, string(b))
	})

	t.Run("sdk error is wrapped", func(t *testing.T) {
		sdk := &fakeSDKClient{
			executeWorkflowFunc: func(context.Context, client.StartWorkflowOptions, interface{}, ...interface{}) (client.WorkflowRun, error) {
				return nil, errors.New("temporal unavailable")
			},
		}
		c := temporalclient.New(sdk)
		_, err := c.StartWorkflow(context.Background(), port.StartWorkflowInput{TenantID: tenantID, InstanceID: instanceID, WorkflowVersionID: versionID})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "temporal unavailable")
	})
}

func TestClient_SignalWorkflow(t *testing.T) {
	instanceID := uuid.New()

	t.Run("success builds the instance-suffixed channel name", func(t *testing.T) {
		var gotWorkflowID, gotRunID, gotSignalName string
		var gotArg interface{}
		sdk := &fakeSDKClient{
			signalWorkflowFunc: func(_ context.Context, workflowID, runID, signalName string, arg interface{}) error {
				gotWorkflowID, gotRunID, gotSignalName, gotArg = workflowID, runID, signalName, arg
				return nil
			},
		}
		c := temporalclient.New(sdk)

		payload := map[string]any{"AdminUserID": "u1"}
		err := c.SignalWorkflow(context.Background(), "tenant:biz-key", instanceID, port.SignalInstancePause, payload)
		require.NoError(t, err)

		assert.Equal(t, "tenant:biz-key", gotWorkflowID)
		assert.Empty(t, gotRunID)
		assert.Equal(t, port.SignalInstancePause+":"+instanceID.String(), gotSignalName)
		assert.Equal(t, payload, gotArg)
	})

	t.Run("sdk error is wrapped", func(t *testing.T) {
		sdk := &fakeSDKClient{
			signalWorkflowFunc: func(context.Context, string, string, string, interface{}) error {
				return errors.New("not found")
			},
		}
		c := temporalclient.New(sdk)
		err := c.SignalWorkflow(context.Background(), "tenant:biz-key", instanceID, port.SignalInstancePause, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})
}

func TestClient_TerminateWorkflow(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		var gotWorkflowID, gotRunID, gotReason string
		sdk := &fakeSDKClient{
			terminateWorkflowFunc: func(_ context.Context, workflowID, runID, reason string, _ ...interface{}) error {
				gotWorkflowID, gotRunID, gotReason = workflowID, runID, reason
				return nil
			},
		}
		c := temporalclient.New(sdk)
		require.NoError(t, c.TerminateWorkflow(context.Background(), "tenant:biz-key", "admin requested"))
		assert.Equal(t, "tenant:biz-key", gotWorkflowID)
		assert.Empty(t, gotRunID)
		assert.Equal(t, "admin requested", gotReason)
	})

	t.Run("sdk error is wrapped", func(t *testing.T) {
		sdk := &fakeSDKClient{
			terminateWorkflowFunc: func(context.Context, string, string, string, ...interface{}) error {
				return errors.New("already closed")
			},
		}
		c := temporalclient.New(sdk)
		err := c.TerminateWorkflow(context.Background(), "tenant:biz-key", "reason")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "already closed")
	})
}

func TestClient_QueryWorkflow(t *testing.T) {
	type statusResult struct {
		Status string
	}

	t.Run("success decodes into out", func(t *testing.T) {
		sdk := &fakeSDKClient{
			queryWorkflowFunc: func(_ context.Context, workflowID, runID, queryType string, _ ...interface{}) (converter.EncodedValue, error) {
				assert.Equal(t, "tenant:biz-key", workflowID)
				assert.Empty(t, runID)
				assert.Equal(t, "get-workflow-status", queryType)
				return &fakeEncodedValue{val: statusResult{Status: "RUNNING"}}, nil
			},
		}
		c := temporalclient.New(sdk)

		var out statusResult
		require.NoError(t, c.QueryWorkflow(context.Background(), "tenant:biz-key", "get-workflow-status", &out))
		assert.Equal(t, "RUNNING", out.Status)
	})

	t.Run("sdk error is wrapped", func(t *testing.T) {
		sdk := &fakeSDKClient{
			queryWorkflowFunc: func(context.Context, string, string, string, ...interface{}) (converter.EncodedValue, error) {
				return nil, errors.New("query rejected")
			},
		}
		c := temporalclient.New(sdk)
		var out statusResult
		err := c.QueryWorkflow(context.Background(), "tenant:biz-key", "get-workflow-status", &out)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "query rejected")
	})

	t.Run("decode error is wrapped", func(t *testing.T) {
		sdk := &fakeSDKClient{
			queryWorkflowFunc: func(context.Context, string, string, string, ...interface{}) (converter.EncodedValue, error) {
				return &fakeEncodedValue{err: errors.New("bad payload")}, nil
			},
		}
		c := temporalclient.New(sdk)
		var out statusResult
		err := c.QueryWorkflow(context.Background(), "tenant:biz-key", "get-workflow-status", &out)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "bad payload")
	})
}
