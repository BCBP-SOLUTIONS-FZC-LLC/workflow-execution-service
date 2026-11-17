package temporalclient_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporalclient"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestClient_AReadinessRaceIsMarkedUnavailableOnEveryCall(t *testing.T) {
	races := map[string]error{
		"workflow not ready":   serviceerror.NewWorkflowNotReady("workflow is not ready"),
		"namespace not active": serviceerror.NewNamespaceNotActive("workflow", "cluster-a", "cluster-b"),
	}
	for name, sdkErr := range races {
		calls := map[string]func() error{
			"start": func() error {
				sdk := &fakeSDKClient{executeWorkflowFunc: func(context.Context, client.StartWorkflowOptions, interface{}, ...interface{}) (client.WorkflowRun, error) {
					return nil, sdkErr
				}}
				_, err := temporalclient.New(sdk).StartWorkflow(context.Background(), port.StartWorkflowInput{TenantID: uuid.New(), InstanceID: uuid.New(), WorkflowVersionID: uuid.New()})
				return err
			},
			"signal": func() error {
				sdk := &fakeSDKClient{signalWorkflowFunc: func(context.Context, string, string, string, interface{}) error { return sdkErr }}
				return temporalclient.New(sdk).SignalWorkflow(context.Background(), "wf", uuid.New(), "instance-pause", nil)
			},
			"update":      func() error { return updateFailingWith(sdkErr, nil) },
			"update wait": func() error { return updateFailingWith(nil, sdkErr) },
			"query": func() error {
				sdk := &fakeSDKClient{queryWorkflowFunc: func(context.Context, string, string, string, ...interface{}) (converter.EncodedValue, error) {
					return nil, sdkErr
				}}
				var out []string
				return temporalclient.New(sdk).QueryWorkflow(context.Background(), "wf", port.QueryWaitingExternal, &out)
			},
			"terminate": func() error {
				sdk := &fakeSDKClient{terminateWorkflowFunc: func(context.Context, string, string, string, ...interface{}) error { return sdkErr }}
				return temporalclient.New(sdk).TerminateWorkflow(context.Background(), "wf", "", "reason")
			},
		}
		for call, run := range calls {
			t.Run(name+" on "+call, func(t *testing.T) {
				err := run()

				require.Error(t, err)
				assert.ErrorIs(t, err, port.ErrTemporalUnavailable)
			})
		}
	}
}
