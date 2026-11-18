package service_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
)

// w14bSignalResult decodes the result a stage-transition signal carries.
func w14bSignalResult(t *testing.T, temporal *fakeTemporalClient) map[string]any {
	t.Helper()
	require.Len(t, temporal.signals, 1)
	b, err := json.Marshal(temporal.signals[0].Payload)
	require.NoError(t, err)
	var wire struct{ ResultJSON string }
	require.NoError(t, json.Unmarshal(b, &wire))
	var result map[string]any
	require.NoError(t, json.Unmarshal([]byte(wire.ResultJSON), &result))
	return result
}

func TestConnectorTaskServiceComplete_OutputMappingEdges(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		mapping string
		output  map[string]any
		want    map[string]any
	}{
		{
			name:    "two entries for one target, the later entry in the list wins",
			mapping: `[{"source":"first","target":"packet"},{"source":"second","target":"packet"}]`,
			output:  map[string]any{"first": "one", "second": "two"},
			want:    map[string]any{"packet": "two"},
		},
		{
			name:    "the list order decides, not the output key order",
			mapping: `[{"source":"second","target":"packet"},{"source":"first","target":"packet"}]`,
			output:  map[string]any{"first": "one", "second": "two"},
			want:    map[string]any{"packet": "one"},
		},
		{
			name:    "the equals spelling of a source takes part in the later-wins rule",
			mapping: `[{"source":"first","target":"packet"},{"source":"=second","target":"packet"}]`,
			output:  map[string]any{"first": "one", "second": "two"},
			want:    map[string]any{"packet": "two"},
		},
		{
			name:    "an entry whose output key is absent is left out, not null",
			mapping: `[{"source":"present","target":"kept"},{"source":"missing","target":"dropped"}]`,
			output:  map[string]any{"present": "yes"},
			want:    map[string]any{"kept": "yes"},
		},
		{
			name:    "an absent key later in the list does not overwrite an earlier value for the same target",
			mapping: `[{"source":"present","target":"packet"},{"source":"missing","target":"packet"}]`,
			output:  map[string]any{"present": "yes"},
			want:    map[string]any{"packet": "yes"},
		},
		{
			name:    "a present key whose value is null is kept as null",
			mapping: `[{"source":"nothing","target":"packet"}]`,
			output:  map[string]any{"nothing": nil},
			want:    map[string]any{"packet": nil},
		},
		{
			name:    "no entry finds its key, so the result is an empty object",
			mapping: `[{"source":"missing","target":"dropped"}]`,
			output:  map[string]any{"other": "x"},
			want:    map[string]any{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			extras := fmt.Sprintf(`{"resolved_inputs":{},"output_mapping":%s}`, tc.mapping)
			task := connectorTask("storage", domain.TaskStatusReady, extras)
			inst := instanceWithStatus(task, domain.InstanceStatusRunning)
			temporal := &fakeTemporalClient{}
			svc := newConnectorTaskService(task, inst, temporal, newFakeCacheStore())

			require.NoError(t, svc.Complete(context.Background(), task.TenantID, task.ID, tc.output))

			assert.Equal(t, "stage-transition", temporal.signals[0].SignalName)
			assert.Equal(t, inst.ID, temporal.signals[0].InstanceID)
			assert.Equal(t, tc.want, w14bSignalResult(t, temporal))
		})
	}
}

func TestConnectorTaskServiceFail_LogsOnlySortedOutputKeys(t *testing.T) {
	t.Parallel()

	const secret = "bearer-token-do-not-log"
	cases := []struct {
		name     string
		output   map[string]any
		wantKeys []string
	}{
		{
			name:     "several keys are listed in ascending order",
			output:   map[string]any{"zeta": secret, "alpha": secret, "mid": secret, "Beta": secret},
			wantKeys: []string{"Beta", "alpha", "mid", "zeta"},
		},
		{name: "a nil output gives an empty list", output: nil, wantKeys: []string{}},
		{name: "an empty output gives an empty list", output: map[string]any{}, wantKeys: []string{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			task := connectorTask("send-email", domain.TaskStatusReady, `{}`)
			inst := instanceWithStatus(task, domain.InstanceStatusRunning)
			temporal := &fakeTemporalClient{}
			svc := newConnectorTaskServiceWith(task, inst, temporal, newFakeCacheStore(), newFakeAssignmentRepo())
			log := &fakeLogger{}
			svc.Log = log

			require.NoError(t, svc.Fail(context.Background(), task.TenantID, task.ID, "upstream_error", tc.output))

			require.Len(t, log.warnFields, 1)
			fields := log.warnFields[0]
			assert.Equal(t, tc.wantKeys, fields["output_keys"])
			assert.Equal(t, "upstream_error", fields["error_class"])
			assert.Equal(t, task.ID, fields["task_id"])
			assert.Equal(t, task.TenantID, fields["tenant_id"])
			assert.NotContains(t, fmt.Sprint(fields), secret, "output values never reach the log")
			require.Len(t, temporal.signals, 1)
			assert.Equal(t, "stage-fail", temporal.signals[0].SignalName)
		})
	}
}
