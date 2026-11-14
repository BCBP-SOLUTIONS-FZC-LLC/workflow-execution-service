package valkeystream_test

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkeystream"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type vsxRecordingPublisher struct {
	calls     int
	streamKey string
	fields    map[string]string
}

func (r *vsxRecordingPublisher) Publish(_ context.Context, streamKey string, fields map[string]string) (string, error) {
	r.calls++
	r.streamKey = streamKey
	r.fields = fields
	return "1-0", nil
}

func TestEventPublisher_PublishTaskCreated_AbsentOptionalFields_WritesNullNullAndEmptyNodeKey(t *testing.T) {
	t.Parallel()

	rec := &vsxRecordingPublisher{}
	pub := valkeystream.NewEventPublisher(rec, "vsx-connector-tasks")
	event := port.ConnectorTaskCreatedEvent{
		EventID:       uuid.New(),
		TenantID:      uuid.New(),
		InstanceID:    uuid.New(),
		TaskID:        uuid.New(),
		DepartmentID:  uuid.New(),
		ConnectorType: "storage",
		PrincipalID:   uuid.New(),
		PrincipalSub:  "sub-1",
	}

	require.NoError(t, pub.PublishTaskCreated(context.Background(), event))

	require.Equal(t, 1, rec.calls)
	assert.Equal(t, "vsx-connector-tasks", rec.streamKey)
	assert.Equal(t, "null", rec.fields["resolved_inputs"])
	assert.Equal(t, "null", rec.fields["output_mapping"])
	val, present := rec.fields["node_key"]
	assert.True(t, present, "node_key is always written")
	assert.Empty(t, val)
	assert.Equal(t, event.TaskID.String(), rec.fields["task_id"])
	assert.Equal(t, event.DepartmentID.String(), rec.fields["department_id"])
	assert.Equal(t, "storage", rec.fields["connector_type"])
}

func TestEventPublisher_PublishTaskCreated_UnencodableResolvedInputs_ErrorsWithoutPublishing(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		value any
		check func(t *testing.T, err error)
	}{
		{"func", func() {}, func(t *testing.T, err error) {
			var target *json.UnsupportedTypeError
			assert.ErrorAs(t, err, &target)
		}},
		{"channel", make(chan int), func(t *testing.T, err error) {
			var target *json.UnsupportedTypeError
			assert.ErrorAs(t, err, &target)
		}},
		{"NaN", math.NaN(), func(t *testing.T, err error) {
			var target *json.UnsupportedValueError
			assert.ErrorAs(t, err, &target)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := &vsxRecordingPublisher{}
			pub := valkeystream.NewEventPublisher(rec, "vsx-connector-tasks")
			event := port.ConnectorTaskCreatedEvent{
				EventID:        uuid.New(),
				ResolvedInputs: map[string]any{"bad": tc.value},
			}

			err := pub.PublishTaskCreated(context.Background(), event)

			require.Error(t, err)
			assert.True(t, strings.HasPrefix(err.Error(), "valkeystream: marshal resolved_inputs: "), err.Error())
			tc.check(t, err)
			assert.Zero(t, rec.calls, "nothing is published when the inputs cannot be encoded")
		})
	}
}

func TestConsumer_Touch_Unreachable_ReturnsTouchPrefixedError(t *testing.T) {
	t.Parallel()

	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 200 * time.Millisecond})
	t.Cleanup(func() { _ = client.Close() })
	consumer := valkeystream.NewConsumer(client)

	err := consumer.Touch(context.Background(), "vsx-stream", "vsx-group", "vsx-consumer", []string{"1-0"})

	require.Error(t, err)
	assert.True(t, strings.HasPrefix(err.Error(), "valkeystream touch: "), err.Error())
}
