package valkeystream_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkeystream"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

type w30bRecordingPublisher struct {
	calls     int
	streamKey string
	fields    map[string]string
	err       error
}

func (r *w30bRecordingPublisher) Publish(_ context.Context, streamKey string, fields map[string]string) (string, error) {
	r.calls++
	r.streamKey = streamKey
	r.fields = fields
	if r.err != nil {
		return "", r.err
	}
	return "1-0", nil
}

func w30bUnreachable(t *testing.T) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{
		Addr:        "127.0.0.1:1",
		DialTimeout: 200 * time.Millisecond,
		MaxRetries:  -1,
	})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func w30bClosed(t *testing.T) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 200 * time.Millisecond})
	require.NoError(t, client.Close())
	return client
}

func TestEventPublisher_PublishTaskCreated_FullEvent_WritesExactlyElevenStringFields(t *testing.T) {
	t.Parallel()

	rec := &w30bRecordingPublisher{}
	pub := valkeystream.NewEventPublisher(rec, "w30b-connector-tasks")
	event := port.ConnectorTaskCreatedEvent{
		EventID:        uuid.New(),
		TenantID:       uuid.New(),
		InstanceID:     uuid.New(),
		TaskID:         uuid.New(),
		NodeKey:        "dept-1/nodeA",
		DepartmentID:   uuid.New(),
		ConnectorType:  "storage",
		ResolvedInputs: map[string]any{"bucket": "b1"},
		OutputMapping:  []dsl.IOVar{{Source: "contentRef", Target: "docRef"}},
		PrincipalID:    uuid.New(),
		PrincipalSub:   "svc-automation",
	}

	require.NoError(t, pub.PublishTaskCreated(context.Background(), event))

	require.Equal(t, 1, rec.calls, "one XADD per event")
	assert.Equal(t, "w30b-connector-tasks", rec.streamKey)
	require.Len(t, rec.fields, 11)
	assert.Equal(t, event.EventID.String(), rec.fields["event_id"])
	assert.Equal(t, event.TenantID.String(), rec.fields["tenant_id"])
	assert.Equal(t, event.InstanceID.String(), rec.fields["instance_id"])
	assert.Equal(t, event.TaskID.String(), rec.fields["task_id"])
	assert.Equal(t, "dept-1/nodeA", rec.fields["node_key"])
	assert.Equal(t, event.DepartmentID.String(), rec.fields["department_id"])
	assert.Equal(t, "storage", rec.fields["connector_type"])
	assert.JSONEq(t, `{"bucket":"b1"}`, rec.fields["resolved_inputs"])
	assert.JSONEq(t, `[{"source":"contentRef","target":"docRef"}]`, rec.fields["output_mapping"])
	assert.Equal(t, event.PrincipalID.String(), rec.fields["principal_id"])
	assert.Equal(t, "svc-automation", rec.fields["principal_sub"])
}

func TestEventPublisher_PublishTaskCreated_ZeroEvent_AdapterPassesFieldsThroughWithoutValidating(t *testing.T) {
	t.Parallel()

	rec := &w30bRecordingPublisher{}
	pub := valkeystream.NewEventPublisher(rec, "w30b-connector-tasks")

	err := pub.PublishTaskCreated(context.Background(), port.ConnectorTaskCreatedEvent{})

	require.NoError(t, err, "no required id is validated before publishing")
	require.Equal(t, 1, rec.calls)
	const zero = "00000000-0000-0000-0000-000000000000"
	assert.Len(t, rec.fields, 11)
	for _, key := range []string{"event_id", "tenant_id", "instance_id", "task_id", "department_id", "principal_id"} {
		assert.Equal(t, zero, rec.fields[key], key)
	}
	for _, key := range []string{"node_key", "connector_type", "principal_sub"} {
		value, present := rec.fields[key]
		assert.True(t, present, "%s is always written", key)
		assert.Empty(t, value, key)
	}
}

func TestEventPublisher_PublishTaskCreated_PublishFails_WrapsTheCause(t *testing.T) {
	t.Parallel()

	cause := errors.New("w30b stream down")
	rec := &w30bRecordingPublisher{err: cause}
	pub := valkeystream.NewEventPublisher(rec, "w30b-connector-tasks")

	err := pub.PublishTaskCreated(context.Background(), port.ConnectorTaskCreatedEvent{EventID: uuid.New()})

	require.ErrorIs(t, err, cause)
	assert.True(t, strings.HasPrefix(err.Error(), "valkeystream: publish task-created: "), err.Error())
	assert.Equal(t, 1, rec.calls)
}

func TestProducer_Publish_Failure_NamesXaddAndWrapsTheCause(t *testing.T) {
	t.Parallel()

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	cases := []struct {
		name   string
		client func(t *testing.T) *redis.Client
		ctx    context.Context
		check  func(t *testing.T, err error)
	}{
		{"unreachable server", w30bUnreachable, context.Background(), func(t *testing.T, err error) {
			var netErr net.Error
			assert.ErrorAs(t, err, &netErr)
		}},
		{"cancelled context", w30bUnreachable, cancelled, func(t *testing.T, err error) {
			assert.ErrorIs(t, err, context.Canceled)
		}},
		{"closed client", w30bClosed, context.Background(), func(t *testing.T, err error) {
			assert.ErrorIs(t, err, redis.ErrClosed)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			producer := valkeystream.NewProducer(tc.client(t))

			id, err := producer.Publish(tc.ctx, "w30b-stream", map[string]string{"k": "v"})

			require.Error(t, err)
			assert.Empty(t, id)
			assert.True(t, strings.HasPrefix(err.Error(), "valkeystream xadd: "), err.Error())
			tc.check(t, err)
		})
	}
}

func TestConsumer_Calls_FailureWrapsStepPrefixAndCause(t *testing.T) {
	t.Parallel()

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	type call func(c *valkeystream.Consumer, ctx context.Context) (entries []valkeystream.Entry, err error)
	calls := []struct {
		name   string
		prefix string
		run    call
	}{
		{"ensure group", "valkeystream ensure group: ", func(c *valkeystream.Consumer, ctx context.Context) ([]valkeystream.Entry, error) {
			return nil, c.EnsureGroup(ctx, "w30b-stream", "w30b-group")
		}},
		{"read group", "valkeystream read group: ", func(c *valkeystream.Consumer, ctx context.Context) ([]valkeystream.Entry, error) {
			return c.ReadGroup(ctx, "w30b-stream", "w30b-group", "w30b-consumer", 10, time.Second)
		}},
		{"ack", "valkeystream ack: ", func(c *valkeystream.Consumer, ctx context.Context) ([]valkeystream.Entry, error) {
			return nil, c.Ack(ctx, "w30b-stream", "w30b-group", "1-0")
		}},
		{"reclaim stale", "valkeystream reclaim stale: ", func(c *valkeystream.Consumer, ctx context.Context) ([]valkeystream.Entry, error) {
			entries, _, err := c.ReclaimStale(ctx, "w30b-stream", "w30b-group", "w30b-consumer", "0", time.Second, 10)
			return entries, err
		}},
	}
	causes := []struct {
		name   string
		client func(t *testing.T) *redis.Client
		ctx    context.Context
		check  func(t *testing.T, err error)
	}{
		{"unreachable server", w30bUnreachable, context.Background(), func(t *testing.T, err error) {
			var netErr net.Error
			assert.ErrorAs(t, err, &netErr)
		}},
		{"cancelled context", w30bUnreachable, cancelled, func(t *testing.T, err error) {
			assert.ErrorIs(t, err, context.Canceled)
		}},
		{"closed client", w30bClosed, context.Background(), func(t *testing.T, err error) {
			assert.ErrorIs(t, err, redis.ErrClosed)
		}},
	}
	for _, cl := range calls {
		for _, cause := range causes {
			t.Run(cl.name+"/"+cause.name, func(t *testing.T) {
				t.Parallel()

				consumer := valkeystream.NewConsumer(cause.client(t))

				entries, err := cl.run(consumer, cause.ctx)

				require.Error(t, err)
				assert.Nil(t, entries, "no entries come back with an error")
				assert.True(t, strings.HasPrefix(err.Error(), cl.prefix), err.Error())
				cause.check(t, err)
			})
		}
	}
}

func TestEventPublisher_PublishTaskCreated_UnreachableServer_WrapsProducerError(t *testing.T) {
	t.Parallel()

	pub := valkeystream.NewEventPublisher(valkeystream.NewProducer(w30bUnreachable(t)), "w30b-stream")

	err := pub.PublishTaskCreated(context.Background(), port.ConnectorTaskCreatedEvent{EventID: uuid.New()})

	require.Error(t, err)
	assert.True(t, strings.HasPrefix(err.Error(), "valkeystream: publish task-created: valkeystream xadd: "), err.Error())
	var netErr net.Error
	assert.ErrorAs(t, err, &netErr)
}
