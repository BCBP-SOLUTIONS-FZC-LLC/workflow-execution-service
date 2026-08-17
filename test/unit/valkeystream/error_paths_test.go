// Error paths for the raw Stream command wrappers, without a real Valkey
// instance — connecting to a port nothing listens on forces every command to
// fail immediately. Happy paths are covered against a real container in
// test/integration/valkeystream (mirrors internal/adapter/outbound/valkey's
// own unit/integration split).
package valkeystream_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkeystream"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func newUnreachableClient() redis.Cmdable {
	return redis.NewClient(&redis.Options{
		Addr:        "127.0.0.1:1",
		DialTimeout: 200 * time.Millisecond,
	})
}

func TestProducer_Publish_ConnectionError(t *testing.T) {
	producer := valkeystream.NewProducer(newUnreachableClient())
	_, err := producer.Publish(context.Background(), "stream", map[string]string{"k": "v"})
	assert.Error(t, err)
}

func TestConsumer_EnsureGroup_ConnectionError(t *testing.T) {
	consumer := valkeystream.NewConsumer(newUnreachableClient())
	err := consumer.EnsureGroup(context.Background(), "stream", "group")
	assert.Error(t, err)
}

func TestConsumer_ReadGroup_ConnectionError(t *testing.T) {
	consumer := valkeystream.NewConsumer(newUnreachableClient())
	_, err := consumer.ReadGroup(context.Background(), "stream", "group", "consumer", 10, time.Second)
	assert.Error(t, err)
}

func TestConsumer_Ack_ConnectionError(t *testing.T) {
	consumer := valkeystream.NewConsumer(newUnreachableClient())
	err := consumer.Ack(context.Background(), "stream", "group", "1-0")
	assert.Error(t, err)
}

func TestConsumer_ReclaimStale_ConnectionError(t *testing.T) {
	consumer := valkeystream.NewConsumer(newUnreachableClient())
	_, err := consumer.ReclaimStale(context.Background(), "stream", "group", "consumer", time.Second, 10)
	assert.Error(t, err)
}

func TestEventPublisher_PublishTaskCreated_ConnectionError(t *testing.T) {
	pub := valkeystream.NewEventPublisher(valkeystream.NewProducer(newUnreachableClient()), "stream")
	err := pub.PublishTaskCreated(context.Background(), port.ConnectorTaskCreatedEvent{EventID: uuid.New()})
	assert.Error(t, err)
}
