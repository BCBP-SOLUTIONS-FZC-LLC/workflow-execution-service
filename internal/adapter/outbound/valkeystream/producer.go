package valkeystream

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type Producer struct {
	client redis.Cmdable
}

func NewProducer(client redis.Cmdable) *Producer {
	return &Producer{client: client}
}

func (p *Producer) Publish(ctx context.Context, streamKey string, fields map[string]string) (string, error) {
	id, err := p.client.XAdd(ctx, &redis.XAddArgs{Stream: streamKey, Values: fields}).Result()
	if err != nil {
		return "", fmt.Errorf("valkeystream xadd: %w", err)
	}
	return id, nil
}
