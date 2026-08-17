package valkeystream

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Consumer wraps the Valkey Stream consumer-group primitives
// cmd/connector-worker's dispatch loop needs: EnsureGroup once at startup,
// then ReadGroup/Ack/ReclaimStale in its main loop. connector-worker only
// ever XACKs an entry once dispatch has genuinely completed (success or
// fail-signal sent) — an un-acked entry becomes reclaimable by another
// consumer via ReclaimStale.
type Consumer struct {
	client redis.Cmdable
}

func NewConsumer(client redis.Cmdable) *Consumer {
	return &Consumer{client: client}
}

// EnsureGroup creates group on streamKey (MKSTREAM: the stream itself may
// not exist yet if nothing has published to it), tolerating a BUSYGROUP
// error as success — this runs every startup, not just the first.
func (c *Consumer) EnsureGroup(ctx context.Context, streamKey, group string) error {
	err := c.client.XGroupCreateMkStream(ctx, streamKey, group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("valkeystream ensure group: %w", err)
	}
	return nil
}

// ReadGroup blocks up to block for up to count new entries (">" — never yet
// delivered to this group) on streamKey.
func (c *Consumer) ReadGroup(ctx context.Context, streamKey, group, consumerName string, count int64, block time.Duration) ([]Entry, error) {
	streams, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: group, Consumer: consumerName, Streams: []string{streamKey, ">"}, Count: count, Block: block,
	}).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("valkeystream read group: %w", err)
	}
	return toEntries(streams), nil
}

// Ack acknowledges entryID on streamKey/group — only call this once dispatch
// has genuinely completed (success, or a fail-signal was sent); an unacked
// entry stays reclaimable.
func (c *Consumer) Ack(ctx context.Context, streamKey, group, entryID string) error {
	if err := c.client.XAck(ctx, streamKey, group, entryID).Err(); err != nil {
		return fmt.Errorf("valkeystream ack: %w", err)
	}
	return nil
}

// ReclaimStale claims entries idle for at least minIdle and still pending
// under group, reassigning them to consumerName — the modern XAUTOCLAIM
// replacement for hand-rolled XPENDING+XCLAIM, picking up entries whose
// original consumer died mid-dispatch.
func (c *Consumer) ReclaimStale(ctx context.Context, streamKey, group, consumerName string, minIdle time.Duration, count int64) ([]Entry, error) {
	messages, _, err := c.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream: streamKey, Group: group, Consumer: consumerName, MinIdle: minIdle, Start: "0", Count: count,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("valkeystream reclaim stale: %w", err)
	}
	return toEntriesFromMessages(messages), nil
}

func toEntries(streams []redis.XStream) []Entry {
	var out []Entry
	for _, s := range streams {
		out = append(out, toEntriesFromMessages(s.Messages)...)
	}
	return out
}

func toEntriesFromMessages(messages []redis.XMessage) []Entry {
	out := make([]Entry, len(messages))
	for i, m := range messages {
		fields := make(map[string]string, len(m.Values))
		for k, v := range m.Values {
			if s, ok := v.(string); ok {
				fields[k] = s
			}
		}
		out[i] = Entry{ID: m.ID, Fields: fields}
	}
	return out
}
