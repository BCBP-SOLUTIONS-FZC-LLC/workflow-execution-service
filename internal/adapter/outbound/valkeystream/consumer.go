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
	entries := toEntries(streams)
	for i := range entries {
		entries[i].Deliveries = 1
	}
	return entries, nil
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
	// The entries are claimed and counted already, so a count that can't be
	// read leaves Deliveries at 0 rather than dropping them.
	entries := toEntriesFromMessages(messages)
	for i := range entries {
		pending, err := c.client.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: streamKey, Group: group, Start: entries[i].ID, End: entries[i].ID, Count: 1,
		}).Result()
		if err == nil && len(pending) == 1 {
			entries[i].Deliveries = pending[0].RetryCount
		}
	}
	return entries, nil
}

// Touch resets the idle time of entries this consumer is still handling.
// XCLAIM with JUSTID leaves the delivery count alone.
func (c *Consumer) Touch(ctx context.Context, streamKey, group, consumerName string, ids []string) error {
	err := c.client.XClaimJustID(ctx, &redis.XClaimArgs{
		Stream: streamKey, Group: group, Consumer: consumerName, MinIdle: 0, Messages: ids,
	}).Err()
	if err != nil {
		return fmt.Errorf("valkeystream touch: %w", err)
	}
	return nil
}

// DeadLetter copies entry onto deadLetterKey with the reason it was given
// up on, then acks it on streamKey.
func (c *Consumer) DeadLetter(ctx context.Context, streamKey, group, deadLetterKey string, entry Entry, reason string) error {
	values := make(map[string]any, len(entry.Fields)+2)
	for k, v := range entry.Fields {
		values[k] = v
	}
	values["reason"] = reason
	values["source_entry_id"] = entry.ID
	_, err := c.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.XAdd(ctx, &redis.XAddArgs{Stream: deadLetterKey, Values: values})
		pipe.XAck(ctx, streamKey, group, entry.ID)
		return nil
	})
	if err != nil {
		return fmt.Errorf("valkeystream dead letter: %w", err)
	}
	return nil
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
