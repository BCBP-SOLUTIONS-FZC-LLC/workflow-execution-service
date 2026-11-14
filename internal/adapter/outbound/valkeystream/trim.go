package valkeystream

import (
	"context"
	"fmt"
	"time"
)

// TrimToOldestPending removes the entries before the oldest one the group has not acked, so an unacknowledged
// entry is never lost. With nothing pending the bound is the group's last delivered entry, so entries not yet
// delivered stay. The last delivered id is read before the pending list, so a delivery in between cannot make
// the bound skip past an entry that has just become pending.
func (c *Consumer) TrimToOldestPending(ctx context.Context, streamKey, group string) (int64, error) {
	bound, err := c.trimBound(ctx, streamKey, group)
	if err != nil {
		return 0, fmt.Errorf("valkeystream trim: %w", err)
	}
	trimmed, err := c.client.XTrimMinID(ctx, streamKey, bound).Result()
	if err != nil {
		return 0, fmt.Errorf("valkeystream trim: %w", err)
	}
	return trimmed, nil
}

// TrimOlderThan removes the entries whose id carries a timestamp before cutoff.
func (c *Consumer) TrimOlderThan(ctx context.Context, streamKey string, cutoff time.Time) (int64, error) {
	trimmed, err := c.client.XTrimMinID(ctx, streamKey, fmt.Sprintf("%d-0", cutoff.UnixMilli())).Result()
	if err != nil {
		return 0, fmt.Errorf("valkeystream trim by age: %w", err)
	}
	return trimmed, nil
}

func (c *Consumer) trimBound(ctx context.Context, streamKey, group string) (string, error) {
	lastDelivered, err := c.lastDeliveredID(ctx, streamKey, group)
	if err != nil {
		return "", err
	}
	summary, err := c.client.XPending(ctx, streamKey, group).Result()
	if err != nil {
		return "", fmt.Errorf("pending summary: %w", err)
	}
	if summary.Count > 0 {
		return summary.Lower, nil
	}
	return lastDelivered, nil
}

func (c *Consumer) lastDeliveredID(ctx context.Context, streamKey, group string) (string, error) {
	groups, err := c.client.XInfoGroups(ctx, streamKey).Result()
	if err != nil {
		return "", fmt.Errorf("group info: %w", err)
	}
	for _, g := range groups {
		if g.Name == group {
			return g.LastDeliveredID, nil
		}
	}
	return "", fmt.Errorf("group %q does not exist on %q", group, streamKey)
}
