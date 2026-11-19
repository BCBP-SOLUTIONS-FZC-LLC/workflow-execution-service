package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/connectors/docref/valkeystore"
)

func TestBuildDocRefs_BucketSetAndValkeyFailsTheDurabilityCheck_RefusesWithNoRefs(t *testing.T) {
	cases := []struct {
		name string
		addr string
	}{
		{name: "valkey refuses connections", addr: "127.0.0.1:1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := redis.NewClient(&redis.Options{Addr: tc.addr, DialTimeout: time.Second, MaxRetries: -1})
			t.Cleanup(func() { _ = client.Close() })

			refs, err := buildDocRefs(context.Background(), &config.Config{ConnectorDocsBucket: "docs-bucket"}, client)

			require.Error(t, err)
			assert.Nil(t, refs)
			assert.Contains(t, err.Error(), "document refs: ")
			var durability *valkeystore.DurabilityError
			require.True(t, errors.As(err, &durability), "the refusal carries the durability error, got %v", err)
			assert.NotEmpty(t, durability.Problems)
		})
	}
}
