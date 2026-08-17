package main

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/redis/go-redis/v9"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events/mock"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/glue"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkey"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// newAppPool builds the RLS-enforced execution_app pool every ordinary
// tenant-scoped repo call runs against.
func newAppPool(ctx context.Context, cfg *config.Config) (*pgcommon.Pool, error) {
	pool, err := pgcommon.NewPool(ctx, pgcommon.Config{
		DSN:                cfg.DatabaseURL,
		MaxConns:           cfg.PGMaxConns,
		MinConns:           cfg.PGMinConns,
		PGBouncerMode:      cfg.PGBouncerMode,
		SlowQueryThreshold: time.Duration(cfg.PGSlowQueryThresholdMS) * time.Millisecond,
		GUCProvider:        pgcommon.GUCSetFromContext,
	})
	if err != nil {
		return nil, fmt.Errorf("app db pool: %w", err)
	}
	return pool, nil
}

// newRelayPool builds the BYPASSRLS execution_outbox_relay pool the outbox
// relay uses to scan every tenant's unpublished rows in one query. No
// GUCProvider: the relay role bypasses RLS, so a GUC would be meaningless.
func newRelayPool(ctx context.Context, cfg *config.Config) (*pgcommon.Pool, error) {
	pool, err := pgcommon.NewPool(ctx, pgcommon.Config{
		DSN:           cfg.OutboxRelayDSN(),
		MaxConns:      cfg.PGMaxConns,
		MinConns:      cfg.PGMinConns,
		PGBouncerMode: cfg.PGBouncerMode,
	})
	if err != nil {
		return nil, fmt.Errorf("outbox relay db pool: %w", err)
	}
	return pool, nil
}

func newCacheStore(ctx context.Context, cfg *config.Config) (port.CacheStore, *redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:         cfg.ValkeyAddr,
		Password:     cfg.ValkeyPassword,
		DialTimeout:  cfg.ValkeyDialTimeout,
		ReadTimeout:  cfg.ValkeyReadTimeout,
		WriteTimeout: cfg.ValkeyWriteTimeout,
	})
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, nil, fmt.Errorf("valkey ping: %w", err)
	}
	return valkey.NewCache(client), client, nil
}

func newGlueCodec(ctx context.Context, cfg *config.Config) (*glue.Codec, error) {
	var awsCfg aws.Config
	var err error
	if !cfg.AWSUseStub {
		awsCfg, err = awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.AWSRegion))
		if err != nil {
			return nil, fmt.Errorf("load aws config for glue: %w", err)
		}
	}
	return glue.NewCodec(awsCfg, cfg.GlueRegistryName, cfg.AWSUseStub, cfg.AWSEndpointURL, cfg.GlueSchemaCacheTTL), nil
}

func newPublisher(cfg *config.Config, log port.Logger, codec events.Codec) (events.Publisher, error) {
	if cfg.AWSUseStub {
		return &mock.Publisher{}, nil
	}
	pub, err := events.NewSNSPublisher(events.SNSConfig{
		TopicARN:    cfg.SNSTopicARN,
		Region:      cfg.AWSRegion,
		EndpointURL: cfg.AWSEndpointURL,
		Logger:      log,
	}, events.WithCodec(codec))
	if err != nil {
		return nil, fmt.Errorf("sns publisher: %w", err)
	}
	return pub, nil
}
