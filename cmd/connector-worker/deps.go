package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/redis/go-redis/v9"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/openbao"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/pglogger"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkey"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkeystream"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/connectors"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/connectors/aliasconfig"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/connectors/docref"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/connectors/docref/s3content"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/connectors/docref/valkeystore"
	documentsql "github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/connectors/documents/sqlstore"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/connectors/sendemail"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/connectors/sendemail/gmail"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/connectors/sendemail/msgraph"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/connectors/sendemail/sendgrid"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/connectors/sendemail/ses"
	intentsql "github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/connectors/sendintent/sqlstore"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/connectors/storage"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/connectors/storage/gocloud"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/connectors/storage/googledrive"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/registry"
)

type streamConsumer interface {
	ReadGroup(ctx context.Context, streamKey, group, consumerName string, count int64, block time.Duration) ([]valkeystream.Entry, error)
	Ack(ctx context.Context, streamKey, group, entryID string) error
	ReclaimStale(ctx context.Context, streamKey, group, consumerName string, minIdle time.Duration, count int64) ([]valkeystream.Entry, error)
	DeadLetter(ctx context.Context, streamKey, group, deadLetterKey string, entry valkeystream.Entry, reason string) error
	Touch(ctx context.Context, streamKey, group, consumerName string, ids []string) error
}

type deps struct {
	valkeyClient *redis.Client
	consumer     streamConsumer
	dedup        port.CacheStore
	openbao      *openbao.Reader
	aliases      aliasconfig.Config
	pools        map[string]*typePool
	completion   *completionClient
	pgPool       *pgcommon.Pool
	documents    *documentsql.Store
	pruner       attemptPruner
	retry        retryBounds

	streamKey    string
	streamGroup  string
	consumerName string
	blockTimeout time.Duration
	claimMinIdle time.Duration
	batchSize    int64

	maxDeliveries int64
	deadLetterKey string
	inflight      inflightSet
	now           func() time.Time
}

func buildDeps(cfg *config.Config, wlog port.Logger) (*deps, func(), error) {
	aliases, err := fetchAliases(context.Background(), cfg.DefinitionServiceURL, cfg.DefinitionServiceToken, cfg.ConnectorAliasFetchTimeout)
	if err != nil {
		return nil, nil, fmt.Errorf("fetch alias registry: %w", err)
	}

	valkeyClient := redis.NewClient(&redis.Options{
		Addr:         cfg.ValkeyAddr,
		Password:     cfg.ValkeyPassword,
		DialTimeout:  cfg.ValkeyDialTimeout,
		ReadTimeout:  cfg.ValkeyReadTimeout,
		WriteTimeout: cfg.ValkeyWriteTimeout,
	})
	pingCtx, cancel := context.WithTimeout(context.Background(), cfg.ValkeyDialTimeout)
	defer cancel()
	if err := valkeyClient.Ping(pingCtx).Err(); err != nil {
		return nil, nil, fmt.Errorf("valkey ping: %w", err)
	}

	consumer := valkeystream.NewConsumer(valkeyClient)
	groupCtx, groupCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer groupCancel()
	if err := consumer.EnsureGroup(groupCtx, cfg.ConnectorStreamKey, cfg.ConnectorStreamGroup); err != nil {
		_ = valkeyClient.Close()
		return nil, nil, fmt.Errorf("ensure consumer group: %w", err)
	}

	pgPool, err := pgcommon.NewPool(context.Background(), pgcommon.Config{
		DSN:                cfg.DatabaseURL,
		MaxConns:           cfg.PGMaxConns,
		MinConns:           cfg.PGMinConns,
		PGBouncerMode:      cfg.PGBouncerMode,
		SlowQueryThreshold: time.Duration(cfg.PGSlowQueryThresholdMS) * time.Millisecond,
		GUCProvider:        pgcommon.GUCSetFromContext,
		Logger:             pglogger.New(wlog),
		Tracer:             gincommon.NewSpanTracer(cfg.OTELServiceName),
	})
	if err != nil {
		_ = valkeyClient.Close()
		return nil, nil, fmt.Errorf("connector postgres pool: %w", err)
	}
	prunePool, err := pgcommon.NewPool(context.Background(), pgcommon.Config{
		DSN:           cfg.OutboxRelayDSN(),
		MaxConns:      2,
		MinConns:      0,
		PGBouncerMode: cfg.PGBouncerMode,
		Logger:        pglogger.New(wlog),
		Tracer:        gincommon.NewSpanTracer(cfg.OTELServiceName),
	})
	if err != nil {
		_ = valkeyClient.Close()
		pgPool.Close()
		return nil, nil, fmt.Errorf("connector prune pool: %w", err)
	}
	closeAll := func() {
		_ = valkeyClient.Close()
		pgPool.Close()
		prunePool.Close()
	}

	docRefs, err := buildDocRefs(context.Background(), cfg, valkeyClient)
	if err != nil {
		closeAll()
		return nil, nil, err
	}
	documents := documentsql.New(pgPool)

	connectorSet, err := connectors.New(connectors.Config{
		Aliases:       aliases,
		InternalToken: cfg.InternalAPIToken,
		StorageProviders: map[string]storage.ProviderConstructor{
			"aws-s3":       gocloud.NewProvider,
			"azure-blob":   gocloud.NewProvider,
			"gcp-gcs":      gocloud.NewProvider,
			"google-drive": googledrive.NewProvider(documents),
		},
		SendEmailProviders: map[string]sendemail.ProviderConstructor{
			"sendgrid":         sendgrid.NewProvider,
			"aws-ses":          ses.NewProvider,
			"microsoft-365":    msgraph.NewProvider,
			"google-workspace": gmail.NewProvider,
		},
		DocRefs:     docRefs,
		SendIntents: intentsql.New(pgPool),
	})
	if err != nil {
		closeAll()
		return nil, nil, fmt.Errorf("build connectors: %w", err)
	}

	consumerName := cfg.ConnectorStreamConsumerName
	if consumerName == "" {
		if host, hostErr := os.Hostname(); hostErr == nil && host != "" {
			consumerName = host
		} else {
			consumerName = "connector-worker"
		}
	}

	d := &deps{
		valkeyClient: valkeyClient,
		consumer:     consumer,
		dedup:        valkey.NewCache(valkeyClient),
		openbao:      openbao.NewReader(cfg.OpenBaoAddr, cfg.OpenBaoToken, cfg.OpenBaoMount, cfg.OpenBaoTimeout),
		aliases:      aliases,
		pools:        buildPools(cfg, connectorSet),
		completion:   newCompletionClient(cfg.ExecutionServiceInternalAddr, cfg.InternalAPIToken, cfg.ConnectorCompletionTimeout),
		pgPool:       pgPool,
		documents:    documents,
		pruner:       documentsql.New(prunePool),
		retry: retryBounds{
			MaxAttempts:    cfg.ConnectorRetryMaxAttempts,
			InitialBackoff: cfg.ConnectorRetryInitialBackoff,
			MaxBackoff:     cfg.ConnectorRetryMaxBackoff,
		},
		streamKey:    cfg.ConnectorStreamKey,
		streamGroup:  cfg.ConnectorStreamGroup,
		consumerName: consumerName,
		blockTimeout: cfg.ConnectorStreamBlockTimeout,
		claimMinIdle: cfg.ConnectorStreamClaimMinIdle,
		batchSize:    cfg.ConnectorStreamBatchSize,

		maxDeliveries: cfg.ConnectorStreamMaxDeliveries,
		deadLetterKey: cfg.ConnectorStreamDeadLetterKey,
	}

	return d, closeAll, nil
}

const docRefPrefix = "docrefs/"

func buildDocRefs(ctx context.Context, cfg *config.Config, valkeyClient *redis.Client) (*docref.Service, error) {
	if cfg.ConnectorDocsBucket == "" {
		return nil, nil
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	content := s3content.New(s3.NewFromConfig(awsCfg), cfg.ConnectorDocsBucket)
	if err := valkeystore.CheckDurability(ctx, valkeyClient); err != nil {
		return nil, fmt.Errorf("document refs: %w", err)
	}
	if err := content.Check(ctx, docRefPrefix); err != nil {
		return nil, fmt.Errorf("document refs: %w", err)
	}
	return docref.NewService(valkeystore.New(valkeyClient, valkeystore.Options{}), content, docref.WithKeyPrefix(docRefPrefix)), nil
}

type typePool struct {
	connectorType string
	sem           chan struct{}
	timeout       time.Duration

	mu        sync.RWMutex
	connector connectors.Connector
}

func (d *deps) clock() time.Time {
	if d.now == nil {
		return time.Now()
	}
	return d.now()
}

func (p *typePool) currentConnector() connectors.Connector {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.connector
}

func (p *typePool) setConnector(c connectors.Connector) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.connector = c
}

func buildPools(cfg *config.Config, connectorSet map[string]connectors.Connector) map[string]*typePool {
	sizes := map[string]int{
		registry.TypeStorage:         cfg.ConnectorPoolSizeStorage,
		registry.TypeSendEmail:       cfg.ConnectorPoolSizeSendEmail,
		registry.TypeDocumentExtract: cfg.ConnectorPoolSizeDocumentExtract,
		registry.TypeRestCall:        cfg.ConnectorPoolSizeRestCall,
		registry.TypeSQLQuery:        cfg.ConnectorPoolSizeSQLQuery,
		registry.TypeChatNotify:      cfg.ConnectorPoolSizeChatNotify,
	}
	timeouts := map[string]time.Duration{
		registry.TypeStorage:         cfg.ConnectorTimeoutStorage,
		registry.TypeSendEmail:       cfg.ConnectorTimeoutSendEmail,
		registry.TypeDocumentExtract: cfg.ConnectorTimeoutDocumentExtract,
		registry.TypeRestCall:        cfg.ConnectorTimeoutRestCall,
		registry.TypeSQLQuery:        cfg.ConnectorTimeoutSQLQuery,
		registry.TypeChatNotify:      cfg.ConnectorTimeoutChatNotify,
	}
	pools := make(map[string]*typePool, len(connectorSet))
	for typ, conn := range connectorSet {
		size := sizes[typ]
		if size <= 0 {
			size = 1
		}
		pools[typ] = &typePool{
			connectorType: typ,
			sem:           make(chan struct{}, size),
			timeout:       timeouts[typ],
			connector:     conn,
		}
	}
	return pools
}
