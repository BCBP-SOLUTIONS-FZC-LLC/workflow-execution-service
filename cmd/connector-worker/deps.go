package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/openbao"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkeystream"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/pkg/connectors"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/pkg/connectors/aliasconfig"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/pkg/registry"
)

// deps is cmd/connector-worker's own held composition-root state. There is
// no shared port.XxxDeps struct to build against here (unlike cmd/worker's
// outboundtemporal.Deps) — this binary's dependency shape (Stream consumer,
// OpenBao reader, per-connector-type dispatch pools) is unique to it and has
// no Temporal Activity counterpart.
type deps struct {
	valkeyClient *redis.Client
	consumer     *valkeystream.Consumer
	openbao      *openbao.Reader
	aliases      aliasconfig.Config
	pools        map[string]*typePool
	completion   *completionClient

	streamKey    string
	streamGroup  string
	consumerName string
	blockTimeout time.Duration
	claimMinIdle time.Duration
	batchSize    int64
}

// buildDeps is cmd/connector-worker's composition root (mirrors cmd/worker's
// buildDeps/cmd/server's newApp convention). It never dials Temporal — this
// binary reaches the workflow only through completionClient's HTTP calls
// into execution_service's own /internal/connector-tasks endpoints (LLD
// workflow_connectors.md §6.1 Decision #2).
func buildDeps(cfg *config.Config) (*deps, func(), error) {
	aliases, err := fetchAliases(context.Background(), cfg.DefinitionServiceInternalHTTPAddr, cfg.InternalAPIToken, cfg.ConnectorAliasFetchTimeout)
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

	// TODO: wire Config.StorageProviders/SendEmailProviders once workflow-connectors
	// is pushed and go.mod is bumped past its pkg/connectors reorg (provider
	// constructors moved out of pkg/connectors into their own subpackages) —
	// that version predates storage.ProviderConstructor/storage.NewGocloudStorageProvider/
	// storage.NewDriveStorageProvider and sendemail.ProviderConstructor/
	// sendemail.NewSendGridProvider/sendemail.NewSESProvider/
	// sendemail.NewGraphSendMailProvider/sendemail.NewGmailProvider.
	connectorSet, err := connectors.New(connectors.Config{
		Aliases:       aliases,
		InternalToken: cfg.InternalAPIToken,
	})
	if err != nil {
		_ = valkeyClient.Close()
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
		openbao:      openbao.NewReader(cfg.OpenBaoAddr, cfg.OpenBaoToken, cfg.OpenBaoMount, cfg.OpenBaoTimeout),
		aliases:      aliases,
		pools:        buildPools(cfg, connectorSet),
		completion:   newCompletionClient(cfg.ExecutionServiceInternalAddr, cfg.InternalAPIToken, cfg.ConnectorCompletionTimeout),
		streamKey:    cfg.ConnectorStreamKey,
		streamGroup:  cfg.ConnectorStreamGroup,
		consumerName: consumerName,
		blockTimeout: cfg.ConnectorStreamBlockTimeout,
		claimMinIdle: cfg.ConnectorStreamClaimMinIdle,
		batchSize:    cfg.ConnectorStreamBatchSize,
	}

	cleanup := func() { _ = valkeyClient.Close() }
	return d, cleanup, nil
}

// typePool is one connector type's bounded dispatch pool (LLD §6.5 step 2):
// sem gates concurrency, timeout covers total dispatch time including
// retries/backoff, retry is the ratified per-type policy (Decision #6).
type typePool struct {
	connectorType string
	sem           chan struct{}
	timeout       time.Duration
	connector     connectors.Connector
	retry         registry.RetryPolicy
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
	defs := registry.All()

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
			retry:         defs[typ].Retry,
		}
	}
	return pools
}
