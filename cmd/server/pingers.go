package main

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/client"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"

	httpadapter "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http"
)

// dbPinger and temporalPinger adapt this binary's concrete infra clients to
// httpadapter's Pinger/DBPinger interfaces — the composition root's job, per
// that package's own router.go doc comment, so it never imports pgcommon or
// the Temporal SDK directly.

type dbPinger struct{ pool *pgcommon.Pool }

func (d dbPinger) Health(ctx context.Context) httpadapter.DBHealth {
	hs := d.pool.Health(ctx)
	return httpadapter.DBHealth{
		Healthy:       hs.Healthy,
		Utilization:   hs.Utilization,
		AcquiredConns: hs.AcquiredConns,
		MaxConns:      hs.MaxConns,
	}
}

type temporalPinger struct{ sdk client.Client }

func (t temporalPinger) Ping(ctx context.Context) error {
	if _, err := t.sdk.CheckHealth(ctx, &client.CheckHealthRequest{}); err != nil {
		return fmt.Errorf("temporal check health: %w", err)
	}
	return nil
}
