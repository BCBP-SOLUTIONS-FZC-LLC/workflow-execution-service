package main

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/client"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	httpadapter "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http"
)

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
