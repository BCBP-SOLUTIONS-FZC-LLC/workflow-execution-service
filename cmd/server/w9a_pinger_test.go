package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/client"
)

var errW9aHealth = errors.New("temporal frontend unavailable")

// w9aHealthClient answers CheckHealth only; every other client method panics through the nil embedded interface.
type w9aHealthClient struct {
	client.Client
	result   error
	blockOn  bool
	gotCtx   context.Context
	gotReq   *client.CheckHealthRequest
	numCalls int
}

func (c *w9aHealthClient) CheckHealth(ctx context.Context, req *client.CheckHealthRequest) (*client.CheckHealthResponse, error) {
	c.numCalls++
	c.gotCtx, c.gotReq = ctx, req
	if c.blockOn {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if c.result != nil {
		return nil, c.result
	}
	return &client.CheckHealthResponse{}, nil
}

func TestTemporalPingerPing_ReportsTheClientsHealthCheck(t *testing.T) {
	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	cases := []struct {
		name       string
		fake       *w9aHealthClient
		ctx        context.Context
		wantErr    error
		wantPrefix string
	}{
		{name: "a healthy client returns nil", fake: &w9aHealthClient{}, ctx: context.Background()},
		{name: "a health error is wrapped and stays matchable", fake: &w9aHealthClient{result: errW9aHealth}, ctx: context.Background(), wantErr: errW9aHealth, wantPrefix: "temporal check health: "},
		{name: "a check that blocks ends with the context deadline", fake: &w9aHealthClient{blockOn: true}, ctx: expired, wantErr: context.DeadlineExceeded, wantPrefix: "temporal check health: "},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := temporalPinger{sdk: tc.fake}.Ping(tc.ctx)

			assert.Equal(t, 1, tc.fake.numCalls)
			assert.Equal(t, tc.ctx, tc.fake.gotCtx, "the caller's context reaches the check")
			require.NotNil(t, tc.fake.gotReq)
			if tc.wantErr == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, tc.wantErr)
			assert.True(t, strings.HasPrefix(err.Error(), tc.wantPrefix), "error = %v", err)
		})
	}
}
