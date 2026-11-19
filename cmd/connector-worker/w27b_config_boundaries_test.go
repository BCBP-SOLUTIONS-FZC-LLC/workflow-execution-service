package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckRequiredConfig_MaxDeliveriesBoundary(t *testing.T) {
	cases := []struct {
		name       string
		deliveries int64
		wantErr    string
	}{
		{name: "negative is refused", deliveries: -1, wantErr: "CONNECTOR_STREAM_MAX_DELIVERIES must be >= 1"},
		{name: "zero is refused", deliveries: 0, wantErr: "CONNECTOR_STREAM_MAX_DELIVERIES must be >= 1"},
		{name: "one is accepted", deliveries: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validConnectorWorkerConfig()
			cfg.ConnectorStreamMaxDeliveries = tc.deliveries

			err := checkRequiredConfig(cfg)

			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Equal(t, tc.wantErr, err.Error())
		})
	}
}

func TestCheckRequiredConfig_ClaimMinIdleBoundary(t *testing.T) {
	const wantMsg = "CONNECTOR_STREAM_CLAIM_MIN_IDLE must be at least 3s, the heartbeat runs three times within it"
	cases := []struct {
		name    string
		idle    time.Duration
		wantErr bool
	}{
		{name: "negative is refused", idle: -time.Second, wantErr: true},
		{name: "zero is refused", idle: 0, wantErr: true},
		{name: "two seconds is refused", idle: 2 * time.Second, wantErr: true},
		{name: "one nanosecond under three seconds is refused", idle: 3*time.Second - time.Nanosecond, wantErr: true},
		{name: "exactly three seconds is accepted", idle: 3 * time.Second},
		{name: "one nanosecond over three seconds is accepted", idle: 3*time.Second + time.Nanosecond},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validConnectorWorkerConfig()
			cfg.ConnectorStreamClaimMinIdle = tc.idle

			err := checkRequiredConfig(cfg)

			if !tc.wantErr {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Equal(t, wantMsg, err.Error())
		})
	}
}

// 2 x deliveries x idle must stay strictly under the 24h outcome record.
func TestCheckRequiredConfig_OutcomeRecordLifetimeEdge(t *testing.T) {
	cases := []struct {
		name       string
		deliveries int64
		idle       time.Duration
		wantErr    bool
	}{
		{name: "product exactly 24h is refused", deliveries: 1, idle: 12 * time.Hour, wantErr: true},
		{name: "product two nanoseconds under 24h is accepted", deliveries: 1, idle: 12*time.Hour - time.Nanosecond},
		{name: "product above 24h is refused", deliveries: 1, idle: 12*time.Hour + time.Nanosecond, wantErr: true},
		{name: "many deliveries reaching 24h exactly are refused", deliveries: 480, idle: 90 * time.Second, wantErr: true},
		{name: "many deliveries just under 24h are accepted", deliveries: 479, idle: 90 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validConnectorWorkerConfig()
			cfg.ConnectorStreamMaxDeliveries = tc.deliveries
			cfg.ConnectorStreamClaimMinIdle = tc.idle

			err := checkRequiredConfig(cfg)

			if !tc.wantErr {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Equal(t, "2 x CONNECTOR_STREAM_MAX_DELIVERIES x CONNECTOR_STREAM_CLAIM_MIN_IDLE must stay under the 24h0m0s outcome record", err.Error())
		})
	}
}
