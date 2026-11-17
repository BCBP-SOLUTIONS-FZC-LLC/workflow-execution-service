package main

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
)

type relayRoleLogger struct {
	silentLogger
	mu    sync.Mutex
	warns []string
}

func (l *relayRoleLogger) Warn(msg string, _ map[string]any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warns = append(l.warns, msg)
}

func bypassReader(bypasses bool, err error) func(context.Context) (bool, error) {
	return func(context.Context) (bool, error) { return bypasses, err }
}

func TestCheckRelayRole_ARoleThatBypassesRLSPasses(t *testing.T) {
	log := &relayRoleLogger{}

	err := checkRelayRole(context.Background(), bypassReader(true, nil), &config.Config{AppEnv: "prod"}, log)

	require.NoError(t, err)
	assert.Empty(t, log.warns)
}

func TestCheckRelayRole_ARoleThatCannotBypassRLSRefusesTheStartOutsideDev(t *testing.T) {
	for _, env := range []string{"test", "staging", "prod"} {
		t.Run(env, func(t *testing.T) {
			err := checkRelayRole(context.Background(), bypassReader(false, nil), &config.Config{AppEnv: env}, &relayRoleLogger{})

			require.Error(t, err)
			assert.True(t, strings.Contains(err.Error(), "OUTBOX_RELAY_DATABASE_URL") && strings.Contains(err.Error(), "BYPASSRLS"), "error = %v", err)
		})
	}
}

func TestCheckRelayRole_ARoleThatCannotBypassRLSOnlyWarnsInDev(t *testing.T) {
	for _, env := range []string{"dev", "local"} {
		t.Run(env, func(t *testing.T) {
			log := &relayRoleLogger{}

			err := checkRelayRole(context.Background(), bypassReader(false, nil), &config.Config{AppEnv: env}, log)

			require.NoError(t, err)
			assert.Len(t, log.warns, 1)
		})
	}
}

func TestCheckRelayRole_AFailedLookupRefusesTheStart(t *testing.T) {
	err := checkRelayRole(context.Background(), bypassReader(false, errors.New("connection reset")), &config.Config{AppEnv: "dev"}, &relayRoleLogger{})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection reset")
}
