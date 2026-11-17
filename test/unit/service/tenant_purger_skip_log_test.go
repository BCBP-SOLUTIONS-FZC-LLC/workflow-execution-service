package service_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

type infoRecordingLogger struct{ info []string }

func (l *infoRecordingLogger) Debug(string, map[string]any) {}
func (l *infoRecordingLogger) Info(msg string, _ map[string]any) {
	l.info = append(l.info, msg)
}
func (l *infoRecordingLogger) Warn(string, map[string]any)  {}
func (l *infoRecordingLogger) Error(string, map[string]any) {}

func TestTenantPurger_ASkippedTenantIsLoggedWithBothPossibleReasons(t *testing.T) {
	tenant := uuid.New()
	r := &fakeRetention{due: dueTenants(tenant)}
	d := &fakeDataPurger{errFor: map[uuid.UUID]error{tenant: port.ErrTenantRetentionChanged}}
	logger := &infoRecordingLogger{}
	purger := service.NewTenantPurger(service.TenantPurgerDeps{
		Retention: r, Data: d, Batch: 50, Interval: time.Hour,
		Record: func(string, int64) {}, Failed: func() {}, Logger: logger,
	})

	require.NoError(t, purger.Tick(context.Background()))

	require.Len(t, logger.info, 1)
	assert.Equal(t, "tenant purge skipped: its record changed or another replica is purging it", logger.info[0])
}
