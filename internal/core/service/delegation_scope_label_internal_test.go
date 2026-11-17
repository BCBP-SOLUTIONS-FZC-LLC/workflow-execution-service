package service

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/logger"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

type scopeLabelLogger struct{}

func (scopeLabelLogger) Debug(string, map[string]any) {}
func (scopeLabelLogger) Info(string, map[string]any)  {}
func (scopeLabelLogger) Warn(string, map[string]any)  {}
func (scopeLabelLogger) Error(string, map[string]any) {}

func registerMetricsForScopeLabelTest(t *testing.T) {
	t.Helper()
	log, err := logger.NewLogger("dev")
	require.NoError(t, err)
	_ = gincommon.ObservabilityMiddlewares(gincommon.Config{
		Logger:       log,
		ServiceName:  "workflow-execution-service",
		BuildVersion: "test",
		Domain:       observability.Domain,
		Environment:  "test",
	})
	observability.Register()
}

func TestScopeMatches_AnUnhandledScopeIsCountedUnderOneBoundedLabel(t *testing.T) {
	registerMetricsForScopeLabelTest(t)
	candidate := delegationCandidate{task: &domain.Task{ID: uuid.New()}, instance: &domain.Instance{}}
	unknown := testutil.ToFloat64(observability.DelegationScopeUnhandledTotal.WithLabelValues("unknown"))

	for _, scope := range []string{"region-" + uuid.NewString(), "region-" + uuid.NewString(), ""} {
		assert.False(t, scopeMatches(context.Background(), scopeLabelLogger{}, nil, uuid.New(), candidate, scope, nil))
	}

	assert.Equal(t, unknown+3, testutil.ToFloat64(observability.DelegationScopeUnhandledTotal.WithLabelValues("unknown")))
	assert.Equal(t, 1, testutil.CollectAndCount(observability.DelegationScopeUnhandledTotal), "no series is created per scope value")
}
