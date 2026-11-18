package observability_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

func TestGinConfig_CopiesTheIdentityArgumentsAndTheLogger(t *testing.T) {
	cases := []struct {
		name         string
		serviceName  string
		buildVersion string
		environment  string
	}{
		{name: "release identity", serviceName: "workflow-execution-service", buildVersion: "v1.4.2", environment: "prod"},
		{name: "worker identity", serviceName: "workflow-execution-worker", buildVersion: "dev-abc123", environment: "staging"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			log := &w9bRecordingLogger{}

			cfg := observability.GinConfig(tc.serviceName, tc.buildVersion, tc.environment, log)

			assert.Equal(t, tc.serviceName, cfg.ServiceName)
			assert.Equal(t, tc.buildVersion, cfg.BuildVersion)
			assert.Equal(t, tc.environment, cfg.Environment)
			assert.Equal(t, "workflow", cfg.Domain)
			assert.Same(t, log, cfg.Logger, "the logger passed in is the one the gin stack gets")
		})
	}
}
