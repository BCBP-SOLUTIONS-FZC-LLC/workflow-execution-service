package config

import (
	"strings"
	"testing"
)

func TestRequire_RefusesADefinitionServiceURLThatIsNotAbsolute(t *testing.T) {
	requires := map[string]func(*Config) error{
		"server":           (*Config).RequireServer,
		"worker":           (*Config).RequireWorker,
		"connector-worker": (*Config).RequireConnectorWorker,
	}
	for name, require := range requires {
		for _, bad := range []string{"definition:8080", "definition-without-scheme", "ftp://definition", "http://", "://x"} {
			t.Run(name+" "+bad, func(t *testing.T) {
				cfg := w19aLaunchConfig("dev")
				cfg.TokenServiceBaseURL = "http://token-service"
				cfg.AWSUseStub = true
				cfg.DefinitionServiceURL = bad

				err := require(cfg)

				if err == nil || !strings.Contains(err.Error(), "DEFINITION_SERVICE_URL") {
					t.Fatalf("error = %v, want one naming DEFINITION_SERVICE_URL", err)
				}
			})
		}
	}
}

func TestLoad_RefusesAZeroGlueSchemaCacheTTL(t *testing.T) {
	for _, value := range []string{"0s", "-1s"} {
		err := cfgfixLoadWith(t, "GLUE_SCHEMA_CACHE_TTL", value)
		if err == nil || !strings.Contains(err.Error(), "GLUE_SCHEMA_CACHE_TTL") {
			t.Fatalf("Load() error = %v, want one naming GLUE_SCHEMA_CACHE_TTL", err)
		}
	}
}
