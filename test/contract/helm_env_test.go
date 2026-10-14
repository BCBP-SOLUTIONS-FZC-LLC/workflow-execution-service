package contract_test

import (
	"os"
	"os/exec"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var envEntry = regexp.MustCompile(`(?m)^\s*- name: ([A-Z][A-Z0-9_]+)\s*$`)

var requiredEnv = map[string][]string{
	"deployment-api.yaml": {
		"APP_ENV", "BUILD_VERSION", "OTEL_SERVICE_NAME", "OTEL_EXPORTER_OTLP_ENDPOINT",
		"DATABASE_URL", "OUTBOX_RELAY_DATABASE_URL", "VALKEY_ADDR", "TEMPORAL_HOST_PORT",
		"DEFINITION_SERVICE_URL", "DEFINITION_SERVICE_TOKEN", "INTERNAL_API_TOKEN",
		"ELIGIBILITY_BASE_URL", "TOKEN_SERVICE_BASE_URL", "SNS_TOPIC_ARN", "GLUE_REGISTRY_NAME", "AWS_USE_STUB",
	},
	"deployment-worker.yaml": {
		"APP_ENV", "BUILD_VERSION", "OTEL_SERVICE_NAME", "OTEL_EXPORTER_OTLP_ENDPOINT",
		"DATABASE_URL", "TEMPORAL_HOST_PORT", "DEFINITION_SERVICE_URL", "DEFINITION_SERVICE_TOKEN",
	},
	"deployment-connector-worker.yaml": {
		"APP_ENV", "BUILD_VERSION", "OTEL_SERVICE_NAME", "OTEL_EXPORTER_OTLP_ENDPOINT",
		"VALKEY_ADDR", "DEFINITION_SERVICE_URL", "DEFINITION_SERVICE_TOKEN", "INTERNAL_API_TOKEN",
		"EXECUTION_SERVICE_INTERNAL_ADDR", "OPENBAO_ADDR", "OPENBAO_TOKEN",
		"CONNECTOR_STREAM_KEY", "CONNECTOR_STREAM_GROUP",
	},
}

func TestHelmDeployments_SetEveryEnvTheirBinaryRequires(t *testing.T) {
	for file, required := range requiredEnv {
		body, err := os.ReadFile("../../deploy/helm/templates/" + file)
		require.NoError(t, err)
		set := map[string]bool{}
		for _, m := range envEntry.FindAllStringSubmatch(string(body), -1) {
			set[m[1]] = true
		}
		for _, name := range required {
			assert.True(t, set[name], "%s must set %s", file, name)
		}
	}
}

func TestHelmSecret_RefusesARenderWithoutTheValkeyAddress(t *testing.T) {
	helm, err := exec.LookPath("helm")
	if err != nil {
		t.Skip("helm is not installed")
	}
	args := []string{"template", "x", "../../deploy/helm"}
	for _, key := range []string{"DATABASE_URL", "TEMPORAL_HOST_PORT", "INTERNAL_API_TOKEN", "DEFINITION_SERVICE_TOKEN", "OUTBOX_RELAY_DATABASE_URL", "SNS_TOPIC_ARN", "OPENBAO_TOKEN"} {
		args = append(args, "--set-string", "secret.secretValues."+key+"=placeholder")
	}
	args = append(args, "--set-string", "env.eligibilityBaseURL=http://eligibility.invalid", "--set-string", "env.tokenServiceBaseURL=http://token-service.invalid")

	out, err := exec.Command(helm, args...).CombinedOutput()

	require.Error(t, err, "a Secret without VALKEY_ADDR renders a pod that cannot start")
	assert.Contains(t, string(out), "VALKEY_ADDR")
}
