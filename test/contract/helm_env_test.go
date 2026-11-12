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
		"DEFINITION_SERVICE_URL",
		"ELIGIBILITY_BASE_URL", "TOKEN_SERVICE_BASE_URL", "ENTITLEMENTS_BASE_URL", "ENTITLEMENTS_CLIENT_TIMEOUT",
		"ENTITLEMENTS_CACHE_TTL", "ENTITLEMENTS_FRESHNESS_BOUND", "SNS_TOPIC_ARN", "GLUE_REGISTRY_NAME", "AWS_USE_STUB",
		"START_REFUSED_STATUSES", "TENANT_PURGE_ENABLED", "TENANT_HISTORY_RETENTION", "LOCK_DATABASE_URL",
		"PG_MAX_CONNS", "PG_BOUNCER_MODE", "GOMEMLIMIT",
	},
	"migrate-job.yaml": {"DATABASE_URL", "MIGRATION_DATABASE_URL", "PG_BOUNCER_MODE"},
	"deployment-worker.yaml": {
		"APP_ENV", "BUILD_VERSION", "OTEL_SERVICE_NAME", "OTEL_EXPORTER_OTLP_ENDPOINT",
		"DATABASE_URL", "TEMPORAL_HOST_PORT", "DEFINITION_SERVICE_URL",
		"PG_MAX_CONNS", "PG_BOUNCER_MODE", "QUEUE_TOPOLOGY_POLL_INTERVAL", "WORKFLOW_CACHE_SIZE", "GOMEMLIMIT",
	},
	"deployment-connector-worker.yaml": {
		"APP_ENV", "BUILD_VERSION", "OTEL_SERVICE_NAME", "OTEL_EXPORTER_OTLP_ENDPOINT",
		"VALKEY_ADDR", "DEFINITION_SERVICE_URL",
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
	for _, key := range []string{"DATABASE_URL", "TEMPORAL_HOST_PORT", "OUTBOX_RELAY_DATABASE_URL", "SNS_TOPIC_ARN", "OPENBAO_TOKEN"} {
		args = append(args, "--set-string", "secret.secretValues."+key+"=placeholder")
	}
	args = append(args, "--set-string", "env.eligibilityBaseURL=http://eligibility.invalid", "--set-string", "env.tokenServiceBaseURL=http://token-service.invalid")

	out, err := exec.Command(helm, args...).CombinedOutput()

	require.Error(t, err, "a Secret without VALKEY_ADDR renders a pod that cannot start")
	assert.Contains(t, string(out), "VALKEY_ADDR")
}

func helmRender(t *testing.T, extra ...string) (string, error) {
	t.Helper()
	return helmRenderAs(t, "x", extra...)
}

func helmRenderAs(t *testing.T, release string, extra ...string) (string, error) {
	t.Helper()
	helm, err := exec.LookPath("helm")
	if err != nil {
		t.Skip("helm is not installed")
	}
	args := []string{"template", release, "../../deploy/helm"}
	for _, key := range []string{"DATABASE_URL", "TEMPORAL_HOST_PORT", "VALKEY_ADDR", "OUTBOX_RELAY_DATABASE_URL", "SNS_TOPIC_ARN", "OPENBAO_TOKEN"} {
		args = append(args, "--set-string", "secret.secretValues."+key+"=placeholder")
	}
	args = append(args, "--set-string", "env.eligibilityBaseURL=http://eligibility.invalid", "--set-string", "env.tokenServiceBaseURL=http://token-service.invalid")
	args = append(args, extra...)
	out, err := exec.Command(helm, args...).CombinedOutput()
	return string(out), err
}

func TestHelmSecret_BehindAPoolerItNeedsTheDirectLockAndMigrationAddresses(t *testing.T) {
	out, err := helmRender(t, "--set-string", "env.pgBouncerMode=true")
	require.Error(t, err, "a pooled deployment without direct addresses would take its session locks through the pooler")
	assert.Contains(t, out, "LOCK_DATABASE_URL")

	out, err = helmRender(t, "--set-string", "env.pgBouncerMode=true", "--set-string", "secret.secretValues.LOCK_DATABASE_URL=postgres://direct")
	require.Error(t, err)
	assert.Contains(t, out, "MIGRATION_DATABASE_URL")

	_, err = helmRender(t, "--set-string", "env.pgBouncerMode=true",
		"--set-string", "secret.secretValues.LOCK_DATABASE_URL=postgres://direct",
		"--set-string", "secret.secretValues.MIGRATION_DATABASE_URL=postgres://direct")
	require.NoError(t, err)
}

func TestHelmDeployments_AGoMemLimitMustCarryAUnit(t *testing.T) {
	out, err := helmRender(t, "--set-string", "worker.goMemLimit=200")
	require.Error(t, err, "GOMEMLIMIT=200 means 200 bytes")
	assert.Contains(t, out, "goMemLimit")

	_, err = helmRender(t, "--set-string", "worker.goMemLimit=200MiB", "--set-string", "api.goMemLimit=1GiB")
	require.NoError(t, err)
}

func TestHelmPurgeAlert_LooksBackOverTheSweepIntervalPlusTwoHours(t *testing.T) {
	out, err := helmRender(t, "-s", "templates/prometheusrule.yaml", "--set", "serviceMonitor.enabled=true", "--set-string", "env.retentionSweepInterval=48h")
	require.NoError(t, err, out)
	assert.Contains(t, out, "tenant_purge_failures_total[50h]")

	out, err = helmRender(t, "-s", "templates/deployment-api.yaml", "--set-string", "env.retentionSweepInterval=48h")
	require.NoError(t, err, out)
	assert.Regexp(t, `name: RETENTION_SWEEP_INTERVAL\s+value: "48h"`, out)

	_, err = helmRender(t, "-s", "templates/prometheusrule.yaml", "--set", "serviceMonitor.enabled=true", "--set-string", "env.retentionSweepInterval=90m")
	require.Error(t, err, "the alert window is set in whole hours")
}

func TestHelmEntitlements_TheSettingsReachTheAPIPod(t *testing.T) {
	out, err := helmRender(t, "-s", "templates/deployment-api.yaml",
		"--set-string", "env.entitlementsBaseURL=http://entitlements.commercial:8080",
		"--set-string", "env.entitlementsCacheTTL=30s")
	require.NoError(t, err, out)
	assert.Regexp(t, `name: ENTITLEMENTS_BASE_URL\s+value: "http://entitlements.commercial:8080"`, out)
	assert.Regexp(t, `name: ENTITLEMENTS_CACHE_TTL\s+value: "30s"`, out)
	assert.Regexp(t, `name: ENTITLEMENTS_CLIENT_TIMEOUT\s+value: "5s"`, out)
	assert.Regexp(t, `name: ENTITLEMENTS_FRESHNESS_BOUND\s+value: "10m"`, out)
}

func TestHelmConnectorWorker_HasNoAliasCallToken(t *testing.T) {
	out, err := helmRender(t, "-s", "templates/deployment-connector-worker.yaml", "--set", "connectorWorker.enabled=true", "--set-string", "connectorWorker.docsBucket=docs")
	require.NoError(t, err, out)
	assert.NotContains(t, out, "CONNECTOR_ALIAS_CALL_TOKEN")
}
