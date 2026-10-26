package contract_test

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHelmNetworkPolicy_TheAPIReachesOrgMembershipAndTokenServiceInTheIAMNamespace(t *testing.T) {
	out, err := helmRender(t, "-s", "templates/networkpolicy-api.yaml")
	require.NoError(t, err, out)

	for name, port := range map[string]string{"iam-org-membership": "8080", "iam-token-service": "8080"} {
		rule := regexp.MustCompile(`(?s)- to:\s+- podSelector:\s+matchLabels:\s+app\.kubernetes\.io/name: ` + name +
			`\s+namespaceSelector:\s+matchLabels:\s+kubernetes\.io/metadata\.name: iam\s+ports:\s+- port: ` + port + `\s`)
		assert.Regexp(t, rule, out, "the API's NetworkPolicy must allow %s in the iam namespace on %s", name, port)
	}
}

func TestHelmSecurityPolicy_MergesWithTheGatewayPolicy(t *testing.T) {
	out, err := helmRender(t, "-s", "templates/securitypolicy.yaml",
		"--set", "ingress.enabled=true", "--set", "ingress.gateway.name=gw",
		"--set", "ingress.securityPolicy.enabled=true", "--set", "ingress.securityPolicy.cors.enabled=true",
		"--set", "ingress.securityPolicy.cors.allowOrigins[0]=https://app.example.com")
	require.NoError(t, err, out)

	assert.Contains(t, out, "mergeType: StrategicMerge", "a route policy without mergeType replaces the gateway's JWT and authorisation filter")
}

func TestHelmNetworkPolicy_ScrapeAndGatewayUseThePlatformNamespaces(t *testing.T) {
	for _, tmpl := range []string{"networkpolicy-api.yaml", "networkpolicy-worker.yaml", "networkpolicy-connector-worker.yaml"} {
		out, err := helmRender(t, "-s", "templates/"+tmpl, "--set", "connectorWorker.enabled=true", "--set-string", "connectorWorker.docsBucket=docs", "--set-string", "connectorWorker.openBaoAddr=http://openbao.invalid:8200")
		require.NoError(t, err, out)

		assert.Regexp(t, `kubernetes\.io/metadata\.name: observability\n`, out, "%s: Prometheus runs in the observability namespace", tmpl)
		assert.NotRegexp(t, `kubernetes\.io/metadata\.name: monitoring\n`, out, "%s: the monitoring namespace was renamed observability", tmpl)
	}
	out, err := helmRender(t, "-s", "templates/networkpolicy-api.yaml")
	require.NoError(t, err, out)
	assert.Regexp(t, `kubernetes\.io/metadata\.name: gateway\n`, out, "Envoy Gateway's data plane runs in the gateway namespace")
}
