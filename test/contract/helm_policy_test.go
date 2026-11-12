package contract_test

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

const (
	authorizationPolicyTemplate = "templates/authorizationpolicy.yaml"
	networkPolicyTemplate       = "templates/networkpolicy-api.yaml"
	releaseNamespace            = "team-a"
)

var connectorWorkerOn = []string{
	"--set", "connectorWorker.enabled=true",
	"--set-string", "connectorWorker.docsBucket=docs",
	"--set-string", "connectorWorker.openBaoAddr=http://openbao.invalid:8200",
}

type chartObject struct {
	Kind     string `yaml:"kind"`
	Metadata struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Spec struct {
		Ports []struct {
			Name        string `yaml:"name"`
			Port        int    `yaml:"port"`
			AppProtocol string `yaml:"appProtocol"`
		} `yaml:"ports"`
	} `yaml:"spec"`
}

type policyRule struct {
	From []struct {
		Source struct {
			Principals []string `yaml:"principals"`
		} `yaml:"source"`
	} `yaml:"from"`
	To []struct {
		Operation struct {
			Paths    []string `yaml:"paths"`
			NotPaths []string `yaml:"notPaths"`
		} `yaml:"operation"`
	} `yaml:"to"`
}

type authorizationPolicy struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Metadata   struct {
		Name string `yaml:"name"`
	} `yaml:"metadata"`
	Spec struct {
		Selector struct {
			MatchLabels map[string]string `yaml:"matchLabels"`
		} `yaml:"selector"`
		Action string       `yaml:"action"`
		Rules  []policyRule `yaml:"rules"`
	} `yaml:"spec"`
}

type labelSelector struct {
	MatchLabels map[string]string `yaml:"matchLabels"`
}

type peer struct {
	PodSelector       *labelSelector `yaml:"podSelector"`
	NamespaceSelector *labelSelector `yaml:"namespaceSelector"`
}

type netRule struct {
	From  []peer `yaml:"from"`
	To    []peer `yaml:"to"`
	Ports []struct {
		Port int `yaml:"port"`
	} `yaml:"ports"`
}

type networkPolicy struct {
	Spec struct {
		Ingress []netRule `yaml:"ingress"`
		Egress  []netRule `yaml:"egress"`
	} `yaml:"spec"`
}

// decodeAll decodes every document of a render, rejecting keys the target type does not name.
func decodeAll[T any](t *testing.T, out string) []T {
	t.Helper()
	dec := yaml.NewDecoder(strings.NewReader(out))
	var docs []T
	for {
		var doc T
		err := dec.Decode(&doc)
		if errors.Is(err, io.EOF) {
			return docs
		}
		require.NoError(t, err, out)
		docs = append(docs, doc)
	}
}

func renderPolicy(t *testing.T, release string, extra ...string) authorizationPolicy {
	t.Helper()
	out, err := helmRenderAs(t, release, append([]string{"-s", authorizationPolicyTemplate}, extra...)...)
	require.NoError(t, err, out)
	docs := decodeAll[authorizationPolicy](t, out)
	require.Len(t, docs, 1)
	return docs[0]
}

func renderNetworkPolicy(t *testing.T, release string, extra ...string) networkPolicy {
	t.Helper()
	out, err := helmRenderAs(t, release, append([]string{"-s", networkPolicyTemplate}, extra...)...)
	require.NoError(t, err, out)
	docs := decodeAll[networkPolicy](t, out)
	require.Len(t, docs, 1)
	return docs[0]
}

func writeValues(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "values.yaml")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

func principal(namespace, serviceAccount string) string {
	return "cluster.local/ns/" + namespace + "/sa/" + serviceAccount
}

// wantRule is one ALLOW rule on an internal route group: its callers and its paths together.
type wantRule struct {
	principals []string
	paths      []string
}

func defaultInternalRules(connectorNamespace string, connectorOn bool) []wantRule {
	rules := []wantRule{
		{[]string{principal("workflow-app", "workflow-events-consumer")}, []string{"/api/v1/internal/events/*"}},
		{[]string{principal("workflow-app", "workflow-definition-service")}, []string{"/api/v1/internal/users/*", "*/active-instances"}},
		{[]string{principal("iam", "iam-org-membership")}, []string{
			"/api/v1/internal/workflows/delegate-impact",
			"/api/v1/internal/workflows/reassign-delegate",
			"/api/v1/internal/workflows/cancel-by-delegate",
		}},
	}
	if connectorOn {
		rules = append(rules, wantRule{
			[]string{principal(connectorNamespace, "workflow-execution-service-connector-worker")},
			[]string{"/api/v1/internal/connector-tasks/*"},
		})
	}
	return rules
}

// policyProblems compares a decoded policy with the rules it must have: rule 1 admits what is
// outside the internal routes with no source, and every other rule carries one group's principals
// and paths together.
func policyProblems(rules []policyRule, want []wantRule) []string {
	var problems []string
	if len(rules) != len(want)+1 {
		return []string{"rule count differs from the expected groups plus the admit-everything-else rule"}
	}
	first := rules[0]
	if len(first.From) != 0 || len(first.To) != 1 ||
		!equalStrings(first.To[0].Operation.NotPaths, []string{"/api/v1/internal/*"}) || len(first.To[0].Operation.Paths) != 0 {
		problems = append(problems, "rule 1 must be only to.operation.notPaths [/api/v1/internal/*]")
	}
	for i, w := range want {
		r := rules[i+1]
		if len(r.From) != 1 || len(r.To) != 1 {
			problems = append(problems, "an internal rule must hold one from and one to")
			continue
		}
		if !equalStrings(r.From[0].Source.Principals, w.principals) {
			problems = append(problems, "principals differ for the group with paths "+strings.Join(w.paths, ","))
		}
		if !equalStrings(r.To[0].Operation.Paths, w.paths) || len(r.To[0].Operation.NotPaths) != 0 {
			problems = append(problems, "paths differ for the group with paths "+strings.Join(w.paths, ","))
		}
	}
	return problems
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestHelmAuthorizationPolicy_RendersExactlyTheExpectedRules(t *testing.T) {
	p := renderPolicy(t, "wf", append([]string{"--namespace", releaseNamespace}, connectorWorkerOn...)...)

	assert.Equal(t, "security.istio.io/v1", p.APIVersion)
	assert.Equal(t, "AuthorizationPolicy", p.Kind)
	assert.Equal(t, "ALLOW", p.Spec.Action)
	assert.Equal(t, "workflow-execution-service-api", p.Metadata.Name)
	assert.Empty(t, policyProblems(p.Spec.Rules, defaultInternalRules(releaseNamespace, true)))
}

func TestHelmAuthorizationPolicy_RuleOneAdmitsOnlyTheNonInternalPaths(t *testing.T) {
	p := renderPolicy(t, "wf")
	require.NotEmpty(t, p.Spec.Rules)

	assert.Empty(t, p.Spec.Rules[0].From, "rule 1 names no source: the gateway, probes and the scrape must stay admitted")
	require.Len(t, p.Spec.Rules[0].To, 1)
	assert.Equal(t, []string{"/api/v1/internal/*"}, p.Spec.Rules[0].To[0].Operation.NotPaths)
	assert.Empty(t, p.Spec.Rules[0].To[0].Operation.Paths)
}

func TestHelmAuthorizationPolicy_TheConnectorWorkerPrincipalFollowsTheReleaseNamespace(t *testing.T) {
	p := renderPolicy(t, "wf", append([]string{"--namespace", releaseNamespace}, connectorWorkerOn...)...)

	last := p.Spec.Rules[len(p.Spec.Rules)-1]
	require.Len(t, last.From, 1)
	assert.Equal(t, []string{"cluster.local/ns/team-a/sa/workflow-execution-service-connector-worker"}, last.From[0].Source.Principals)
	assert.Equal(t, []string{"/api/v1/internal/connector-tasks/*"}, last.To[0].Operation.Paths)
}

func TestHelmAuthorizationPolicy_WithoutTheConnectorWorkerTheConnectorRouteGroupHasNoRule(t *testing.T) {
	p := renderPolicy(t, "wf")

	assert.Empty(t, policyProblems(p.Spec.Rules, defaultInternalRules(releaseNamespace, false)))
	for _, r := range p.Spec.Rules {
		for _, to := range r.To {
			assert.NotContains(t, to.Operation.Paths, "/api/v1/internal/connector-tasks/*", "a group with no caller fails closed")
		}
	}
}

func TestHelmAuthorizationPolicy_AGroupWithNoCallersRendersNoRule(t *testing.T) {
	values := writeValues(t, `authorizationPolicy:
  internalRoutes:
    - name: events
      paths: ["/api/v1/internal/events/*"]
      callers: []
    - name: workflow-client
      paths: ["/api/v1/internal/workflows/delegate-impact"]
      callers:
        - {namespace: iam, serviceAccount: iam-org-membership}
`)
	p := renderPolicy(t, "wf", "-f", values)

	want := []wantRule{{[]string{principal("iam", "iam-org-membership")}, []string{"/api/v1/internal/workflows/delegate-impact"}}}
	assert.Empty(t, policyProblems(p.Spec.Rules, want), "the events group has no caller, so no rule admits it")
}

func TestHelmAuthorizationPolicy_SelectsTheAPIPodsOnly(t *testing.T) {
	p := renderPolicy(t, "wf")

	assert.Equal(t, map[string]string{
		"app.kubernetes.io/name":      "workflow-execution-service",
		"app.kubernetes.io/instance":  "wf",
		"app.kubernetes.io/component": "api",
	}, p.Spec.Selector.MatchLabels)
}

// The check above must fail on the unsafe shapes it exists to catch.
func TestHelmAuthorizationPolicy_TheRuleCheckCatchesPrincipalsAndPathsSplitAcrossRules(t *testing.T) {
	p := renderPolicy(t, "wf", append([]string{"--namespace", releaseNamespace}, connectorWorkerOn...)...)
	want := defaultInternalRules(releaseNamespace, true)
	require.Empty(t, policyProblems(p.Spec.Rules, want))

	t.Run("principals in one rule, paths in another", func(t *testing.T) {
		original := p.Spec.Rules[1]
		principalsOnly := policyRule{From: original.From}
		pathsOnly := policyRule{To: original.To}
		mutated := append([]policyRule{p.Spec.Rules[0], principalsOnly, pathsOnly}, p.Spec.Rules[2:]...)

		assert.NotEmpty(t, policyProblems(mutated, want))
	})
	t.Run("the internal paths opened to every caller", func(t *testing.T) {
		mutated := append([]policyRule(nil), p.Spec.Rules...)
		open := policyRule{To: mutated[1].To}
		mutated[1] = open

		assert.NotEmpty(t, policyProblems(mutated, want))
	})
	t.Run("a probe path added to the notPaths list", func(t *testing.T) {
		mutated := append([]policyRule(nil), p.Spec.Rules...)
		mutated[0].To = append(mutated[0].To[:0:0], mutated[0].To...)
		mutated[0].To[0].Operation.NotPaths = []string{"/api/v1/internal/*", "/healthz"}

		assert.NotEmpty(t, policyProblems(mutated, want))
	})
	t.Run("the connector principal in the wrong namespace", func(t *testing.T) {
		wrong := append([]wantRule(nil), want...)
		wrong[3] = wantRule{[]string{principal("elsewhere", "workflow-execution-service-connector-worker")}, want[3].paths}

		assert.NotEmpty(t, policyProblems(p.Spec.Rules, wrong))
	})
}

func TestHelmAuthorizationPolicy_IsNotRenderedWhenDisabled(t *testing.T) {
	out, err := helmRender(t, "-s", authorizationPolicyTemplate, "--set", "authorizationPolicy.enabled=false", "--set-string", "env.appEnv=dev")
	require.Error(t, err)
	assert.Contains(t, out, "could not find template "+authorizationPolicyTemplate, "a disabled policy renders nothing, so helm finds no template to show")

	out, err = helmRender(t, "--set", "authorizationPolicy.enabled=false", "--set-string", "env.appEnv=dev")
	require.NoError(t, err, out)
	assert.NotContains(t, out, "kind: AuthorizationPolicy")
}

func TestHelmAuthorizationPolicy_TheRenderRefusesAPolicyThatIsOffInProdAndStaging(t *testing.T) {
	for _, env := range []string{"prod", "staging"} {
		out, err := helmRender(t, "--set", "authorizationPolicy.enabled=false", "--set-string", "env.appEnv="+env)
		require.Error(t, err, "env.appEnv=%s with the policy off leaves the internal routes to the NetworkPolicy alone", env)
		assert.Contains(t, out, "authorizationPolicy.allowDisabled", env)

		out, err = helmRender(t, "--set", "authorizationPolicy.enabled=false", "--set", "authorizationPolicy.allowDisabled=true", "--set-string", "env.appEnv="+env)
		require.NoError(t, err, out)
		assert.NotContains(t, out, "kind: AuthorizationPolicy")
	}
	for _, env := range []string{"local", "dev", "test"} {
		out, err := helmRender(t, "--set", "authorizationPolicy.enabled=false", "--set-string", "env.appEnv="+env)
		require.NoError(t, err, out)
	}
	out, err := helmRender(t, "--set-string", "env.appEnv=prod")
	require.NoError(t, err, out, "the policy on is the default and renders in prod")
}

func TestHelmNotes_StateTheMeshRequirement(t *testing.T) {
	body, err := os.ReadFile("../../deploy/helm/templates/NOTES.txt")
	require.NoError(t, err)
	notes := string(body)

	assert.Contains(t, notes, "sidecar")
	assert.Contains(t, notes, "mutual TLS")
	assert.Contains(t, notes, "refused on the internal paths")
	assert.Contains(t, notes, "PeerAuthentication")
	assert.Contains(t, notes, "Platform")
}

func TestHelmNames_ReleaseWfRendersTheFixedServiceAndServiceAccountNames(t *testing.T) {
	out, err := helmRenderAs(t, "wf", connectorWorkerOn...)
	require.NoError(t, err, out)

	var services, accounts []string
	for _, o := range decodeAll[chartObject](t, out) {
		switch o.Kind {
		case "Service":
			services = append(services, o.Metadata.Name)
		case "ServiceAccount":
			accounts = append(accounts, o.Metadata.Name)
		}
	}

	assert.ElementsMatch(t, []string{"workflow-execution-service-api", "workflow-execution-service-worker"}, services)
	assert.ElementsMatch(t, []string{
		"workflow-execution-service-api", "workflow-execution-service-worker", "workflow-execution-service-connector-worker",
	}, accounts)
}

func TestHelmNames_TheAddressesInTheValuesAgreeWithTheRenderedAPIService(t *testing.T) {
	body, err := os.ReadFile("../../deploy/helm/values.yaml")
	require.NoError(t, err)
	var values struct {
		FullnameOverride string `yaml:"fullnameOverride"`
		ConnectorWorker  struct {
			ExecutionServiceInternalAddr string `yaml:"executionServiceInternalAddr"`
		} `yaml:"connectorWorker"`
	}
	require.NoError(t, yaml.Unmarshal(body, &values))

	assert.Equal(t, "workflow-execution-service", values.FullnameOverride)
	assert.Equal(t, "http://"+values.FullnameOverride+"-api:8080", values.ConnectorWorker.ExecutionServiceInternalAddr)
}

func TestHelmServices_TheMetricsPortsAreNamedAsHTTP(t *testing.T) {
	out, err := helmRenderAs(t, "wf")
	require.NoError(t, err, out)

	seen := 0
	for _, o := range decodeAll[chartObject](t, out) {
		if o.Kind != "Service" {
			continue
		}
		for _, port := range o.Spec.Ports {
			if port.Name == "metrics" || port.Name == "http" {
				assert.Equal(t, "http", port.AppProtocol, "%s port %s: without it a mesh may treat the port as TCP and path rules match nothing", o.Metadata.Name, port.Name)
				seen++
			}
		}
	}
	assert.Equal(t, 3, seen, "the API's http and metrics ports and the worker's metrics port")
}

func ingressOnHTTPPort(np networkPolicy) []peer {
	var peers []peer
	for _, r := range np.Spec.Ingress {
		for _, port := range r.Ports {
			if port.Port == 8080 {
				peers = append(peers, r.From...)
			}
		}
	}
	return peers
}

func TestHelmNetworkPolicy_TheAPIAdmitsTheConnectorWorkerAndOrgMembership(t *testing.T) {
	np := renderNetworkPolicy(t, "wf", connectorWorkerOn...)
	peers := ingressOnHTTPPort(np)

	assert.Contains(t, peers, peer{PodSelector: &labelSelector{MatchLabels: map[string]string{"app.kubernetes.io/name": "workflow-events-consumer"}}})
	assert.Contains(t, peers, peer{PodSelector: &labelSelector{MatchLabels: map[string]string{"app.kubernetes.io/name": "workflow-definition-service"}}})
	assert.Contains(t, peers, peer{
		PodSelector:       &labelSelector{MatchLabels: map[string]string{"app.kubernetes.io/name": "iam-org-membership"}},
		NamespaceSelector: &labelSelector{MatchLabels: map[string]string{"kubernetes.io/metadata.name": "iam"}},
	}, "Org & Membership runs in the iam namespace, so its rule needs a namespace selector")
	assert.Contains(t, peers, peer{PodSelector: &labelSelector{MatchLabels: map[string]string{
		"app.kubernetes.io/name":      "workflow-execution-service",
		"app.kubernetes.io/instance":  "wf",
		"app.kubernetes.io/component": "connector-worker",
	}}})
}

func TestHelmNetworkPolicy_WithoutTheConnectorWorkerNoRuleAdmitsIt(t *testing.T) {
	np := renderNetworkPolicy(t, "wf")

	for _, p := range ingressOnHTTPPort(np) {
		if p.PodSelector != nil {
			assert.NotEqual(t, "connector-worker", p.PodSelector.MatchLabels["app.kubernetes.io/component"])
		}
	}
}

func entitlementsEgress(np networkPolicy) []netRule {
	var rules []netRule
	for _, r := range np.Spec.Egress {
		for _, to := range r.To {
			if to.NamespaceSelector != nil && to.NamespaceSelector.MatchLabels["kubernetes.io/metadata.name"] == "commercial" {
				rules = append(rules, r)
			}
		}
	}
	return rules
}

func TestHelmEntitlements_TheEgressRuleIsRenderedOnlyWhenTheAddressPortsAndPodsAreSet(t *testing.T) {
	const address = "env.entitlementsBaseURL=http://entitlements.commercial:8080"

	assert.Empty(t, entitlementsEgress(renderNetworkPolicy(t, "wf")), "no address, no rule")

	rules := entitlementsEgress(renderNetworkPolicy(t, "wf", "--set-string", address))
	require.Len(t, rules, 1)
	require.Len(t, rules[0].To, 1)
	assert.Equal(t, map[string]string{"app.kubernetes.io/name": "commercial-entitlements"}, rules[0].To[0].PodSelector.MatchLabels)
	require.Len(t, rules[0].Ports, 1)
	assert.Equal(t, 8080, rules[0].Ports[0].Port)

	noPorts := writeValues(t, "networkPolicy:\n  entitlementsEgress:\n    ports: []\n")
	assert.Empty(t, entitlementsEgress(renderNetworkPolicy(t, "wf", "--set-string", address, "-f", noPorts)),
		"without ports the rule would admit every port")

	noPods := writeValues(t, "networkPolicy:\n  entitlementsEgress:\n    podSelector: null\n")
	assert.Empty(t, entitlementsEgress(renderNetworkPolicy(t, "wf", "--set-string", address, "-f", noPods)),
		"without a pod selector the rule would admit every pod in the namespace")
}

// The shared token is gone: no template reads it and the values do not carry it.
func TestHelmChart_CarriesNoSharedToken(t *testing.T) {
	var files []string
	var matches []string
	removed := []string{"INTERNAL_API_TOKEN", "DEFINITION_SERVICE_TOKEN"}

	err := filepath.WalkDir("../../deploy/helm", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		body, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		files = append(files, path)
		for _, name := range removed {
			if bytes.Contains(body, []byte(name)) {
				matches = append(matches, path+" names "+name)
			}
		}
		return nil
	})

	require.NoError(t, err)
	require.NotEmpty(t, files, "the chart directory was not found, so the check proved nothing")
	assert.Contains(t, files, filepath.Join("..", "..", "deploy", "helm", "values.yaml"))
	assert.Empty(t, matches)
}
