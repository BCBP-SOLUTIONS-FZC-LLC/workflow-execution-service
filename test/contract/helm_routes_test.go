package contract_test

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	routerFile     = "../../internal/adapter/inbound/http/router.go"
	internalPrefix = "/api/v1/internal"
)

var (
	routerGroupLine = regexp.MustCompile(`(\w+)\s*:=\s*(\w+)\.Group\("([^"]*)"\)`)
	routerRouteLine = regexp.MustCompile(`(\w+)\.(GET|POST|PUT|PATCH|DELETE)\("([^"]*)"`)
)

type internalRoute struct {
	method string
	path   string // the full path, with each :param replaced by a sample segment
}

// routerInternalRoutes reads the routes registered on the internal group or on a group below it. A route is
// registered through its group variable, so the group lines give each variable its prefix.
func routerInternalRoutes(t *testing.T) []internalRoute {
	t.Helper()
	body, err := os.ReadFile(routerFile)
	require.NoError(t, err)

	prefixes := map[string]string{}
	for _, m := range routerGroupLine.FindAllStringSubmatch(string(body), -1) {
		prefixes[m[1]] = prefixes[m[2]] + m[3]
	}
	var routes []internalRoute
	for _, m := range routerRouteLine.FindAllStringSubmatch(string(body), -1) {
		prefix, grouped := prefixes[m[1]]
		if !grouped || !strings.HasPrefix(prefix, internalPrefix) {
			continue
		}
		segments := strings.Split(prefix+m[3], "/")
		for i, s := range segments {
			if strings.HasPrefix(s, ":") {
				segments[i] = "sample"
			}
		}
		routes = append(routes, internalRoute{method: m[2], path: strings.Join(segments, "/")})
	}
	return routes
}

// istioPathMatches follows Istio's path matching: exact, or one "*" at the start or the end. A
// pattern "/x/*" matches paths below "/x/" and never "/x" itself.
func istioPathMatches(pattern, path string) bool {
	switch {
	case strings.HasSuffix(pattern, "*"):
		return strings.HasPrefix(path, strings.TrimSuffix(pattern, "*"))
	case strings.HasPrefix(pattern, "*"):
		return strings.HasSuffix(path, strings.TrimPrefix(pattern, "*"))
	default:
		return path == pattern
	}
}

// groupsAdmitting returns the indexes into want of the rules (after rule 1) whose paths match the route.
func groupsAdmitting(rules []policyRule, r internalRoute) []int {
	var groups []int
	for i, rule := range rules[1:] {
		for _, to := range rule.To {
			for _, p := range to.Operation.Paths {
				if istioPathMatches(p, r.path) {
					groups = append(groups, i)
				}
			}
		}
	}
	return groups
}

// A route added to the router with no caller group must fail here, not be silently closed (or open).
// The groups are the entries of authorizationPolicy.internalRoutes in the order of defaultInternalRules,
// with the derived connector-tasks group last.
func TestHelmAuthorizationPolicy_EveryInternalRouteBelongsToExactlyOneGroup(t *testing.T) {
	routes := routerInternalRoutes(t)
	require.NotEmpty(t, routes, "no internal route was read from the router, so the check proved nothing")

	p := renderPolicy(t, "wf", append([]string{"--namespace", releaseNamespace}, connectorWorkerOn...)...)
	require.Empty(t, policyProblems(p.Spec.Rules, defaultInternalRules(releaseNamespace, true)))

	const events, definition, workflowClient, connectorTasks = 0, 1, 2, 3
	expectedGroup := map[string]int{
		"POST " + internalPrefix + "/events/delegation":                events,
		"POST " + internalPrefix + "/events/user-profile":              events,
		"POST " + internalPrefix + "/events/tenant":                    events,
		"POST " + internalPrefix + "/events/workflow-task":             events,
		"POST " + internalPrefix + "/users/sample/pause-tasks":         definition,
		"GET " + internalPrefix + "/workflows/sample/active-instances": definition,
		"POST " + internalPrefix + "/workflows/reassign-delegate":      workflowClient,
		"POST " + internalPrefix + "/workflows/cancel-by-delegate":     workflowClient,
		"GET " + internalPrefix + "/workflows/delegate-impact":         workflowClient,
		"POST " + internalPrefix + "/connector-tasks/sample/complete":  connectorTasks,
		"POST " + internalPrefix + "/connector-tasks/sample/fail":      connectorTasks,
	}

	seen := map[string]bool{}
	for _, r := range routes {
		key := r.method + " " + r.path
		seen[key] = true
		groups := groupsAdmitting(p.Spec.Rules, r)
		assert.Len(t, groups, 1, "%s must match exactly one caller group, matched %v: add it to authorizationPolicy.internalRoutes", key, groups)
		if len(groups) == 1 {
			want, known := expectedGroup[key]
			assert.True(t, known, "%s is a new internal route: name its group in this test", key)
			assert.Equal(t, want, groups[0], key)
		}
	}
	for key := range expectedGroup {
		assert.True(t, seen[key], "%s is expected but the router no longer lists it", key)
	}
}

func TestHelmAuthorizationPolicy_TheRouteCheckSeesAnUncoveredRouteAndAnOverlap(t *testing.T) {
	p := renderPolicy(t, "wf", append([]string{"--namespace", releaseNamespace}, connectorWorkerOn...)...)

	assert.Empty(t, groupsAdmitting(p.Spec.Rules, internalRoute{"POST", internalPrefix + "/tenants/purge"}), "a new route no group names")
	assert.Equal(t, []int{1}, groupsAdmitting(p.Spec.Rules, internalRoute{"GET", internalPrefix + "/workflows/sample/active-instances"}),
		"the suffix pattern matches a route below /workflows/ that the delegate group's exact paths do not")

	overlapping := append(append([]policyRule(nil), p.Spec.Rules...), p.Spec.Rules[1])
	assert.Len(t, groupsAdmitting(overlapping, internalRoute{"POST", internalPrefix + "/events/tenant"}), 2)
}
