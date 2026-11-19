package handler_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	adapterhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func w25aStartWithError(t *testing.T, err error) (*w25aProblem, string) {
	t.Helper()
	fake := &fakeInstanceService{
		start: func(context.Context, port.StartInstanceInput) (*port.Instance, error) { return nil, err },
	}
	r := w25aRaw(http.MethodPost, "/api/v1/instances", "application/json",
		fmt.Sprintf(`{"subject_id":"SUBJ-1","business_key":"TND-001","workflow_version_id":%q}`, uuid.NewString()))
	w := w25aServe(newRouter(newInstanceHandler(fake)), r)
	p := w25aDecodeProblem(t, w, w.Code)
	assert.Equal(t, "/api/v1/instances", p.Instance)
	return &p, w.Body.String()
}

// An error wrapping the assignee-ineligible error answers 422 ASSIGNEE_INELIGIBLE and names every node in
// invalid_params, in order, each with the reason and the code; with no nodes there are no entries.
func TestStartInstance_AssigneeIneligibleNodes_AreListedWithReasonAndCode(t *testing.T) {
	const nodeDetail = "the assignee of one or more nodes does not satisfy the node's eligibility requirement"
	cases := []struct {
		name       string
		err        error
		nodes      []string
		wantDetail string
	}{
		{name: "two nodes", err: &port.AssigneeIneligibleError{Nodes: []string{"finance/review", "legal/review"}}, nodes: []string{"finance/review", "legal/review"}, wantDetail: nodeDetail},
		{name: "one node", err: &port.AssigneeIneligibleError{Nodes: []string{"finance/review"}}, nodes: []string{"finance/review"}, wantDetail: nodeDetail},
		{name: "a wrapped error with two nodes", err: fmt.Errorf("start: %w", &port.AssigneeIneligibleError{Nodes: []string{"b/x", "a/y"}}), nodes: []string{"b/x", "a/y"}, wantDetail: nodeDetail},
		{name: "no nodes", err: &port.AssigneeIneligibleError{}, nodes: nil, wantDetail: nodeDetail},
		{name: "the plain sentinel carries no entries", err: port.ErrAssigneeIneligible, nodes: nil, wantDetail: "the user does not satisfy the task's department and role level"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, body := w25aStartWithError(t, tc.err)

			assert.Equal(t, http.StatusUnprocessableEntity, p.Status)
			assert.Equal(t, tc.wantDetail, p.Detail)
			assert.NotContains(t, body, "start:", "the wrapping text stays out of the body")
			assert.Equal(t, "ASSIGNEE_INELIGIBLE", p.Code)
			assert.Equal(t, "https://errors.bcbp.io/execution/assignee-ineligible", p.Type)
			assert.Equal(t, "Assignee Ineligible", p.Title)
			require.Len(t, p.InvalidParams, len(tc.nodes))
			for i, node := range tc.nodes {
				assert.Equal(t, node, p.InvalidParams[i].Name)
				assert.Equal(t, "assignee ineligible", p.InvalidParams[i].Reason)
				assert.Equal(t, "ASSIGNEE_INELIGIBLE", p.InvalidParams[i].Code)
			}
		})
	}
}

// The edge answers a Definition Service or upstream failure with 503 and a missing automation principal with 409,
// each with its fixed detail text; the cause text of a wrapped error stays out of the body.
func TestStartInstance_DefinitionUpstreamAndPrincipalErrors_Answer503Or409WithFixedDetail(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
		wantTitle  string
		wantType   string
		wantDetail string
	}{
		{
			name: "definition service unavailable", err: fmt.Errorf("%w: GET http://definition.internal:8080/x refused", port.ErrDefinitionUnavailable),
			wantStatus: http.StatusServiceUnavailable, wantCode: "UPSTREAM_UNAVAILABLE", wantTitle: "Service Unavailable",
			wantType:   "https://errors.bcbp.io/execution/upstream-unavailable",
			wantDetail: "the Definition Service could not serve the workflow version",
		},
		{
			name: "definition service rejected the request", err: fmt.Errorf("%w: status 403 from definition.internal", port.ErrDefinitionRejected),
			wantStatus: http.StatusServiceUnavailable, wantCode: "UPSTREAM_UNAVAILABLE", wantTitle: "Service Unavailable",
			wantType:   "https://errors.bcbp.io/execution/upstream-unavailable",
			wantDetail: "the Definition Service could not serve the workflow version",
		},
		{
			name: "upstream service unavailable", err: fmt.Errorf("%w: iam.internal:9000 timed out", adapterhttp.ErrUpstreamUnavailable),
			wantStatus: http.StatusServiceUnavailable, wantCode: "UPSTREAM_UNAVAILABLE", wantTitle: "Service Unavailable",
			wantType:   "https://errors.bcbp.io/execution/upstream-unavailable",
			wantDetail: "an upstream service could not serve the request",
		},
		{
			name: "no automation principal", err: fmt.Errorf("%w: token service has none for tenant 77", port.ErrNoAutomationPrincipal),
			wantStatus: http.StatusConflict, wantCode: "NO_AUTOMATION_PRINCIPAL", wantTitle: "No Automation Principal",
			wantType:   "https://errors.bcbp.io/execution/no-automation-principal",
			wantDetail: "tenant has no automation principal",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, body := w25aStartWithError(t, tc.err)
			for _, cause := range []string{"definition.internal", "status 403", "iam.internal", "tenant 77"} {
				assert.NotContains(t, body, cause, "the wrapped cause stays out of the body")
			}

			assert.Equal(t, tc.wantStatus, p.Status)
			assert.Equal(t, tc.wantCode, p.Code)
			assert.Equal(t, tc.wantTitle, p.Title)
			assert.Equal(t, tc.wantType, p.Type)
			assert.Equal(t, tc.wantDetail, p.Detail)
			assert.Empty(t, p.InvalidParams)
		})
	}
}
