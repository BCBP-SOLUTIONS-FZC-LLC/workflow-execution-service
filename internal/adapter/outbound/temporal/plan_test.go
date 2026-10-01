package temporal_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"

	outboundhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/http"
	outboundtemporal "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl/dsltest"
)

// planServer answers every compiled-plan read with status and body, as the
// Definition Service's internal API would.
func planServer(t *testing.T, status int, body string) *outboundtemporal.Deps {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return &outboundtemporal.Deps{Definitions: outboundhttp.NewDefinitionClient(srv.URL, "", 2*time.Second)}
}

func planBody(planJSON string) string {
	return `{"workflow_id":"` + uuid.NewString() + `","version_id":"` + uuid.NewString() +
		`","version_number":1,"status":"PUBLISHED","is_valid":true,"compiled_plan":` + planJSON + `}`
}

func planDeps(t *testing.T, planJSON string) *outboundtemporal.Deps {
	t.Helper()
	return planServer(t, http.StatusOK, planBody(planJSON))
}

func readPlan(deps *outboundtemporal.Deps) (port.GetCompiledPlanOutput, error) {
	return deps.GetCompiledPlan(context.Background(), port.GetCompiledPlanInput{TenantID: uuid.NewString(), VersionID: uuid.NewString()})
}

func requireNonRetryable(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	var appErr *temporal.ApplicationError
	require.True(t, errors.As(err, &appErr), "want a non-retryable ApplicationError, got %v", err)
	assert.Equal(t, "DefinitionServiceClientError", appErr.Type())
}

func TestGetCompiledPlan_Success(t *testing.T) {
	out, err := readPlan(planDeps(t, `{"main_plan":"main","schema_version":1}`))
	require.NoError(t, err)
	assert.Equal(t, "main", out.Collaboration.MainPlan)
}

func TestGetCompiledPlan_InvalidTenantID_IsNonRetryable(t *testing.T) {
	deps := &outboundtemporal.Deps{}
	_, err := deps.GetCompiledPlan(context.Background(), port.GetCompiledPlanInput{TenantID: "not-a-uuid", VersionID: uuid.New().String()})
	requireNonRetryable(t, err)
}

func TestGetCompiledPlan_UpstreamRejected_IsNonRetryable(t *testing.T) {
	_, err := readPlan(planServer(t, http.StatusNotFound, `{}`))
	requireNonRetryable(t, err)
}

// A 5xx exhausts the client's own retries and stays retryable for Temporal.
func TestGetCompiledPlan_UpstreamUnavailable_IsRetryable(t *testing.T) {
	_, err := readPlan(planServer(t, http.StatusServiceUnavailable, `{}`))
	require.Error(t, err)
	assert.ErrorContains(t, err, "get compiled workflow")
	var appErr *temporal.ApplicationError
	assert.False(t, errors.As(err, &appErr), "an exhausted-retry upstream-unavailable error must stay retryable, not become a nonRetryable ApplicationError")
}

// A version with no stored plan has nothing to run, which no retry changes.
func TestGetCompiledPlan_VersionWithoutAPlan_IsNonRetryable(t *testing.T) {
	_, err := readPlan(planDeps(t, `null`))
	requireNonRetryable(t, err)
}

// The activity result is what the workflow records and replays, so the
// expansion of library calls happens here: a later change to ExpandCalls
// never alters a running instance's plan.
func TestGetCompiledPlan_ExpandsLibraryCalls(t *testing.T) {
	out, err := readPlan(planDeps(t, string(dsltest.LibraryCalls())))
	require.NoError(t, err)
	var callPlans int
	var walk func(steps []dsl.ExecutionStep)
	walk = func(steps []dsl.ExecutionStep) {
		for _, st := range steps {
			if st.CallPlan != nil {
				callPlans++
			}
			if st.SubWorkflow != nil {
				walk(st.SubWorkflow.Plan.Steps)
			}
		}
	}
	for _, p := range out.Collaboration.Plans {
		walk(p.Execution.Steps)
	}
	assert.Zero(t, callPlans, "the workflow must never see a call_plan step")
	var ids []string
	for _, d := range out.Collaboration.Plans[0].Departments {
		ids = append(ids, d.ID)
	}
	assert.Contains(t, ids, "CA_Ops::Review_Check::Process_Check")
}

func TestGetCompiledPlan_UnexpandablePlan_IsNonRetryable(t *testing.T) {
	missing := `{"main_plan":"main","schema_version":1,"plans":[{"name":"main","departments":[],"execution":{"steps":[{"call_plan":{"node_id":"CA_1","plan":"Nowhere"}}]}}]}`
	_, err := readPlan(planDeps(t, missing))
	requireNonRetryable(t, err)
}

// Expansion follows the current schema; a plan of another major is handed to
// the workflow as stored, whose schema check refuses or normalizes it.
func TestGetCompiledPlan_OtherSchemaVersionIsNotExpanded(t *testing.T) {
	other := `{"main_plan":"main","schema_version":99,"plans":[{"name":"main","departments":[],"execution":{"steps":[{"call_plan":{"node_id":"CA_1","plan":"Nowhere"}}]}}]}`
	out, err := readPlan(planDeps(t, other))
	require.NoError(t, err)
	assert.Equal(t, 99, out.Collaboration.SchemaVersion)
	assert.NotNil(t, out.Collaboration.Plans[0].Execution.Steps[0].CallPlan)
}
