package temporal_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"

	outboundhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/http"
	outboundtemporal "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl/dsltest"
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

// The API's plan cache can hold a version's status for an hour, so the
// activity's read is the authoritative check (execution LLD §1, §3.1): a
// version that is no longer PUBLISHED, or that Definition marked invalid
// after a membership revocation, starts nothing.
func TestGetCompiledPlan_VersionNotRunnable_IsNonRetryable(t *testing.T) {
	plan := `{"main_plan":"main","schema_version":1}`
	for name, body := range map[string]string{
		"draft":    strings.Replace(planBody(plan), `"status":"PUBLISHED"`, `"status":"DRAFT"`, 1),
		"archived": strings.Replace(planBody(plan), `"status":"PUBLISHED"`, `"status":"ARCHIVED"`, 1),
		"invalid":  strings.Replace(planBody(plan), `"is_valid":true`, `"is_valid":false`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := readPlan(planServer(t, http.StatusOK, body))
			requireNonRetryable(t, err)
		})
	}
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

func sizedPlan(stages int) string {
	defs := make([]dsl.StageDef, stages)
	for i := range defs {
		defs[i] = dsl.StageDef{Type: "approve", NodeID: fmt.Sprintf("T%d", i)}
	}
	raw, _ := json.Marshal(dsl.CompiledCollaboration{
		MainPlan:      "main",
		SchemaVersion: dsl.CurrentSchemaVersion,
		Plans:         []*dsl.CompiledPlan{{Name: "main", Departments: []dsl.DepartmentDef{{ID: "Ops", Stages: defs}}}},
	})
	return string(raw)
}

func TestGetCompiledPlan_PlanAtTheStageBudgetLoads(t *testing.T) {
	_, err := readPlan(planDeps(t, sizedPlan(domain.MaxPlanStages)))
	require.NoError(t, err)
}

func TestGetCompiledPlan_PlanOverTheStageBudget_IsNonRetryable(t *testing.T) {
	_, err := readPlan(planDeps(t, sizedPlan(domain.MaxPlanStages+1)))
	requireNonRetryable(t, err)
	assert.ErrorIs(t, err, dsl.ErrPlanTooLarge)
}

func TestGetCompiledPlan_ATransientFailureCountsAsADefinitionDependencyError(t *testing.T) {
	obstest.Init(t)
	counter := observability.UpstreamDependencyErrorsTotal.WithLabelValues("definition_service")
	before := testutil.ToFloat64(counter)

	_, err := readPlan(planServer(t, http.StatusServiceUnavailable, `{}`))
	require.Error(t, err)

	assert.Equal(t, before+1, testutil.ToFloat64(counter), "the activity retries without limit, so each failed read must show on the upstream error alert")
}

func TestGetCompiledPlan_ARejectionIsNotADependencyError(t *testing.T) {
	obstest.Init(t)
	counter := observability.UpstreamDependencyErrorsTotal.WithLabelValues("definition_service")
	before := testutil.ToFloat64(counter)

	_, err := readPlan(planServer(t, http.StatusNotFound, `{}`))
	require.Error(t, err)

	assert.Equal(t, before, testutil.ToFloat64(counter))
}
