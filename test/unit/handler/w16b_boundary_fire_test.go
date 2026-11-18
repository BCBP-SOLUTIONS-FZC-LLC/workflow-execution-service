package handler_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

var errW16bDefinitionsDown = errors.New("w16b definitions down")

type w16bRead struct{ tenantID, id uuid.UUID }

type w16bFireInstances struct {
	port.InstanceRepository
	inst  *domain.Instance
	reads []w16bRead
}

func (r *w16bFireInstances) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	r.reads = append(r.reads, w16bRead{tenantID, id})
	if r.inst.TenantID != tenantID || r.inst.ID != id {
		return nil, domain.ErrNotFound
	}
	return r.inst, nil
}

type w16bFireDefinitions struct {
	resp  *port.CompiledWorkflow
	err   error
	calls []w16bRead
}

func (d *w16bFireDefinitions) GetCompiledWorkflow(_ context.Context, tenantID, versionID uuid.UUID) (*port.CompiledWorkflow, error) {
	d.calls = append(d.calls, w16bRead{tenantID, versionID})
	return d.resp, d.err
}

type w16bFireSignal struct {
	workflowID string
	instanceID uuid.UUID
	name       string
	payload    []byte
}

type w16bFireTemporal struct {
	port.TemporalClient
	mu       sync.Mutex
	waiting  []string
	queries  []string
	signals  []w16bFireSignal
	queryErr error
}

func (c *w16bFireTemporal) QueryWorkflow(_ context.Context, workflowID, queryType string, out any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queries = append(c.queries, workflowID+" "+queryType)
	if c.queryErr != nil {
		return c.queryErr
	}
	raw, err := json.Marshal(c.waiting)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, out)
}

func (c *w16bFireTemporal) SignalWorkflow(_ context.Context, workflowID string, instanceID uuid.UUID, name string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.signals = append(c.signals, w16bFireSignal{workflowID, instanceID, name, raw})
	return nil
}

// w16bFirePlan has a sub-workflow Sub with a message, a timer and an error boundary, a call CA_1 with a message
// boundary, and a stage Prepare of department finance with a timer boundary.
func w16bFirePlan(t *testing.T) string {
	t.Helper()
	collab := dsl.CompiledCollaboration{
		MainPlan:      "main",
		SchemaVersion: dsl.CurrentSchemaVersion,
		Plans: []*dsl.CompiledPlan{
			{
				Name:      "main",
				TaskQueue: "wf-queue-default",
				Departments: []dsl.DepartmentDef{
					{ID: "finance", Label: "Finance", Stages: []dsl.StageDef{{
						Type: "prep", NodeID: "Prepare", DefaultAssignees: []string{uuid.NewString()},
						BoundaryTimer: &dsl.BoundaryTimer{BoundaryID: "Boundary_Timer", Duration: "PT1H", Interrupting: true, Terminates: true},
					}}},
					{ID: "ops", Label: "Ops", Stages: []dsl.StageDef{{Type: "prep", NodeID: "Inner", DefaultAssignees: []string{uuid.NewString()}}}},
				},
				Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{
					{Sequential: []string{"finance"}},
					{SubWorkflow: &dsl.SubWorkflowStep{
						NodeID: "Sub", Name: "Sub",
						Plan:         dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"ops"}}}},
						MessagePaths: []dsl.MessagePath{{MessageName: "cancel", BoundaryID: "Boundary_Cancel", Interrupting: true, Terminates: true}},
						TimerPaths:   []dsl.TimerPath{{BoundaryID: "Boundary_SubTimer", Duration: "PT1H", Interrupting: true, Terminates: true}},
						ErrorPaths:   []dsl.ErrorPath{{BoundaryID: "Boundary_SubError", ErrorCode: "E1", Terminates: true}},
					}},
					{CallPlan: &dsl.CallPlanStep{
						NodeID: "CA_1", Name: "CA_1", Plan: "lib",
						MessagePaths: []dsl.MessagePath{{MessageName: "stop", BoundaryID: "Boundary_Stop", Interrupting: true, Terminates: true}},
					}},
				}},
			},
			{
				Name:        "lib",
				Departments: []dsl.DepartmentDef{{ID: "legal", Label: "Legal", Stages: []dsl.StageDef{{Type: "prep", NodeID: "Check", DefaultAssignees: []string{uuid.NewString()}}}}},
				Execution:   dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"legal"}}}},
			},
		},
	}
	raw, err := json.Marshal(collab)
	require.NoError(t, err)
	return string(raw)
}

type w16bFireWorld struct {
	instances   *w16bFireInstances
	definitions *w16bFireDefinitions
	temporal    *w16bFireTemporal
	versionID   uuid.UUID
}

func w16bNewFireWorld(t *testing.T, waiting ...string) (*w16bFireWorld, *gin.Engine) {
	t.Helper()
	versionID := uuid.New()
	w := &w16bFireWorld{
		versionID: versionID,
		instances: &w16bFireInstances{inst: &domain.Instance{
			ID: testInstID, TenantID: testTenantID, WorkflowVersionID: versionID,
			TemporalWorkflowID: "tenant:subject:w16b", Status: domain.InstanceStatusRunning,
		}},
		definitions: &w16bFireDefinitions{resp: &port.CompiledWorkflow{
			VersionID: versionID, Status: "PUBLISHED", IsValid: true, CompiledPlanJSON: w16bFirePlan(t),
		}},
		temporal: &w16bFireTemporal{waiting: waiting},
	}
	svc := &service.InstanceService{Instances: w.instances, Temporal: w.temporal, Definitions: w.definitions}
	return w, newRouter(handler.New(handler.Services{Instances: svc}))
}

func w16bFireTarget(node, boundary string) string {
	return fmt.Sprintf("/api/v1/instances/%s/nodes/%s/boundaries/%s/fire", testInstID, node, boundary)
}

type w16bFireProblem struct {
	Code string `json:"code"`
}

func TestFireOutsideBoundary_OnASubWorkflowOrCallMessageBoundary_Returns202AndSignalsTheHostedKey(t *testing.T) {
	cases := []struct {
		name, node, boundary, wantKey string
	}{
		{"a sub-workflow's message boundary", "Sub", "Boundary_Cancel", "Sub#Boundary_Cancel"},
		{"a call's message boundary", "CA_1", "Boundary_Stop", "CA_1#Boundary_Stop"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			world, router := w16bNewFireWorld(t, tc.wantKey)

			r := req(http.MethodPost, w16bFireTarget(tc.node, tc.boundary), map[string]any{"variables": map[string]any{"reason": "withdrawn"}})
			r.Header.Set("x-tenant-roles", "tenant_admin")
			w := do(router, r)

			require.Equal(t, http.StatusAccepted, w.Code, w.Body.String())
			assert.Equal(t, []w16bRead{{testTenantID, testInstID}}, world.instances.reads)
			assert.Equal(t, []w16bRead{{testTenantID, world.versionID}}, world.definitions.calls)
			assert.Equal(t, []string{"tenant:subject:w16b " + port.QueryWaitingExternal}, world.temporal.queries)
			require.Len(t, world.temporal.signals, 1)
			got := world.temporal.signals[0]
			assert.Equal(t, "tenant:subject:w16b", got.workflowID)
			assert.Equal(t, testInstID, got.instanceID)
			assert.Equal(t, port.SignalInstanceExternalAnswer, got.name)
			assert.JSONEq(t, `{"Key":"`+tc.wantKey+`","Variables":{"reason":"withdrawn"}}`, string(got.payload))
		})
	}
}

func TestFireOutsideBoundary_OnATimerOrErrorBoundaryOfThePlan_Returns404WaitNotFoundAndSignalsNothing(t *testing.T) {
	cases := []struct {
		name, node, boundary string
	}{
		{"a stage's timer boundary", "finance%2FPrepare", "Boundary_Timer"},
		{"a sub-workflow's timer boundary", "Sub", "Boundary_SubTimer"},
		{"a sub-workflow's error boundary", "Sub", "Boundary_SubError"},
		{"a boundary the plan does not have", "Sub", "Boundary_Missing"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The instance waits in every key, so only the plan can refuse the answer.
			world, router := w16bNewFireWorld(t, "finance/Prepare#Boundary_Timer", "Sub#Boundary_SubTimer", "Sub#Boundary_SubError", "Sub#Boundary_Missing")

			r := req(http.MethodPost, w16bFireTarget(tc.node, tc.boundary), nil)
			r.Header.Set("x-tenant-roles", "tenant_admin")
			w := do(router, r)

			require.Equal(t, http.StatusNotFound, w.Code, w.Body.String())
			var p w16bFireProblem
			decodeJSON(t, w.Body, &p)
			assert.Equal(t, "WAIT_NOT_FOUND", p.Code)
			assert.Equal(t, []w16bRead{{testTenantID, world.versionID}}, world.definitions.calls)
			assert.Empty(t, world.temporal.queries, "the workflow is not asked before the plan refuses the key")
			assert.Empty(t, world.temporal.signals)
		})
	}
}

func TestFireOutsideBoundary_WhenTheDefinitionServiceCannotServeThePlan_Returns503UpstreamUnavailable(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"the Definition Service is unreachable", fmt.Errorf("%w: %w", port.ErrDefinitionUnavailable, errW16bDefinitionsDown)},
		{"the Definition Service refuses the version", fmt.Errorf("%w: status 404", port.ErrDefinitionRejected)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			world, router := w16bNewFireWorld(t, "Sub#Boundary_Cancel")
			world.definitions.resp, world.definitions.err = nil, tc.err

			r := req(http.MethodPost, w16bFireTarget("Sub", "Boundary_Cancel"), nil)
			r.Header.Set("x-tenant-roles", "tenant_admin")
			w := do(router, r)

			require.Equal(t, http.StatusServiceUnavailable, w.Code, w.Body.String())
			var p w16bFireProblem
			decodeJSON(t, w.Body, &p)
			assert.Equal(t, "UPSTREAM_UNAVAILABLE", p.Code)
			assert.NotContains(t, w.Body.String(), "w16b definitions down", "the cause is not echoed")
			assert.NotContains(t, w.Body.String(), "status 404", "the cause is not echoed")
			assert.Equal(t, []w16bRead{{testTenantID, world.versionID}}, world.definitions.calls, "the plan was asked for once, for this tenant and the instance's version")
			assert.Empty(t, world.temporal.queries)
			assert.Empty(t, world.temporal.signals)
		})
	}
}

func TestFireOutsideBoundary_WithNullOrEmptyVariables_Returns202AndSignalsNoVariables(t *testing.T) {
	cases := []struct {
		name string
		body any
	}{
		{"variables null", map[string]any{"variables": nil}},
		{"variables an empty object", map[string]any{"variables": map[string]any{}}},
		{"an empty object body", map[string]any{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			world, router := w16bNewFireWorld(t, "Sub#Boundary_Cancel")

			r := req(http.MethodPost, w16bFireTarget("Sub", "Boundary_Cancel"), tc.body)
			r.Header.Set("x-tenant-roles", "tenant_admin")
			w := do(router, r)

			require.Equal(t, http.StatusAccepted, w.Code, w.Body.String())
			require.Len(t, world.temporal.signals, 1)
			var sent struct {
				Key       string
				Variables map[string]any
			}
			require.NoError(t, json.Unmarshal(world.temporal.signals[0].payload, &sent))
			assert.Equal(t, "Sub#Boundary_Cancel", sent.Key)
			assert.Empty(t, sent.Variables, "no variables are sent to be merged")
		})
	}
}
