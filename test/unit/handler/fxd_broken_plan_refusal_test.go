package handler_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

type fxdStoredInstance struct {
	port.InstanceRepository
	inst    *domain.Instance
	creates []*domain.Instance
}

func (r *fxdStoredInstance) GetLiveByBusinessKey(context.Context, uuid.UUID, string, string) (*domain.Instance, error) {
	return nil, domain.ErrNotFound
}

func (r *fxdStoredInstance) Create(_ context.Context, inst *domain.Instance) error {
	r.creates = append(r.creates, inst)
	return nil
}

func (r *fxdStoredInstance) RecordRunID(context.Context, uuid.UUID, uuid.UUID, string) error {
	return nil
}

func (r *fxdStoredInstance) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	if r.inst == nil || r.inst.TenantID != tenantID || r.inst.ID != id {
		return nil, domain.ErrNotFound
	}
	return r.inst, nil
}

type fxdPlanRead struct{ tenantID, versionID uuid.UUID }

type fxdPlanSource struct {
	planJSON string
	reads    []fxdPlanRead
}

func (d *fxdPlanSource) GetCompiledWorkflow(_ context.Context, tenantID, versionID uuid.UUID) (*port.CompiledWorkflow, error) {
	d.reads = append(d.reads, fxdPlanRead{tenantID, versionID})
	return &port.CompiledWorkflow{
		WorkflowID: uuid.New(), VersionID: versionID, VersionNumber: 1, Status: "PUBLISHED", IsValid: true, CompiledPlanJSON: d.planJSON,
	}, nil
}

type fxdSignal struct {
	instanceID uuid.UUID
	name       string
}

type fxdTemporalCalls struct {
	port.TemporalClient
	starts  []port.StartWorkflowInput
	signals []fxdSignal
}

func (c *fxdTemporalCalls) StartWorkflow(_ context.Context, in port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
	c.starts = append(c.starts, in)
	return port.StartWorkflowOutput{TemporalWorkflowID: in.TemporalWorkflowID, TemporalRunID: "run-1"}, nil
}

func (c *fxdTemporalCalls) SignalWorkflow(_ context.Context, _ string, instanceID uuid.UUID, name string, _ any) error {
	c.signals = append(c.signals, fxdSignal{instanceID, name})
	return nil
}

type fxdTransactor struct{ port.Transactor }

func (fxdTransactor) RunInTx(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}

type fxdOutbox struct{ port.OutboxRepository }

func (fxdOutbox) Enqueue(context.Context, events.Envelope[json.RawMessage]) error { return nil }

type fxdAcceptingValidator struct{}

func (fxdAcceptingValidator) Validate(context.Context, string, json.RawMessage) error { return nil }

type fxdNoQueues struct{ port.ActiveTaskQueueRepository }

func (fxdNoQueues) GetByQueueName(context.Context, string) (*domain.ActiveTaskQueue, error) {
	return nil, domain.ErrNotFound
}

func fxdCollaborationJSON(t *testing.T, mainPlan string, plans ...*dsl.CompiledPlan) string {
	t.Helper()
	raw, err := json.Marshal(dsl.CompiledCollaboration{SchemaVersion: dsl.CurrentSchemaVersion, MainPlan: mainPlan, Plans: plans})
	require.NoError(t, err)
	return string(raw)
}

func fxdBrokenPlans(t *testing.T) map[string]string {
	t.Helper()
	return map[string]string{
		"the main plan names no plan of the collaboration": fxdCollaborationJSON(t, "ghost", &dsl.CompiledPlan{Name: "main", TaskQueue: "wf-queue-default"}),
		"the plan json does not decode":                    "{",
	}
}

func fxdAssertVersionInvalid(t *testing.T, w *httptest.ResponseRecorder) {
	t.Helper()
	body := w.Body.Bytes()
	assert.Equal(t, http.StatusConflict, w.Code, string(body))
	var p w13aProblem
	require.NoError(t, json.Unmarshal(body, &p), string(body))
	assert.Equal(t, "VERSION_INVALID", p.Code)
	assert.Equal(t, "https://errors.bcbp.io/execution/version-invalid", p.Type)
	assert.NotContains(t, string(body), "unexpected end of JSON", "the decoder text is not echoed")
}

func fxdStartService(planJSON string, instances *fxdStoredInstance, temporal *fxdTemporalCalls, source *fxdPlanSource) *service.InstanceService {
	source.planJSON = planJSON
	return &service.InstanceService{
		Instances: instances, Definitions: source, Temporal: temporal,
		Transactor: fxdTransactor{}, Outbox: fxdOutbox{}, Queues: fxdNoQueues{}, Validator: fxdAcceptingValidator{},
	}
}

func fxdPostStart(router *gin.Engine, versionID uuid.UUID) *httptest.ResponseRecorder {
	return do(router, req(http.MethodPost, instancesPath(), map[string]any{
		"subject_id": "SUBJ-1", "business_key": "TND-001", "workflow_version_id": versionID,
	}))
}

func TestStartInstance_AStoredPlanWithNoMainPlanOrUndecodableJSONAnswersVersionInvalid(t *testing.T) {
	for name, planJSON := range fxdBrokenPlans(t) {
		t.Run(name, func(t *testing.T) {
			versionID := uuid.New()
			instances, temporal, source := &fxdStoredInstance{}, &fxdTemporalCalls{}, &fxdPlanSource{}
			router := newRouter(handler.New(handler.Services{Instances: fxdStartService(planJSON, instances, temporal, source)}))

			w := fxdPostStart(router, versionID)

			fxdAssertVersionInvalid(t, w)
			assert.Equal(t, []fxdPlanRead{{testTenantID, versionID}}, source.reads, "the plan of the requested version is read for the caller's tenant")
			assert.Empty(t, instances.creates, "no instance row is written")
			assert.Empty(t, temporal.starts, "no workflow is started")
			assert.Empty(t, temporal.signals, "no workflow is signalled")
		})
	}
}

func TestForceForward_AStoredPlanWithNoMainPlanOrUndecodableJSONAnswersVersionInvalid(t *testing.T) {
	for name, planJSON := range fxdBrokenPlans(t) {
		t.Run(name, func(t *testing.T) {
			versionID := uuid.New()
			inst := &domain.Instance{
				ID: testInstID, TenantID: testTenantID, WorkflowVersionID: versionID, Status: domain.InstanceStatusRunning,
				RecordVersion: 2, TemporalWorkflowID: "tenant:instance",
			}
			instances, temporal, source := &fxdStoredInstance{inst: inst}, &fxdTemporalCalls{}, &fxdPlanSource{planJSON: planJSON}
			svc := &service.InstanceService{Instances: instances, Definitions: source, Temporal: temporal}
			router := newRouter(handler.New(handler.Services{Instances: svc}))

			w := do(router, asAdmin(req(http.MethodPost, instancePath(testInstID)+"/force-forward", map[string]any{
				"target_node_key": "finance/review", "record_version": 2,
			})))

			fxdAssertVersionInvalid(t, w)
			assert.Equal(t, []fxdPlanRead{{testTenantID, versionID}}, source.reads, "the plan of the instance's own version is read for the caller's tenant")
			assert.Empty(t, temporal.signals, "no workflow is signalled")
			assert.Empty(t, temporal.starts)
		})
	}
}

func fxdNoDepartmentsPlan(t *testing.T) string {
	t.Helper()
	return fxdCollaborationJSON(t, "main", &dsl.CompiledPlan{Name: "main", TaskQueue: "wf-queue-default"})
}

func TestStartInstance_AMainPlanWithNoDepartmentsStartsTheWorkflow(t *testing.T) {
	versionID := uuid.New()
	instances, temporal, source := &fxdStoredInstance{}, &fxdTemporalCalls{}, &fxdPlanSource{}
	router := newRouter(handler.New(handler.Services{Instances: fxdStartService(fxdNoDepartmentsPlan(t), instances, temporal, source)}))

	w := fxdPostStart(router, versionID)

	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
	require.Len(t, instances.creates, 1)
	require.Len(t, temporal.starts, 1)
	assert.Equal(t, testTenantID, temporal.starts[0].TenantID)
	assert.Equal(t, instances.creates[0].ID, temporal.starts[0].InstanceID)
	assert.Equal(t, versionID, temporal.starts[0].WorkflowVersionID)
}

func TestForceForward_AMainPlanWithNoDepartmentsRefusesAnyTargetNode(t *testing.T) {
	versionID := uuid.New()
	inst := &domain.Instance{
		ID: testInstID, TenantID: testTenantID, WorkflowVersionID: versionID, Status: domain.InstanceStatusRunning,
		RecordVersion: 2, TemporalWorkflowID: "tenant:instance",
	}
	temporal, source := &fxdTemporalCalls{}, &fxdPlanSource{planJSON: fxdNoDepartmentsPlan(t)}
	svc := &service.InstanceService{Instances: &fxdStoredInstance{inst: inst}, Definitions: source, Temporal: temporal}
	router := newRouter(handler.New(handler.Services{Instances: svc}))

	w := do(router, asAdmin(req(http.MethodPost, instancePath(testInstID)+"/force-forward", map[string]any{
		"target_node_key": "finance/review", "record_version": 2,
	})))

	require.Equal(t, http.StatusConflict, w.Code, w.Body.String())
	assert.Equal(t, "TARGET_NODE_NOT_FOUND", w13aDecodeProblemCode(t, w))
	assert.Equal(t, []fxdPlanRead{{testTenantID, versionID}}, source.reads)
	assert.Empty(t, temporal.signals, "no workflow is signalled")
}

func w13aDecodeProblemCode(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()
	var p w13aProblem
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &p), w.Body.String())
	return p.Code
}
