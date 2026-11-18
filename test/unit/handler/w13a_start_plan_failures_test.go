package handler_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

type w13aProblem struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Detail string `json:"detail"`
	Code   string `json:"code"`
}

type w13aLookup struct {
	tenantID          uuid.UUID
	subjectID, bizKey string
}

// w13aStartInstances finds no live instance for the key and records the lookup and every attempt to create a row.
type w13aStartInstances struct {
	port.InstanceRepository
	lookups []w13aLookup
	creates []*domain.Instance
}

func (r *w13aStartInstances) GetLiveByBusinessKey(_ context.Context, tenantID uuid.UUID, subjectID, businessKey string) (*domain.Instance, error) {
	r.lookups = append(r.lookups, w13aLookup{tenantID, subjectID, businessKey})
	return nil, domain.ErrNotFound
}

func (r *w13aStartInstances) Create(_ context.Context, inst *domain.Instance) error {
	r.creates = append(r.creates, inst)
	return nil
}

type w13aVersionRead struct{ tenantID, versionID uuid.UUID }

type w13aStartDefinitions struct {
	compiled *port.CompiledWorkflow
	reads    []w13aVersionRead
}

func (d *w13aStartDefinitions) GetCompiledWorkflow(_ context.Context, tenantID, versionID uuid.UUID) (*port.CompiledWorkflow, error) {
	d.reads = append(d.reads, w13aVersionRead{tenantID, versionID})
	return d.compiled, nil
}

func TestStartInstance_AnUnparseablePlanAndAPlanThatCannotBeExpandedBothAnswerVersionInvalid(t *testing.T) {
	staleSchema, err := json.Marshal(dsl.CompiledCollaboration{
		SchemaVersion: dsl.CurrentSchemaVersion + 1, MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{Name: "main", TaskQueue: "wf-queue-default"}},
	})
	require.NoError(t, err)
	cases := []struct {
		name       string
		planJSON   string
		wantStatus int
		wantCode   string
		wantTitle  string
		wantType   string
		hidden     []string
	}{
		{
			name: "a plan whose JSON does not parse", planJSON: "{",
			wantStatus: http.StatusConflict, wantCode: "VERSION_INVALID", wantTitle: "Version Invalid",
			wantType: "https://errors.bcbp.io/execution/version-invalid", hidden: []string{"unmarshal", "unexpected end of JSON"},
		},
		{
			name: "control: a plan that cannot be used by this service version", planJSON: string(staleSchema),
			wantStatus: http.StatusConflict, wantCode: "VERSION_INVALID", wantTitle: "Version Invalid",
			wantType: "https://errors.bcbp.io/execution/version-invalid",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			versionID := uuid.New()
			instances := &w13aStartInstances{}
			definitions := &w13aStartDefinitions{compiled: &port.CompiledWorkflow{
				WorkflowID: uuid.New(), VersionID: versionID, VersionNumber: 1, Status: "PUBLISHED", IsValid: true, CompiledPlanJSON: tc.planJSON,
			}}
			svc := &service.InstanceService{Instances: instances, Definitions: definitions}
			router := newRouter(handler.New(handler.Services{Instances: svc}))

			w := do(router, req(http.MethodPost, instancesPath(), map[string]any{
				"subject_id": "SUBJ-1", "business_key": "TND-001", "workflow_version_id": versionID,
			}))

			require.Equal(t, tc.wantStatus, w.Code, w.Body.String())
			var p w13aProblem
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &p), w.Body.String())
			assert.Equal(t, tc.wantCode, p.Code)
			assert.Equal(t, tc.wantTitle, p.Title)
			assert.Equal(t, tc.wantType, p.Type)
			assert.Equal(t, tc.wantStatus, p.Status)
			assert.NotEmpty(t, p.Detail)
			if tc.wantStatus == http.StatusInternalServerError {
				assert.Equal(t, "an unexpected error occurred", p.Detail)
			}
			for _, text := range tc.hidden {
				assert.NotContains(t, w.Body.String(), text, "the plan error text is not echoed")
			}
			assert.Equal(t, []w13aLookup{{testTenantID, "SUBJ-1", "TND-001"}}, instances.lookups)
			assert.Equal(t, []w13aVersionRead{{testTenantID, versionID}}, definitions.reads)
			assert.Empty(t, instances.creates, "no instance is written")
		})
	}
}
