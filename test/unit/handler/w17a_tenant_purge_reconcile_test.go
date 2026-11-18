package handler_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

var errW17aStep = errors.New("w17a: step failed: password=hunter2 host=10.7.7.7")

type w17aStep struct {
	name     string
	tenantID uuid.UUID
	ref      string
}

// w17aSteps is the shared record of what the real tenant reconciler asked of its stores, in order.
type w17aSteps struct{ steps []w17aStep }

func (s *w17aSteps) add(name string, tenantID uuid.UUID, ref string) {
	s.steps = append(s.steps, w17aStep{name: name, tenantID: tenantID, ref: ref})
}

type w17aStates struct {
	port.TenantStateRepository
	steps *w17aSteps
	err   error
}

func (f *w17aStates) Record(_ context.Context, tenantID uuid.UUID, status string, _ time.Time) error {
	f.steps.add("record status", tenantID, status)
	return f.err
}

type w17aRetention struct {
	port.TenantRetentionRepository
	steps *w17aSteps
	err   error
}

func (f *w17aRetention) RecordOffboarded(_ context.Context, tenantID uuid.UUID, _, _ time.Time) error {
	f.steps.add("record offboarded", tenantID, "")
	return f.err
}

type w17aTenantInstances struct {
	port.InstanceRepository
	steps *w17aSteps
	list  []*domain.Instance
	err   error
}

func (f *w17aTenantInstances) ListByTenant(_ context.Context, tenantID uuid.UUID, _ port.InstanceListFilter, _ port.PageRequest) ([]*domain.Instance, *port.Cursor, error) {
	f.steps.add("list instances", tenantID, "")
	return f.list, nil, f.err
}

type w17aWorkflows struct {
	port.TemporalClient
	steps *w17aSteps
	err   error
}

func (f *w17aWorkflows) TerminateWorkflow(_ context.Context, workflowID, _, _ string) error {
	f.steps.add("terminate workflow", uuid.Nil, workflowID)
	return f.err
}

func TestTenantEvent_MembershipsPurgedRealReconcilerFails_AnswersAndKeepsTheEventUnmarked(t *testing.T) {
	obstest.Init(t)
	failed := map[string]string{"event_type": "TenantMembershipsPurged", "result": "error"}
	succeeded := map[string]string{"event_type": "TenantMembershipsPurged", "result": "ok"}
	tenantID := uuid.New()
	instance := &domain.Instance{ID: uuid.New(), TenantID: tenantID, TemporalWorkflowID: "w17a-workflow", TemporalRunID: "w17a-run"}
	record := w17aStep{name: "record status", tenantID: tenantID, ref: domain.TenantStatusOffboarded}
	list := w17aStep{name: "list instances", tenantID: tenantID}
	terminate := w17aStep{name: "terminate workflow", tenantID: uuid.Nil, ref: "w17a-workflow"}
	offboarded := w17aStep{name: "record offboarded", tenantID: tenantID}
	cases := []struct {
		name       string
		setup      func(states *w17aStates, instances *w17aTenantInstances, workflows *w17aWorkflows, retention *w17aRetention)
		wantSteps  []w17aStep
		wantStatus int
		wantCode   string
		wantType   string
		wantTitle  string
		wantDetail string
	}{
		{
			name:       "the status record fails",
			setup:      func(s *w17aStates, _ *w17aTenantInstances, _ *w17aWorkflows, _ *w17aRetention) { s.err = errW17aStep },
			wantSteps:  []w17aStep{record},
			wantStatus: http.StatusInternalServerError, wantCode: "INTERNAL_ERROR", wantType: w17aInternalType,
			wantTitle: "Internal Server Error", wantDetail: "failed to process event",
		},
		{
			name:       "the list of the tenant's instances fails",
			setup:      func(_ *w17aStates, i *w17aTenantInstances, _ *w17aWorkflows, _ *w17aRetention) { i.err = errW17aStep },
			wantSteps:  []w17aStep{record, list},
			wantStatus: http.StatusInternalServerError, wantCode: "INTERNAL_ERROR", wantType: w17aInternalType,
			wantTitle: "Internal Server Error", wantDetail: "failed to process event",
		},
		{
			name: "ending an instance fails",
			setup: func(_ *w17aStates, i *w17aTenantInstances, w *w17aWorkflows, _ *w17aRetention) {
				i.list = []*domain.Instance{instance}
				w.err = errW17aStep
			},
			wantSteps:  []w17aStep{record, list, terminate},
			wantStatus: http.StatusInternalServerError, wantCode: "INTERNAL_ERROR", wantType: w17aInternalType,
			wantTitle: "Internal Server Error", wantDetail: "failed to process event",
		},
		{
			name: "ending an instance fails because the workflow engine is unavailable",
			setup: func(_ *w17aStates, i *w17aTenantInstances, w *w17aWorkflows, _ *w17aRetention) {
				i.list = []*domain.Instance{instance}
				w.err = fmt.Errorf("%w: %w", port.ErrTemporalUnavailable, errW17aStep)
			},
			wantSteps:  []w17aStep{record, list, terminate},
			wantStatus: http.StatusServiceUnavailable, wantCode: "UPSTREAM_UNAVAILABLE", wantType: "https://errors.bcbp.io/execution/upstream-unavailable",
			wantTitle: "Service Unavailable", wantDetail: "the workflow engine could not take the request; retry it",
		},
		{
			name:       "the offboarded record fails",
			setup:      func(_ *w17aStates, _ *w17aTenantInstances, _ *w17aWorkflows, r *w17aRetention) { r.err = errW17aStep },
			wantSteps:  []w17aStep{record, list, offboarded},
			wantStatus: http.StatusInternalServerError, wantCode: "INTERNAL_ERROR", wantType: w17aInternalType,
			wantTitle: "Internal Server Error", wantDetail: "failed to process event",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			steps := &w17aSteps{}
			states := &w17aStates{steps: steps}
			instances := &w17aTenantInstances{steps: steps}
			workflows := &w17aWorkflows{steps: steps}
			retention := &w17aRetention{steps: steps}
			tc.setup(states, instances, workflows, retention)
			rig := w17aNewRigWithTenant(&service.TenantLifecycleReconciler{
				States: states, Instances: instances, Temporal: workflows, Retention: retention,
			})
			eventID := uuid.New()
			at := time.Now().UTC().Truncate(time.Second)
			failedBefore, okBefore := obstest.Sum(t, w17aIngestMetric, failed), obstest.Sum(t, w17aIngestMetric, succeeded)

			w := rig.postEnvelope(w17aTenantRoute, envelope("TenantMembershipsPurged", eventID, tenantID, at, map[string]any{}))

			require.Equal(t, tc.wantStatus, w.Code, w.Body.String())
			p := w17aDecodeProblem(t, w)
			assert.Equal(t, tc.wantStatus, p.Status)
			assert.Equal(t, tc.wantCode, p.Code)
			assert.Equal(t, tc.wantType, p.Type)
			assert.Equal(t, tc.wantTitle, p.Title)
			assert.Equal(t, tc.wantDetail, p.Detail)
			assert.NotContains(t, w.Body.String(), "hunter2", "the error text is not echoed")
			assert.NotContains(t, w.Body.String(), "10.7.7.7")
			assert.Equal(t, tc.wantSteps, steps.steps, "the reconciler stopped at the failing step")
			assert.Empty(t, rig.recency.commits, "no recency is recorded for a sweep that failed")
			assert.Equal(t, []w17aInboxKey{{eventID, port.ConsumerMembership}}, rig.inbox.lookups)
			assert.Empty(t, rig.inbox.marks, "the event is not marked processed, so it is delivered again")
			assert.Equal(t, failedBefore+1, obstest.Sum(t, w17aIngestMetric, failed))
			assert.Equal(t, okBefore, obstest.Sum(t, w17aIngestMetric, succeeded))
		})
	}
}
