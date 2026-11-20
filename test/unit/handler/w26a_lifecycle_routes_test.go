package handler_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporalclient"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

// The lifecycle routes run the real instance service and the real Temporal client adapter behind the real
// handler; the instance repository and the Temporal SDK client are the only fakes.

type w26aInstanceRepo struct {
	port.InstanceRepository
	rows   []*domain.Instance
	getErr error
	reads  []w26aLookup
}

func (r *w26aInstanceRepo) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	r.reads = append(r.reads, w26aLookup{tenantID, id})
	if r.getErr != nil {
		return nil, r.getErr
	}
	for _, i := range r.rows {
		if i.ID == id && i.TenantID == tenantID {
			return i, nil
		}
	}
	return nil, domain.ErrNotFound
}

type w26aSDKSignal struct {
	workflowID, runID, channel string
	payload                    any
}

type w26aSDK struct {
	client.Client
	signalErr error
	signals   []w26aSDKSignal
}

func (s *w26aSDK) SignalWorkflow(_ context.Context, workflowID, runID, channel string, payload interface{}) error {
	s.signals = append(s.signals, w26aSDKSignal{workflowID, runID, channel, payload})
	return s.signalErr
}

type w26aLifecycleWorld struct {
	repo   *w26aInstanceRepo
	sdk    *w26aSDK
	router *gin.Engine
	inst   *domain.Instance
}

func w26aNewLifecycleWorld(status domain.InstanceStatus, getErr, signalErr error) *w26aLifecycleWorld {
	w := &w26aLifecycleWorld{}
	w.inst = &domain.Instance{
		ID: testInstID, TenantID: testTenantID, Status: status, RecordVersion: 4, TemporalWorkflowID: "tenant:subject:w26a",
	}
	w.repo = &w26aInstanceRepo{rows: []*domain.Instance{w.inst}, getErr: getErr}
	w.sdk = &w26aSDK{signalErr: signalErr}
	svc := &service.InstanceService{Instances: w.repo, Temporal: temporalclient.New(w.sdk)}
	w.router = newRouter(handler.New(handler.Services{Instances: svc}))
	return w
}

type w26aAdminWire struct {
	AdminUserID   string
	Reason        string
	Initiator     string
	RecordVersion int64
}

func TestResumeInstanceRoute_InstanceReadFailsOrTheExecutionIsMissing_AnswersWithTheMappedProblem(t *testing.T) {
	cases := []struct {
		name       string
		getErr     error
		signalErr  error
		wantStatus int
		wantCode   string
	}{
		{name: "the instance read fails with a database error", getErr: errW26aDatabase, wantStatus: http.StatusInternalServerError, wantCode: "INTERNAL_ERROR"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			world := w26aNewLifecycleWorld(domain.InstanceStatusPaused, tc.getErr, tc.signalErr)

			rec := do(world.router, asAdmin(req(http.MethodPost, instancePath(testInstID)+"/resume", map[string]any{"record_version": 4})))

			p := w26aProblemOf(t, rec, tc.wantStatus)
			assert.Equal(t, tc.wantCode, p.Code)
			w26aRequireNoLeak(t, rec.Body.String())
			assert.NotContains(t, rec.Body.String(), "w26a-secret-run", "the Temporal text is not echoed")
			assert.NotContains(t, rec.Body.String(), "instance-resume", "the signal channel is not echoed")
			assert.Equal(t, []w26aLookup{{testTenantID, testInstID}}, world.repo.reads, "the instance is read once, in the caller's tenant")
			if tc.wantStatus == http.StatusInternalServerError {
				assert.Equal(t, w26aInternalDetail, p.Detail)
				assert.Empty(t, world.sdk.signals, "nothing reaches the workflow when the instance cannot be read")
				return
			}
			assert.Equal(t, "workflow execution not found", p.Detail)
			assert.Equal(t, "https://errors.bcbp.io/execution/invalid-instance-state", p.Type)
			require.Len(t, world.sdk.signals, 1, "the signal was tried once")
			sent := world.sdk.signals[0]
			assert.Equal(t, "tenant:subject:w26a", sent.workflowID)
			assert.Equal(t, "instance-resume:"+testInstID.String(), sent.channel)
			var wire w26aAdminWire
			w26aDecodePayload(t, sent.payload, &wire)
			assert.Equal(t, testUserID.String(), wire.AdminUserID)
			assert.EqualValues(t, 4, wire.RecordVersion)
			assert.Empty(t, wire.Reason)
			assert.Equal(t, "admin", wire.Initiator)
		})
	}
}

func TestCancelInstanceRoute_TheWorkflowIsGone_AnswersAcceptedAndEndsTheInstanceRow(t *testing.T) {
	for _, status := range []domain.InstanceStatus{domain.InstanceStatusRunning, domain.InstanceStatusPaused} {
		t.Run(string(status), func(t *testing.T) {
			rig := w19bNewRig(t)
			inst := rig.seed(status, 4)
			rig.sdk.signalErr = serviceerror.NewNotFound("workflow w26a-secret-run not found")

			rec := rig.post(instancePath(inst.ID)+"/cancel", map[string]any{"record_version": 4, "reason": "requested by the owner"})

			assert.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
			assert.NotContains(t, rec.Body.String(), "w26a-secret-run", "the Temporal text is not echoed")
			require.Len(t, rig.sdk.signals, 1, "the signal was tried once")
			assert.Equal(t, "instance-cancel:"+inst.ID.String(), rig.sdk.signals[0].channel)
			require.Len(t, rig.instances.writes, 1, "the row is ended through the status write terminate uses")
			assert.Equal(t, domain.InstanceStatusTerminated, rig.instances.writes[0].status)
			assert.Equal(t, domain.InstanceStatusTerminated, rig.instances.rows[inst.ID].Status)
		})
	}
}
