package handler_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

type csfCancelInstances struct {
	port.InstanceRepository
	inst       *domain.Instance
	gotTenants []uuid.UUID
	gotIDs     []uuid.UUID
}

func (r *csfCancelInstances) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	r.gotTenants = append(r.gotTenants, tenantID)
	r.gotIDs = append(r.gotIDs, id)
	if id != r.inst.ID {
		return nil, domain.ErrNotFound
	}
	copied := *r.inst
	return &copied, nil
}

// csfCancelTemporal records each signal and, like a workflow that honours an
// accepted cancel, ends the instance with a bumped record version.
type csfCancelTemporal struct {
	port.TemporalClient
	instances    *csfCancelInstances
	signals      []string
	workflows    []string
	signalledIDs []uuid.UUID
}

func (c *csfCancelTemporal) SignalWorkflow(_ context.Context, workflowID string, instanceID uuid.UUID, signalName string, _ any) error {
	c.signals = append(c.signals, signalName)
	c.workflows = append(c.workflows, workflowID)
	c.signalledIDs = append(c.signalledIDs, instanceID)
	c.instances.inst.Status = domain.InstanceStatusTerminated
	c.instances.inst.RecordVersion++
	return nil
}

func TestCancelInstance_RepeatedWithANewKeyAfterTermination_IsRefusedNotRepeated(t *testing.T) {
	instances := &csfCancelInstances{inst: &domain.Instance{
		ID: testInstID, TenantID: testTenantID, Status: domain.InstanceStatusRunning,
		RecordVersion: 3, TemporalWorkflowID: "tenant:subject:key",
	}}
	temporal := &csfCancelTemporal{instances: instances}
	svc := &service.InstanceService{Instances: instances, Temporal: temporal}
	router := newRouter(handler.New(handler.Services{Instances: svc, Cache: newFakeCacheStore(), IdempotencyTTL: time.Hour}))

	cancel := func(key string, recordVersion int64) *httptest.ResponseRecorder {
		r := asAdmin(req(http.MethodPost, instancePath(testInstID)+"/cancel", map[string]any{
			"reason": "duplicate request", "record_version": recordVersion,
		}))
		r.Header.Set("Idempotency-Key", key)
		return do(router, r)
	}

	first := cancel("cancel-key-1", 3)
	require.Equal(t, http.StatusAccepted, first.Code, first.Body.String())
	require.Equal(t, []string{port.SignalInstanceCancel}, temporal.signals)
	assert.Equal(t, []string{"tenant:subject:key"}, temporal.workflows)
	assert.Equal(t, []uuid.UUID{testInstID}, temporal.signalledIDs)
	require.Equal(t, domain.InstanceStatusTerminated, instances.inst.Status)
	freshVersion := instances.inst.RecordVersion
	require.EqualValues(t, 4, freshVersion)

	second := cancel("cancel-key-2", freshVersion)

	require.Equal(t, http.StatusConflict, second.Code, second.Body.String())
	var body problemBody
	decodeJSON(t, second.Body, &body)
	assert.Equal(t, "INSTANCE_ALREADY_TERMINAL", body.Code)
	assert.Equal(t, []string{port.SignalInstanceCancel}, temporal.signals, "the second cancel must not signal the workflow again")
	assert.Equal(t, []uuid.UUID{testTenantID, testTenantID}, instances.gotTenants)
	assert.Equal(t, []uuid.UUID{testInstID, testInstID}, instances.gotIDs)
}
