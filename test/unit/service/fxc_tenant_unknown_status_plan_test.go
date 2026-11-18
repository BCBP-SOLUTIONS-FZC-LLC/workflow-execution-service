package service_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestTenantLifecycleReconciler_Apply_UnknownStatusStillAppliesThePlanChange(t *testing.T) {
	svc, instances, _, _, queues, outbox, temporal := newTenantLifecycleHarness()
	states := &fakeTenantStates{}
	svc.States = states
	tenantID := uuid.New()
	running := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning, TemporalWorkflowID: "tenant:running", RecordVersion: 1}
	instances.byID[running.ID] = running

	err := svc.Apply(context.Background(), port.TenantLifecycleInput{
		TenantID: tenantID, Status: "frozen", PreviousStatus: domain.TenantStatusActive,
		Plan: domain.PlanEnterprise, PreviousPlan: "pro",
	})

	require.NoError(t, err)
	require.Len(t, queues.registered, 1, "the plan change registers the dedicated queue")
	assert.Equal(t, registerCall{TenantID: tenantID, QueueName: domain.TenantTaskQueueName(tenantID)}, queues.registered[0])
	assert.Empty(t, states.recorded, "the unknown status is not kept")
	assert.Empty(t, temporal.signals, "no pause or resume is sent")
	assert.Empty(t, temporal.terminate)
	assert.Empty(t, outbox.enqueued)
	assert.Equal(t, domain.InstanceStatusRunning, running.Status, "instance status unchanged")
}

func TestTenantLifecycleReconciler_Apply_AnEmptyPlanDoesNotStopRoutingTheDedicatedQueue(t *testing.T) {
	svc, _, _, _, queues, _, _ := newTenantLifecycleHarness()
	tenantID := uuid.New()
	queueName := domain.TenantTaskQueueName(tenantID)
	queues.byName[queueName] = &domain.ActiveTaskQueue{TenantID: tenantID, QueueName: queueName, Routable: true}

	err := svc.Apply(context.Background(), port.TenantLifecycleInput{
		TenantID: tenantID, Status: domain.TenantStatusActive, PreviousStatus: domain.TenantStatusActive,
		Plan: "", PreviousPlan: domain.PlanEnterprise,
	})

	require.NoError(t, err)
	assert.Empty(t, queues.stopped, "an event that names no plan says nothing about the queue")
	assert.True(t, queues.byName[queueName].Routable)
}
