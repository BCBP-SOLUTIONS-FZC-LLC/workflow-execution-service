package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

// fxeRecordingPrincipals records every lookup's tenant and can be made to fail.
type fxeRecordingPrincipals struct {
	mu        sync.Mutex
	principal uuid.UUID
	err       error
	tenants   []uuid.UUID
}

func (r *fxeRecordingPrincipals) AutomationPrincipal(_ context.Context, tenantID uuid.UUID) (port.AutomationPrincipal, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tenants = append(r.tenants, tenantID)
	return port.AutomationPrincipal{ID: r.principal}, r.err
}

func (r *fxeRecordingPrincipals) lookups() []uuid.UUID {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]uuid.UUID(nil), r.tenants...)
}

// fxeWireField reads a signal field under either spelling of its name.
func fxeWireField(wire map[string]any, names ...string) any {
	for _, n := range names {
		if v, ok := wire[n]; ok {
			return v
		}
	}
	return nil
}

type fxeCallback struct {
	name string
	call func(*service.ConnectorTaskService, *domain.Task) error
	// outcome is the reservation this callback owns; other is the opposite one.
	outcome, other string
}

func fxeCallbacks() []fxeCallback {
	return []fxeCallback{
		{
			name: "complete", outcome: "complete", other: "fail",
			call: func(s *service.ConnectorTaskService, task *domain.Task) error {
				return s.Complete(context.Background(), task.TenantID, task.ID, map[string]any{})
			},
		},
		{
			name: "fail", outcome: "fail", other: "complete",
			call: func(s *service.ConnectorTaskService, task *domain.Task) error {
				return s.Fail(context.Background(), task.TenantID, task.ID, "upstream_error", nil)
			},
		},
	}
}

func fxeService(task *domain.Task, principals port.AutomationPrincipalResolver) (
	*service.ConnectorTaskService, *fakeTemporalClient, *fakeCacheStore, *fakeAssignmentRepo,
) {
	temporal := &fakeTemporalClient{}
	cache := newFakeCacheStore()
	assignments := newFakeAssignmentRepo()
	svc := newConnectorTaskServiceWith(task, instanceWithStatus(task, domain.InstanceStatusRunning), temporal, cache, assignments)
	svc.Principals = principals
	return svc, temporal, cache, assignments
}

func TestConnectorTaskService_DeliveredOutcomeIsAcknowledgedWithoutResolvingThePrincipal(t *testing.T) {
	t.Parallel()

	for _, cb := range fxeCallbacks() {
		for _, delivered := range []string{cb.outcome, cb.other} {
			t.Run(cb.name+" after delivered "+delivered, func(t *testing.T) {
				task := connectorTask("storage", domain.TaskStatusReady, `{}`)
				principals := &fxeRecordingPrincipals{err: errors.New("token service down")}
				svc, temporal, cache, assignments := fxeService(task, principals)
				key := cbfixKey(delivered, task)
				cache.values[key] = "delivered"

				require.NoError(t, cb.call(svc, task), "the outcome was delivered already, so the repeat is acknowledged")

				assert.Empty(t, principals.lookups(), "an acknowledged repeat makes no outside call")
				assert.Empty(t, temporal.signals)
				assert.Empty(t, assignments.byID, "nothing is written for a repeat")
				assert.Equal(t, map[string]string{key: "delivered"}, cache.values, "the delivered mark is left as it was")
			})
		}
	}
}

func TestConnectorTaskService_InFlightOutcomeIsRetriedLaterWithoutResolvingThePrincipal(t *testing.T) {
	t.Parallel()

	for _, cb := range fxeCallbacks() {
		for _, reserved := range []string{cb.outcome, cb.other} {
			t.Run(cb.name+" while "+reserved+" reserved", func(t *testing.T) {
				task := connectorTask("storage", domain.TaskStatusReady, `{}`)
				principals := &fxeRecordingPrincipals{err: errors.New("token service down")}
				svc, temporal, cache, _ := fxeService(task, principals)
				key := cbfixKey(reserved, task)
				cache.values[key] = "reserved"

				require.ErrorIs(t, cb.call(svc, task), port.ErrCallbackInFlight)

				assert.Empty(t, principals.lookups())
				assert.Empty(t, temporal.signals)
				assert.Equal(t, map[string]string{key: "reserved"}, cache.values)
			})
		}
	}
}

// A fresh callback still needs the principal; when the lookup fails the error
// comes back and no reservation is taken, so the worker's retry starts clean.
func TestConnectorTaskService_FreshCallbackWithTheTokenServiceDownReturnsTheLookupError(t *testing.T) {
	t.Parallel()

	boom := errors.New("token service down")
	for _, cb := range fxeCallbacks() {
		t.Run(cb.name, func(t *testing.T) {
			task := connectorTask("storage", domain.TaskStatusReady, `{}`)
			principals := &fxeRecordingPrincipals{err: boom}
			svc, temporal, cache, assignments := fxeService(task, principals)

			require.ErrorIs(t, cb.call(svc, task), boom)

			assert.Equal(t, []uuid.UUID{task.TenantID}, principals.lookups())
			assert.Empty(t, temporal.signals)
			assert.Empty(t, assignments.byID)
			assert.Empty(t, cache.values, "no reservation is left for the retry to trip over")
		})
	}
}

func TestConnectorTaskService_FreshCallbackWithTheTokenServiceUpSignalsOnceForThePrincipal(t *testing.T) {
	t.Parallel()

	wantSignal := map[string]string{"complete": "stage-transition", "fail": "stage-fail"}
	for _, cb := range fxeCallbacks() {
		t.Run(cb.name, func(t *testing.T) {
			task := connectorTask("storage", domain.TaskStatusReady, `{}`)
			principals := &fxeRecordingPrincipals{principal: testConnectorActorID}
			svc, temporal, cache, assignments := fxeService(task, principals)

			require.NoError(t, cb.call(svc, task))

			assert.Equal(t, []uuid.UUID{task.TenantID}, principals.lookups())
			require.Len(t, temporal.signals, 1)
			assert.Equal(t, wantSignal[cb.name], temporal.signals[0].SignalName)
			assert.Equal(t, task.WorkflowInstanceID, temporal.signals[0].InstanceID)
			payload, err := json.Marshal(temporal.signals[0].Payload)
			require.NoError(t, err)
			var wire map[string]any
			require.NoError(t, json.Unmarshal(payload, &wire))
			if cb.name == "complete" {
				assert.Equal(t, testConnectorActorID.String(), wire["UserID"])
			}
			assert.Equal(t, "dept-1", fxeWireField(wire, "DeptID", "dept_id"))
			assert.Equal(t, "nodeA", fxeWireField(wire, "NodeID", "node_id"))
			active, err := assignments.ListActiveByTask(context.Background(), task.TenantID, task.ID)
			require.NoError(t, err)
			require.Len(t, active, 1)
			assert.Equal(t, testConnectorActorID, active[0].UserID)
			assert.Equal(t, "delivered", cache.values[cbfixKey(cb.outcome, task)])
		})
	}
}
