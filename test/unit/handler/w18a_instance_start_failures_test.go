package handler_test

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

const (
	w18aSubject = "SUBJ-18"
	w18aBizKey  = "TND-18"
)

func w18aStartWorld(t *testing.T) *w18aWorld {
	t.Helper()
	return w18aNewWorld(t, w18aPlanJSON(t, w18aStage("review", uuid.New())))
}

func (w *w18aWorld) startRequest() *http.Request {
	return req(http.MethodPost, instancesPath(), map[string]any{
		"subject_id": w18aSubject, "business_key": w18aBizKey, "workflow_version_id": w.versionID,
	})
}

func (w *w18aWorld) assertNothingStarted(t *testing.T) {
	t.Helper()
	assert.Zero(t, w.instances.committedCount(), "no instance row is left")
	assert.Empty(t, w.outbox.enqueued, "no started event is queued")
	assert.Empty(t, w.temporal.starts, "no workflow is started")
}

func TestStartInstance_LiveLookupFailsWithADatabaseError_Returns500AndWritesNothing(t *testing.T) {
	world := w18aStartWorld(t)
	world.instances.liveErr = errW18aDatabase

	rec := do(world.router, world.startRequest())

	require.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
	var body problemBody
	decodeJSON(t, rec.Body, &body)
	assert.Equal(t, "INTERNAL_ERROR", body.Code)
	assert.Equal(t, "an unexpected error occurred", body.Detail)
	assert.NotContains(t, rec.Body.String(), "hunter2", "the database error text is not echoed")
	assert.Equal(t, []w18aLookup{{testTenantID, w18aSubject, w18aBizKey}}, world.instances.lookups)
	assert.Empty(t, world.definitions.calls, "the plan is not read once the lookup failed")
	world.assertNothingStarted(t)
}

func TestStartInstance_TenantStateReadFails_Returns500AndWritesNothing(t *testing.T) {
	world := w18aStartWorld(t)
	world.svc.Gate = service.NewTenantStartGate(world.states, nil)
	world.states.err = errW18aTenantRead

	rec := do(world.router, world.startRequest())

	require.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
	var body problemBody
	decodeJSON(t, rec.Body, &body)
	assert.Equal(t, "INTERNAL_ERROR", body.Code)
	assert.Equal(t, "an unexpected error occurred", body.Detail)
	assert.NotContains(t, rec.Body.String(), "w18a tenant state", "the read error text is not echoed")
	assert.Equal(t, []uuid.UUID{testTenantID}, world.states.reads, "the caller's tenant state was read once")
	assert.Empty(t, world.definitions.calls)
	world.assertNothingStarted(t)
}

func TestStartInstance_TaskQueueRegistryFails_Returns500AndWritesNothing(t *testing.T) {
	cases := []struct {
		name  string
		setup func(w *w18aWorld)
		check func(t *testing.T, w *w18aWorld)
	}{
		{
			name:  "no registry is configured",
			setup: func(w *w18aWorld) { w.svc.Queues = nil },
			check: func(t *testing.T, w *w18aWorld) {
				assert.Empty(t, w.queues.lookups)
				assert.Empty(t, w.queues.registered)
			},
		},
		{
			name:  "the lookup of the tenant queue fails",
			setup: func(w *w18aWorld) { w.queues.getErr = errW18aDatabase },
			check: func(t *testing.T, w *w18aWorld) {
				require.Len(t, w.queues.lookups, 1)
				assert.Contains(t, w.queues.lookups[0], testTenantID.String(), "the lookup names the caller's tenant queue")
				assert.Empty(t, w.queues.registered)
			},
		},
		{
			name: "the registration of a granted tenant's queue fails",
			setup: func(w *w18aWorld) {
				w.svc.Grants = &w18aGrants{granted: true}
				w.queues.registerErr = errW18aDatabase
			},
			check: func(t *testing.T, w *w18aWorld) {
				require.Len(t, w.queues.registered, 1)
				assert.Equal(t, testTenantID, w.queues.registered[0].tenantID)
				assert.True(t, strings.Contains(w.queues.registered[0].queueName, testTenantID.String()), "the queue registered is the caller's tenant queue")
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			world := w18aStartWorld(t)
			tc.setup(world)

			rec := do(world.router, world.startRequest())

			require.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
			var body problemBody
			decodeJSON(t, rec.Body, &body)
			assert.Equal(t, "INTERNAL_ERROR", body.Code)
			assert.Equal(t, "an unexpected error occurred", body.Detail)
			assert.NotContains(t, rec.Body.String(), "hunter2", "the database error text is not echoed")
			assert.Equal(t, []w18aRead{{testTenantID, world.versionID}}, world.definitions.calls)
			tc.check(t, world)
			world.assertNothingStarted(t)
		})
	}
}

func TestStartInstance_OutboxEnqueueFails_Returns500LeavesNoRowAndTheRetryStarts(t *testing.T) {
	world := w18aStartWorld(t)
	world.outbox.err = errW18aOutbox

	rec := do(world.router, world.startRequest())

	require.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
	var body problemBody
	decodeJSON(t, rec.Body, &body)
	assert.Equal(t, "INTERNAL_ERROR", body.Code)
	assert.Equal(t, "an unexpected error occurred", body.Detail)
	assert.NotContains(t, rec.Body.String(), "outbox table locked", "the failure text is not echoed")
	require.Len(t, world.instances.creates, 1, "the insert was attempted inside the transaction")
	assert.Equal(t, testTenantID, world.instances.creates[0].TenantID)
	world.assertNothingStarted(t)

	world.outbox.err = nil
	rec = do(world.router, world.startRequest())

	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	assert.Equal(t, 1, world.instances.committedCount(), "the retry stores one row")
	require.Len(t, world.outbox.enqueued, 1)
	assert.Equal(t, testTenantID.String(), world.outbox.enqueued[0].TenantID)
	require.Len(t, world.temporal.starts, 1)
	assert.Equal(t, testTenantID, world.temporal.starts[0].TenantID)
	assert.Equal(t, []w18aLookup{
		{testTenantID, w18aSubject, w18aBizKey},
		{testTenantID, w18aSubject, w18aBizKey},
	}, world.instances.lookups, "the retry found no live instance, the first row was rolled back")
}

func TestStartInstance_RequestContextEndsDuringTheStartRetryWait_AnswersByHowItEnded(t *testing.T) {
	cases := []struct {
		name       string
		end        func() (context.Context, context.CancelFunc)
		wantStatus int
		wantCode   string
		wantDetail string
	}{
		{
			name: "the deadline passed",
			end: func() (context.Context, context.CancelFunc) {
				return context.WithDeadline(context.Background(), time.Unix(0, 0))
			},
			wantStatus: http.StatusServiceUnavailable, wantCode: "UPSTREAM_UNAVAILABLE", wantDetail: "request timed out",
		},
		{
			name: "the client cancelled",
			end: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, cancel
			},
			wantStatus: http.StatusInternalServerError, wantCode: "INTERNAL_ERROR", wantDetail: "an unexpected error occurred",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			world := w18aStartWorld(t)
			world.temporal.startFunc = func(context.Context, port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
				return port.StartWorkflowOutput{}, errW18aTemporal
			}
			ctx, cancel := tc.end()
			t.Cleanup(cancel)

			rec := do(world.router, world.startRequest().WithContext(ctx))

			require.Equal(t, tc.wantStatus, rec.Code, rec.Body.String())
			var body problemBody
			decodeJSON(t, rec.Body, &body)
			assert.Equal(t, tc.wantCode, body.Code)
			assert.Equal(t, tc.wantDetail, body.Detail)
			assert.NotContains(t, rec.Body.String(), "10.9.8.7", "the cluster address is not echoed")
			require.Len(t, world.temporal.starts, 1, "no further attempt once the request has ended")
			assert.Equal(t, testTenantID, world.temporal.starts[0].TenantID)
			require.Equal(t, 1, world.instances.committedCount(), "the committed row stays so the same request can resume")
			for _, inst := range world.instances.committed {
				assert.Equal(t, testTenantID, inst.TenantID)
				assert.Equal(t, domain.InstanceStatusRunning, inst.Status)
				assert.Empty(t, inst.TemporalRunID, "no run id is recorded")
			}
			assert.Empty(t, world.instances.runIDs)
		})
	}
}
