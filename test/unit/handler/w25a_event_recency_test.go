package handler_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// w25aGuard is a recency guard that fails the test on any use: the event types under test apply on the
// duplicate check alone and must never read or write recency.
type w25aGuard struct{ t *testing.T }

func (g w25aGuard) ShouldApply(context.Context, string, time.Time) (bool, error) {
	g.t.Error("the recency guard was read")
	return true, nil
}

func (g w25aGuard) ShouldApplyUnlessOlder(context.Context, string, time.Time) (bool, error) {
	g.t.Error("the recency guard was read")
	return true, nil
}

func (g w25aGuard) CheckAndCommit(context.Context, string, time.Time) (bool, error) {
	g.t.Error("the recency guard was written")
	return true, nil
}

func (g w25aGuard) Commit(context.Context, string, time.Time) error {
	g.t.Error("the recency guard was written")
	return nil
}

func (g w25aGuard) WithLock(ctx context.Context, _ string, fn func(context.Context) error) error {
	g.t.Error("the recency lock was taken")
	return fn(ctx)
}

var _ port.RecencyGuard = w25aGuard{}

// w25aInbox is a duplicate check that records the event ids and consumer names it is asked to mark.
type w25aInbox struct {
	marks     []uuid.UUID
	consumers []string
}

func (i *w25aInbox) IsProcessed(context.Context, uuid.UUID, string) (bool, error) { return false, nil }
func (i *w25aInbox) MarkProcessed(_ context.Context, id uuid.UUID, consumer string) error {
	i.marks = append(i.marks, id)
	i.consumers = append(i.consumers, consumer)
	return nil
}
func (i *w25aInbox) Process(ctx context.Context, env events.Envelope[json.RawMessage], consumer string, apply func(context.Context) error) error {
	return processThrough(ctx, i, env, consumer, apply)
}
func (i *w25aInbox) Prune(context.Context, time.Duration) (int64, error) { return 0, nil }

var _ port.ProcessedEventRepository = (*w25aInbox)(nil)

type w25aReconcilers struct {
	reroutes []port.DelegationRerouteInput
	reverses []port.DelegationReversalInput
	vacates  []port.UserDeletedInput
}

func (r *w25aReconcilers) Reroute(_ context.Context, in port.DelegationRerouteInput) error {
	r.reroutes = append(r.reroutes, in)
	return nil
}

func (r *w25aReconcilers) Reverse(_ context.Context, in port.DelegationReversalInput) error {
	r.reverses = append(r.reverses, in)
	return nil
}

func (r *w25aReconcilers) VacateAssignments(_ context.Context, in port.UserDeletedInput) error {
	r.vacates = append(r.vacates, in)
	return nil
}

type w25aPublisher struct {
	published []port.ConnectorTaskCreatedEvent
}

func (p *w25aPublisher) PublishTaskCreated(_ context.Context, e port.ConnectorTaskCreatedEvent) error {
	p.published = append(p.published, e)
	return nil
}

var _ port.ConnectorEventPublisher = (*w25aPublisher)(nil)

// A newer event and then an older one for the same entity both apply for every event type that has no recency
// guard: each reconciler call happens, each event is marked processed, and the guard is never touched.
func TestInternalEvents_TypesWithoutRecencyGuard_ApplyANewerAndThenAnOlderEvent(t *testing.T) {
	newer := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	older := newer.Add(-time.Hour)
	delegationID, delegatorID, delegateID, userID := uuid.New(), uuid.New(), uuid.New(), uuid.New()

	cases := []struct {
		name      string
		eventType string
		route     string
		data      func() map[string]any
		applied   func(r *w25aReconcilers) int
		consumer  string
		// check asserts what each reconciler call received: the event's tenant and, for a start, its own time.
		check func(t *testing.T, r *w25aReconcilers, tenantID uuid.UUID)
	}{
		{
			name: "delegation started", eventType: "DelegationStarted", route: "/api/v1/internal/events/delegation",
			data: func() map[string]any {
				return map[string]any{
					"delegation_id": delegationID, "delegator_id": delegatorID, "delegate_id": delegateID,
					"scope": "all", "starts_at": "2030-01-02T03:04:05Z",
				}
			},
			applied:  func(r *w25aReconcilers) int { return len(r.reroutes) },
			consumer: "membership-execution",
			check: func(t *testing.T, r *w25aReconcilers, tenantID uuid.UUID) {
				require.Len(t, r.reroutes, 2)
				assert.Equal(t, newer, r.reroutes[0].EmittedAt.UTC())
				assert.Equal(t, older, r.reroutes[1].EmittedAt.UTC(), "the older event is applied with its own time")
				for _, in := range r.reroutes {
					assert.Equal(t, tenantID, in.TenantID)
					assert.Equal(t, delegationID, in.DelegationID)
				}
			},
		},
		{
			name: "delegation ended", eventType: "DelegationEnded", route: "/api/v1/internal/events/delegation",
			data: func() map[string]any {
				return map[string]any{
					"delegation_id": delegationID, "delegator_id": delegatorID, "delegate_id": delegateID, "ended_reason": "expired",
				}
			},
			applied:  func(r *w25aReconcilers) int { return len(r.reverses) },
			consumer: "membership-execution",
			check: func(t *testing.T, r *w25aReconcilers, tenantID uuid.UUID) {
				require.Len(t, r.reverses, 2)
				for _, in := range r.reverses {
					assert.Equal(t, tenantID, in.TenantID)
					assert.Equal(t, delegationID, in.DelegationID)
				}
			},
		},
		{
			name: "user deleted", eventType: "UserDeleted", route: "/api/v1/internal/events/user-profile",
			data: func() map[string]any {
				return map[string]any{"user_id": userID.String(), "deleted_at": "2030-01-02T03:04:05Z"}
			},
			applied:  func(r *w25aReconcilers) int { return len(r.vacates) },
			consumer: "user-execution",
			check: func(t *testing.T, r *w25aReconcilers, tenantID uuid.UUID) {
				require.Len(t, r.vacates, 2)
				for _, in := range r.vacates {
					assert.Equal(t, tenantID, in.TenantID)
					assert.Equal(t, userID, in.UserID)
				}
			},
		},
		{
			name: "membership revoked", eventType: "MembershipRevoked", route: "/api/v1/internal/events/user-profile",
			data:     func() map[string]any { return map[string]any{"user_id": userID.String()} },
			applied:  func(r *w25aReconcilers) int { return len(r.vacates) },
			consumer: "membership-execution",
			check: func(t *testing.T, r *w25aReconcilers, tenantID uuid.UUID) {
				require.Len(t, r.vacates, 2)
				assert.Equal(t, newer, r.vacates[0].DeletedAt.UTC())
				assert.Equal(t, older, r.vacates[1].DeletedAt.UTC(), "the older revocation is applied with its own time")
				for _, in := range r.vacates {
					assert.Equal(t, tenantID, in.TenantID)
					assert.Equal(t, userID, in.UserID)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &w25aReconcilers{}
			inbox := &w25aInbox{}
			router := newInternalRouter(handler.New(handler.Services{
				ProcessedEvents: inbox, Recency: w25aGuard{t}, Delegation: rec, UserSafetyNet: rec,
			}))
			tenantID := uuid.New()
			newerID, olderID := uuid.New(), uuid.New()

			first := do(router, internalReq(http.MethodPost, tc.route, envelope(tc.eventType, newerID, tenantID, newer, tc.data())))
			second := do(router, internalReq(http.MethodPost, tc.route, envelope(tc.eventType, olderID, tenantID, older, tc.data())))

			require.Equal(t, http.StatusOK, first.Code, first.Body.String())
			require.Equal(t, http.StatusOK, second.Code, second.Body.String())
			assert.Equal(t, 2, tc.applied(rec), "the older event applies as well as the newer one")
			assert.Equal(t, []uuid.UUID{newerID, olderID}, inbox.marks)
			assert.Equal(t, []string{tc.consumer, tc.consumer}, inbox.consumers)
			tc.check(t, rec, tenantID)
		})
	}
}

// A WorkflowTaskCreated for a connector task is queued whatever its timestamp: a newer and then an older event
// for the same tenant both reach the stream, with no recency guard configured at all.
func TestInternalEvents_WorkflowTaskCreated_QueuesANewerAndThenAnOlderEvent(t *testing.T) {
	newer := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	older := newer.Add(-time.Hour)
	tenantID := uuid.New()
	inbox := &w25aInbox{}
	pub := &w25aPublisher{}
	router := newInternalRouter(handler.New(handler.Services{ProcessedEvents: inbox, ConnectorEvents: pub}))
	payload := func(taskID uuid.UUID) map[string]any {
		return map[string]any{
			"workflow_instance_id": uuid.NewString(), "task_id": taskID.String(), "node_key": "dept/n",
			"department_id": uuid.NewString(), "connector_type": "storage",
		}
	}
	newerTask, olderTask := uuid.New(), uuid.New()
	newerID, olderID := uuid.New(), uuid.New()

	first := do(router, internalReq(http.MethodPost, "/api/v1/internal/events/workflow-task", envelope("WorkflowTaskCreated", newerID, tenantID, newer, payload(newerTask))))
	second := do(router, internalReq(http.MethodPost, "/api/v1/internal/events/workflow-task", envelope("WorkflowTaskCreated", olderID, tenantID, older, payload(olderTask))))

	require.Equal(t, http.StatusOK, first.Code, first.Body.String())
	require.Equal(t, http.StatusOK, second.Code, second.Body.String())
	require.Len(t, pub.published, 2)
	assert.Equal(t, newerTask, pub.published[0].TaskID)
	assert.Equal(t, olderTask, pub.published[1].TaskID)
	assert.Equal(t, tenantID, pub.published[0].TenantID)
	assert.Equal(t, tenantID, pub.published[1].TenantID)
	assert.Equal(t, []uuid.UUID{newerID, olderID}, inbox.marks)
	assert.Equal(t, []string{"connector-stream-execution", "connector-stream-execution"}, inbox.consumers)
}
