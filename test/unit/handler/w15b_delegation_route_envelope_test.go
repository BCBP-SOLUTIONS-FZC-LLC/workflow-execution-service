package handler_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

const w15bDelegationRoute = "/api/v1/internal/events/delegation"

type w15bReconcileCall struct {
	ctxErr      error
	hasDeadline bool
	deadline    time.Time
}

// w15bDelegationReconciler records each call's input and the state of the context it was given.
type w15bDelegationReconciler struct {
	reroutes   []port.DelegationRerouteInput
	reversals  []port.DelegationReversalInput
	rerouteCtx []w15bReconcileCall
	reverseCtx []w15bReconcileCall
}

func w15bCallState(ctx context.Context) w15bReconcileCall {
	deadline, hasDeadline := ctx.Deadline()
	return w15bReconcileCall{ctxErr: ctx.Err(), hasDeadline: hasDeadline, deadline: deadline}
}

func (f *w15bDelegationReconciler) Reroute(ctx context.Context, in port.DelegationRerouteInput) error {
	f.reroutes = append(f.reroutes, in)
	f.rerouteCtx = append(f.rerouteCtx, w15bCallState(ctx))
	return nil
}

func (f *w15bDelegationReconciler) Reverse(ctx context.Context, in port.DelegationReversalInput) error {
	f.reversals = append(f.reversals, in)
	f.reverseCtx = append(f.reverseCtx, w15bCallState(ctx))
	return nil
}

var _ port.DelegationReconciler = (*w15bDelegationReconciler)(nil)

type w15bProcessedMark struct {
	eventID  uuid.UUID
	consumer string
}

type w15bProcessedEvents struct{ marks []w15bProcessedMark }

func (f *w15bProcessedEvents) IsProcessed(context.Context, uuid.UUID, string) (bool, error) {
	return false, nil
}

func (f *w15bProcessedEvents) MarkProcessed(_ context.Context, eventID uuid.UUID, consumer string) error {
	f.marks = append(f.marks, w15bProcessedMark{eventID, consumer})
	return nil
}

func (f *w15bProcessedEvents) Prune(context.Context, time.Duration) (int64, error) { return 0, nil }

var _ port.ProcessedEventRepository = (*w15bProcessedEvents)(nil)

func w15bDelegationHandler(delegation *w15bDelegationReconciler, processed *w15bProcessedEvents) *handler.Handler {
	return handler.New(handler.Services{ProcessedEvents: processed, Delegation: delegation})
}

func w15bEnvelope(eventType string, eventID, tenantID uuid.UUID, schemaID string, data any) string {
	env := map[string]any{
		"id": eventID.String(), "type": eventType, "tenant_id": tenantID.String(),
		"time": time.Now().Format(time.RFC3339), "data": data,
	}
	if schemaID != "" {
		env["dataschema"] = schemaID
	}
	b, err := json.Marshal(env)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func w15bGlueFrame(version, compression byte, payload []byte) string {
	schema := uuid.New()
	frame := append([]byte{version, compression}, schema[:]...)
	return base64.StdEncoding.EncodeToString(append(frame, payload...))
}

func w15bStartedPayload(delegationID, delegatorID, delegateID, departmentID uuid.UUID) map[string]any {
	return map[string]any{
		"delegation_id": delegationID, "delegator_id": delegatorID, "delegate_id": delegateID,
		"scope": "department", "scope_id": departmentID, "starts_at": "2030-01-02T03:04:05Z",
	}
}

func w15bPostDelegation(router http.Handler, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, w15bDelegationRoute, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, r)
	return w
}

func TestHandleDelegationEvents_SchemaRegistryFrame_ReachesTheReconcilerDecoded(t *testing.T) {
	t.Parallel()

	tenantID, delegationID, delegatorID, delegateID, departmentID := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	cases := []struct {
		name      string
		eventType string
		payload   map[string]any
		check     func(t *testing.T, f *w15bDelegationReconciler)
	}{
		{
			name:      "DelegationStarted",
			eventType: "DelegationStarted",
			payload:   w15bStartedPayload(delegationID, delegatorID, delegateID, departmentID),
			check: func(t *testing.T, f *w15bDelegationReconciler) {
				require.Len(t, f.reroutes, 1)
				got := f.reroutes[0]
				assert.Equal(t, tenantID, got.TenantID)
				assert.Equal(t, delegationID, got.DelegationID)
				assert.Equal(t, delegatorID, got.DelegatorID)
				assert.Equal(t, delegateID, got.DelegateID)
				assert.Equal(t, "department", got.Scope)
				require.NotNil(t, got.ScopeID)
				assert.Equal(t, departmentID.String(), *got.ScopeID)
				assert.True(t, time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC).Equal(got.StartsAt))
				assert.Empty(t, f.reversals)
			},
		},
		{
			name:      "DelegationEnded",
			eventType: "DelegationEnded",
			payload: map[string]any{
				"delegation_id": delegationID, "delegator_id": delegatorID, "delegate_id": delegateID, "ended_reason": "revoked",
			},
			check: func(t *testing.T, f *w15bDelegationReconciler) {
				assert.Equal(t, []port.DelegationReversalInput{{
					TenantID: tenantID, DelegationID: delegationID, DelegatorID: delegatorID, DelegateID: delegateID, EndedReason: "revoked",
				}}, f.reversals)
				assert.Empty(t, f.reroutes)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			framed, err := json.Marshal(tc.payload)
			require.NoError(t, err)
			delegation, processed := &w15bDelegationReconciler{}, &w15bProcessedEvents{}
			router := w15bInternalRouter(w15bDelegationHandler(delegation, processed))
			eventID := uuid.New()

			w := w15bPostDelegation(router, w15bEnvelope(tc.eventType, eventID, tenantID, uuid.NewString(),
				w15bGlueFrame(0x03, 0x00, framed)))

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			tc.check(t, delegation)
			assert.Equal(t, []w15bProcessedMark{{eventID, port.ConsumerMembership}}, processed.marks)
		})
	}
}

func TestHandleDelegationEvents_PayloadNotBase64_Returns502(t *testing.T) {
	t.Parallel()

	delegation, processed := &w15bDelegationReconciler{}, &w15bProcessedEvents{}
	router := w15bInternalRouter(w15bDelegationHandler(delegation, processed))

	w := w15bPostDelegation(router, w15bEnvelope("DelegationStarted", uuid.New(), uuid.New(), uuid.NewString(), "###"))

	p := w15bDecodeProblem(t, w, http.StatusBadGateway)
	assert.Equal(t, "EVENT_DECODE_FAILED", p.Code)
	assert.Equal(t, "https://errors.bcbp.io/execution/event-decode-failed", p.Type)
	assert.Equal(t, "schema-registry payload base64 decode failed", p.Detail)
	assert.Empty(t, delegation.reroutes)
	assert.Empty(t, delegation.reversals)
	assert.Empty(t, processed.marks, "an undecodable event is not marked processed")
}

func TestHandleDelegationEvents_FrameTheDecoderRefuses_Returns502(t *testing.T) {
	t.Parallel()

	valid, err := json.Marshal(w15bStartedPayload(uuid.New(), uuid.New(), uuid.New(), uuid.New()))
	require.NoError(t, err)
	cases := []struct {
		name   string
		framed string
	}{
		{"wrong header byte", w15bGlueFrame(0x01, 0x00, valid)},
		{"compressed frame", w15bGlueFrame(0x03, 0x05, valid)},
		{"payload not JSON", w15bGlueFrame(0x03, 0x00, []byte("\x00\x01binary"))},
		{"shorter than the header", base64.StdEncoding.EncodeToString([]byte{0x03, 0x00})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			delegation, processed := &w15bDelegationReconciler{}, &w15bProcessedEvents{}
			router := w15bInternalRouter(w15bDelegationHandler(delegation, processed))

			w := w15bPostDelegation(router, w15bEnvelope("DelegationStarted", uuid.New(), uuid.New(), uuid.NewString(), tc.framed))

			p := w15bDecodeProblem(t, w, http.StatusBadGateway)
			assert.Equal(t, "EVENT_DECODE_FAILED", p.Code)
			assert.Equal(t, "https://errors.bcbp.io/execution/event-decode-failed", p.Type)
			assert.Equal(t, "schema-registry payload decode failed", p.Detail)
			assert.Empty(t, delegation.reroutes)
			assert.Empty(t, delegation.reversals)
			assert.Empty(t, processed.marks, "an undecodable event is not marked processed")
		})
	}
}

func TestHandleDelegationEvents_CallerGivesUp_HandlingContinuesUnderItsOwnDeadline(t *testing.T) {
	t.Parallel()

	tenantID := uuid.New()
	cases := []struct {
		name      string
		eventType string
		payload   map[string]any
		states    func(f *w15bDelegationReconciler) []w15bReconcileCall
	}{
		{
			name:      "DelegationStarted",
			eventType: "DelegationStarted",
			payload:   w15bStartedPayload(uuid.New(), uuid.New(), uuid.New(), uuid.New()),
			states:    func(f *w15bDelegationReconciler) []w15bReconcileCall { return f.rerouteCtx },
		},
		{
			name:      "DelegationEnded",
			eventType: "DelegationEnded",
			payload: map[string]any{
				"delegation_id": uuid.New(), "delegator_id": uuid.New(), "delegate_id": uuid.New(), "ended_reason": "expired",
			},
			states: func(f *w15bDelegationReconciler) []w15bReconcileCall { return f.reverseCtx },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			delegation, processed := &w15bDelegationReconciler{}, &w15bProcessedEvents{}
			router := w15bInternalRouter(w15bDelegationHandler(delegation, processed))
			eventID := uuid.New()
			r := httptest.NewRequest(http.MethodPost, w15bDelegationRoute,
				strings.NewReader(w15bEnvelope(tc.eventType, eventID, tenantID, "", tc.payload)))
			r.Header.Set("Content-Type", "application/json")
			gone, cancel := context.WithCancel(r.Context())
			cancel()
			w := httptest.NewRecorder()

			router.ServeHTTP(w, r.WithContext(gone))

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			states := tc.states(delegation)
			require.Len(t, states, 1)
			assert.NoError(t, states[0].ctxErr, "a caller that gave up must not cancel the handling")
			require.True(t, states[0].hasDeadline, "detached handling still needs its own bound")
			assert.LessOrEqual(t, time.Until(states[0].deadline), 10*time.Minute, "the bound is at most 10 minutes")
			assert.Equal(t, []w15bProcessedMark{{eventID, port.ConsumerMembership}}, processed.marks)
		})
	}
}
