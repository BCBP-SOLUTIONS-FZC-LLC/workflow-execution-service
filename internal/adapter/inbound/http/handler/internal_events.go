package handler

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"
	pgdomain "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

func incIngestTotal(eventType, result string) {
	if observability.InternalEventsIngestTotal != nil {
		observability.InternalEventsIngestTotal.WithLabelValues(eventType, result).Inc()
	}
}

func markLastReceived(eventType string) {
	if observability.InternalEventsLastReceivedTimestamp != nil {
		observability.InternalEventsLastReceivedTimestamp.WithLabelValues(eventType).SetToCurrentTime()
	}
}

func observeDelegationReroute(seconds float64) {
	if observability.DelegationRerouteDurationSeconds != nil {
		observability.DelegationRerouteDurationSeconds.Observe(seconds)
	}
}

const (
	consumerMembership = "membership-execution"
	consumerUser       = "user-execution"
	consumerConnector  = "connector-stream-execution"
)

const (
	eventTypeDelegationStarted       = "DelegationStarted"
	eventTypeDelegationEnded         = "DelegationEnded"
	eventTypeUserDeleted             = "UserDeleted"
	eventTypeUserAvailabilityChanged = "UserAvailabilityChanged"
	eventTypeMembershipRevoked       = "MembershipRevoked"
	eventTypeTenantStateChanged      = "TenantStateChanged"
	eventTypeTenantMembershipsPurged = "TenantMembershipsPurged"
	eventTypeWorkflowTaskCreated     = "WorkflowTaskCreated"
)

func HandledEventTypes() []string {
	return []string{
		eventTypeDelegationStarted, eventTypeDelegationEnded, eventTypeUserDeleted, eventTypeUserAvailabilityChanged,
		eventTypeMembershipRevoked, eventTypeTenantStateChanged, eventTypeTenantMembershipsPurged, eventTypeWorkflowTaskCreated,
	}
}

func (h *Handler) parseEventID(c *gin.Context, eventType, raw string) (uuid.UUID, bool) {
	id, err := uuid.Parse(raw)
	if err != nil {
		h.badPayload(c, eventType, "invalid event id")
		return uuid.Nil, false
	}
	return id, true
}

func (h *Handler) parseTenantID(c *gin.Context, eventType, raw string) (uuid.UUID, bool) {
	id, err := uuid.Parse(raw)
	if err != nil {
		h.badPayload(c, eventType, "invalid tenant_id")
		return uuid.Nil, false
	}
	return id, true
}

func (h *Handler) decodeEnvelope(c *gin.Context) (events.Envelope[json.RawMessage], bool) {
	var env events.Envelope[json.RawMessage]
	if !bindInternalJSON(c, &env) {
		incIngestTotal("unknown", "bad_payload")
		return env, false
	}
	if env.Type == "" {
		h.badPayload(c, "unknown", "missing event type")
		return env, false
	}
	if env.Timestamp.IsZero() {
		h.badPayload(c, env.Type, "missing or zero envelope timestamp")
		return env, false
	}

	if env.SchemaID != "" {
		decoded, ok := h.decodeSchemaRegistryPayload(c, env)
		if !ok {
			return env, false
		}
		env.Payload = decoded
	}
	return env, true
}

func (h *Handler) unhandledType(c *gin.Context, eventType string) {
	h.logWarn("internal events: ignoring unhandled type", map[string]any{"event_type": eventType})
	incIngestTotal(eventType, "unhandled_type")
	markLastReceived(eventType)
	c.Status(http.StatusOK)
}

func (h *Handler) HandleDelegationEvents(c *gin.Context) {
	defer detachRequest(c)()
	env, ok := h.decodeEnvelope(c)
	if !ok {
		return
	}
	switch env.Type {
	case eventTypeDelegationStarted:
		h.handleDelegationStarted(c, env)
	case eventTypeDelegationEnded:
		h.handleDelegationEnded(c, env)
	default:
		h.unhandledType(c, env.Type)
	}
}

func (h *Handler) HandleUserProfileEvents(c *gin.Context) {
	defer detachRequest(c)()
	env, ok := h.decodeEnvelope(c)
	if !ok {
		return
	}
	switch env.Type {
	case eventTypeUserDeleted:
		h.handleUserDeleted(c, env)
	case eventTypeUserAvailabilityChanged:
		h.handleUserAvailabilityChanged(c, env)
	case eventTypeMembershipRevoked:
		h.handleMembershipRevoked(c, env)
	default:
		h.unhandledType(c, env.Type)
	}
}

func (h *Handler) HandleTenantEvents(c *gin.Context) {
	defer detachRequest(c)()
	env, ok := h.decodeEnvelope(c)
	if !ok {
		return
	}
	switch env.Type {
	case eventTypeTenantStateChanged:
		h.handleTenantStateChanged(c, env)
	case eventTypeTenantMembershipsPurged:
		h.handleTenantMembershipsPurged(c, env)
	default:
		h.unhandledType(c, env.Type)
	}
}

func (h *Handler) HandleWorkflowTaskEvents(c *gin.Context) {
	defer detachRequest(c)()
	env, ok := h.decodeEnvelope(c)
	if !ok {
		return
	}
	switch env.Type {
	case eventTypeWorkflowTaskCreated:
		h.handleWorkflowTaskCreated(c, env)
	default:
		h.unhandledType(c, env.Type)
	}
}

func (h *Handler) markOK(eventType string) {
	incIngestTotal(eventType, "ok")
	markLastReceived(eventType)
}

func (h *Handler) badPayload(c *gin.Context, eventType, detail string) {
	incIngestTotal(eventType, "bad_payload")
	writeProblem(c, http.StatusBadRequest, CodeBadRequest, detail, nil)
}

func (h *Handler) reconcilerError(c *gin.Context, eventType string, err error) {
	incIngestTotal(eventType, "error")
	h.logError("internal events: reconciler call failed", map[string]any{"event_type": eventType, "error": err.Error()})
	writeProblem(c, http.StatusInternalServerError, CodeInternal, "failed to process event", nil)
}

func (h *Handler) decodeFailed(c *gin.Context, eventType, detail string) {
	incIngestTotal(eventType, "decode_failed")
	writeProblem(c, http.StatusBadGateway, CodeEventDecodeFailed, detail, nil)
}

func (h *Handler) decodeSchemaRegistryPayload(c *gin.Context, env events.Envelope[json.RawMessage]) (json.RawMessage, bool) {
	var b64 string
	if err := json.Unmarshal(env.Payload, &b64); err != nil {
		h.decodeFailed(c, env.Type, "schema-registry payload is not a base64 JSON string")
		return nil, false
	}
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		h.decodeFailed(c, env.Type, "schema-registry payload base64 decode failed")
		return nil, false
	}

	decoded, err := events.GlueDecodeCodec{}.Decode(c.Request.Context(), env.SchemaID, raw)
	if err != nil {
		h.decodeFailed(c, env.Type, "schema-registry payload decode failed")
		return nil, false
	}
	return decoded, true
}

func (h *Handler) respondOK(c *gin.Context, eventID uuid.UUID, consumer, eventType string) {
	if _, err := h.processedEvents.RecordIfNew(c.Request.Context(), eventID, consumer, eventType); err != nil {
		h.logWarn("internal events: RecordIfNew failed after side effects committed", map[string]any{
			"event_type": eventType, "event_id": eventID, "error": err.Error(),
		})
	}
	h.markOK(eventType)
	c.Status(http.StatusOK)
}

func (h *Handler) alreadyProcessed(c *gin.Context, eventID uuid.UUID, consumer, eventType string) bool {
	processed, err := h.processedEvents.IsProcessed(c.Request.Context(), eventID, consumer)
	if err != nil {
		h.logWarn("internal events: IsProcessed check failed, proceeding", map[string]any{
			"event_type": eventType, "event_id": eventID, "error": err.Error(),
		})
		return false
	}
	if !processed {
		return false
	}
	h.markOK(eventType)
	c.Status(http.StatusOK)
	return true
}

const eventHandlingTimeout = 10 * time.Minute

func detachRequest(c *gin.Context) context.CancelFunc {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(c.Request.Context()), eventHandlingTimeout)
	c.Request = c.Request.WithContext(ctx)
	return cancel
}

func guCtx(ctx context.Context, tenantID string) context.Context {
	return pgcommon.WithGUCSet(ctx, pgdomain.GUCSet{TenantID: tenantID})
}

type delegationStartedPayload struct {
	DelegationID string  `json:"delegation_id"`
	DelegatorID  string  `json:"delegator_id"`
	DelegateID   string  `json:"delegate_id"`
	Scope        string  `json:"scope"`
	ScopeID      *string `json:"scope_id"`
	StartsAt     string  `json:"starts_at"`
	EndsAt       *string `json:"ends_at"`
}

func (h *Handler) handleDelegationStarted(c *gin.Context, env events.Envelope[json.RawMessage]) {
	const eventType = eventTypeDelegationStarted
	var p delegationStartedPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		h.badPayload(c, eventType, "invalid DelegationStarted payload")
		return
	}
	eventID, ok := h.parseEventID(c, eventType, env.ID)
	if !ok {
		return
	}
	tenantID, ok := h.parseTenantID(c, eventType, env.TenantID)
	if !ok {
		return
	}
	delegationID, err := uuid.Parse(p.DelegationID)
	if err != nil {
		h.badPayload(c, eventType, "invalid delegation_id in payload")
		return
	}
	delegatorID, err := uuid.Parse(p.DelegatorID)
	if err != nil {
		h.badPayload(c, eventType, "invalid delegator_id in payload")
		return
	}
	delegateID, err := uuid.Parse(p.DelegateID)
	if err != nil {
		h.badPayload(c, eventType, "invalid delegate_id in payload")
		return
	}
	startsAt, err := time.Parse(time.RFC3339, p.StartsAt)
	if err != nil {
		h.badPayload(c, eventType, "invalid starts_at in payload")
		return
	}
	var endsAt *time.Time
	if p.EndsAt != nil {
		v, err := time.Parse(time.RFC3339, *p.EndsAt)
		if err != nil {
			h.badPayload(c, eventType, "invalid ends_at in payload")
			return
		}
		endsAt = &v
	}

	if h.alreadyProcessed(c, eventID, consumerMembership, eventType) {
		return
	}

	ctx := guCtx(c.Request.Context(), env.TenantID)
	start := env.Timestamp
	err = h.delegation.Reroute(ctx, port.DelegationRerouteInput{
		TenantID:     tenantID,
		DelegationID: delegationID,
		DelegatorID:  delegatorID,
		DelegateID:   delegateID,
		Scope:        p.Scope,
		ScopeID:      p.ScopeID,
		StartsAt:     startsAt,
		EndsAt:       endsAt,
	})
	if err != nil {
		h.reconcilerError(c, eventType, err)
		return
	}
	observeDelegationReroute(time.Since(start).Seconds())

	h.respondOK(c, eventID, consumerMembership, eventType)
}

type delegationEndedPayload struct {
	DelegationID string `json:"delegation_id"`
	DelegatorID  string `json:"delegator_id"`
	DelegateID   string `json:"delegate_id"`
	EndedReason  string `json:"ended_reason"`
}

func (h *Handler) handleDelegationEnded(c *gin.Context, env events.Envelope[json.RawMessage]) {
	const eventType = eventTypeDelegationEnded
	var p delegationEndedPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		h.badPayload(c, eventType, "invalid DelegationEnded payload")
		return
	}
	eventID, ok := h.parseEventID(c, eventType, env.ID)
	if !ok {
		return
	}
	tenantID, ok := h.parseTenantID(c, eventType, env.TenantID)
	if !ok {
		return
	}
	delegationID, err := uuid.Parse(p.DelegationID)
	if err != nil {
		h.badPayload(c, eventType, "invalid delegation_id in payload")
		return
	}
	delegatorID, err := uuid.Parse(p.DelegatorID)
	if err != nil {
		h.badPayload(c, eventType, "invalid delegator_id in payload")
		return
	}
	delegateID, err := uuid.Parse(p.DelegateID)
	if err != nil {
		h.badPayload(c, eventType, "invalid delegate_id in payload")
		return
	}

	if h.alreadyProcessed(c, eventID, consumerMembership, eventType) {
		return
	}

	if !knownEndedReason(p.EndedReason) {
		h.logInfo("internal events: unknown DelegationEnded ended_reason, treating as generic end", map[string]any{
			"ended_reason": p.EndedReason,
		})
	}

	ctx := guCtx(c.Request.Context(), env.TenantID)
	if err := h.delegation.Reverse(ctx, port.DelegationReversalInput{
		TenantID:     tenantID,
		DelegationID: delegationID,
		DelegatorID:  delegatorID,
		DelegateID:   delegateID,
		EndedReason:  p.EndedReason,
	}); err != nil {
		h.reconcilerError(c, eventType, err)
		return
	}

	h.respondOK(c, eventID, consumerMembership, eventType)
}

func knownEndedReason(reason string) bool {
	switch reason {
	case "expired", "cancelled", "delegate_removed", "review_expired":
		return true
	default:
		return false
	}
}

type userDeletedPayload struct {
	UserID    string `json:"user_id"`
	DeletedAt string `json:"deleted_at"`
}

func (h *Handler) handleUserDeleted(c *gin.Context, env events.Envelope[json.RawMessage]) {
	const eventType = eventTypeUserDeleted
	var p userDeletedPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		h.badPayload(c, eventType, "invalid UserDeleted payload")
		return
	}
	eventID, ok := h.parseEventID(c, eventType, env.ID)
	if !ok {
		return
	}
	tenantID, ok := h.parseTenantID(c, eventType, env.TenantID)
	if !ok {
		return
	}
	userID, err := uuid.Parse(p.UserID)
	if err != nil {
		h.badPayload(c, eventType, "invalid user_id in payload")
		return
	}
	deletedAt, err := time.Parse(time.RFC3339, p.DeletedAt)
	if err != nil {
		h.badPayload(c, eventType, "invalid deleted_at in payload")
		return
	}

	if h.alreadyProcessed(c, eventID, consumerUser, eventType) {
		return
	}

	ctx := guCtx(c.Request.Context(), env.TenantID)
	if err := h.userSafetyNet.VacateAssignments(ctx, port.UserDeletedInput{
		TenantID:  tenantID,
		UserID:    userID,
		DeletedAt: deletedAt,
	}); err != nil {
		h.reconcilerError(c, eventType, err)
		return
	}

	h.respondOK(c, eventID, consumerUser, eventType)
}

type userAvailabilityChangedPayload struct {
	UserID         string  `json:"user_id"`
	Status         string  `json:"status"`
	OOOFrom        *string `json:"ooo_from"`
	OOOUntil       *string `json:"ooo_until"`
	DelegateUserID *string `json:"delegate_user_id"`
}

func (h *Handler) handleUserAvailabilityChanged(c *gin.Context, env events.Envelope[json.RawMessage]) {
	const eventType = eventTypeUserAvailabilityChanged
	var p userAvailabilityChangedPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		h.badPayload(c, eventType, "invalid UserAvailabilityChanged payload")
		return
	}
	eventID, ok := h.parseEventID(c, eventType, env.ID)
	if !ok {
		return
	}
	tenantID, ok := h.parseTenantID(c, eventType, env.TenantID)
	if !ok {
		return
	}
	userID, err := uuid.Parse(p.UserID)
	if err != nil {
		h.badPayload(c, eventType, "invalid user_id in payload")
		return
	}
	oooFrom, ok := h.parseOptionalTime(c, eventType, p.OOOFrom)
	if !ok {
		return
	}
	oooUntil, ok := h.parseOptionalTime(c, eventType, p.OOOUntil)
	if !ok {
		return
	}
	var delegateUserID *uuid.UUID
	if p.DelegateUserID != nil {
		v, err := uuid.Parse(*p.DelegateUserID)
		if err != nil {
			h.badPayload(c, eventType, "invalid delegate_user_id in payload")
			return
		}
		delegateUserID = &v
	}

	if h.alreadyProcessed(c, eventID, consumerUser, eventType) {
		return
	}

	scopeKey := "user_availability:" + tenantID.String() + ":" + userID.String()
	input := port.UserAvailabilityInput{
		TenantID: tenantID, UserID: userID, Status: p.Status,
		OOOFrom: oooFrom, OOOUntil: oooUntil, DelegateUserID: delegateUserID,
	}
	if err := h.applyUserAvailabilityLocked(c, env, input, scopeKey); err != nil {
		h.reconcilerError(c, eventType, err)
		return
	}

	h.respondOK(c, eventID, consumerUser, eventType)
}

func (h *Handler) applyUserAvailabilityLocked(
	c *gin.Context, env events.Envelope[json.RawMessage], input port.UserAvailabilityInput, scopeKey string,
) error {
	return h.recency.WithLock(c.Request.Context(), scopeKey, func(ctx context.Context) error {
		shouldApply, err := h.recency.ShouldApply(ctx, scopeKey, env.Timestamp)
		if err != nil {
			return err
		}
		if !shouldApply {
			return nil
		}

		applyCtx := guCtx(ctx, env.TenantID)
		if err := h.oooAvailability.Apply(applyCtx, input); err != nil {
			return err
		}

		if err := h.recency.Commit(ctx, scopeKey, env.Timestamp); err != nil {
			h.logWarn("internal events: recency Commit failed after Apply succeeded", map[string]any{
				"event_type": eventTypeUserAvailabilityChanged, "scope_key": scopeKey, "error": err.Error(),
			})
		}
		return nil
	})
}

func (h *Handler) parseOptionalTime(c *gin.Context, eventType string, raw *string) (*time.Time, bool) {
	if raw == nil {
		return nil, true
	}
	v, err := time.Parse(time.RFC3339, *raw)
	if err != nil {
		h.badPayload(c, eventType, "invalid timestamp in payload")
		return nil, false
	}
	return &v, true
}

type tenantStateChangedPayload struct {
	TenantID       string `json:"tenant_id"`
	Status         string `json:"status"`
	PreviousStatus string `json:"previous_status"`
	Plan           string `json:"plan"`
	PreviousPlan   string `json:"previous_plan"`
	ChangedAt      string `json:"changed_at"`
	Cause          string `json:"cause"`
}

type membershipRevokedPayload struct {
	UserID string `json:"user_id"`
}

func (h *Handler) handleMembershipRevoked(c *gin.Context, env events.Envelope[json.RawMessage]) {
	const eventType = eventTypeMembershipRevoked
	var p membershipRevokedPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		h.badPayload(c, eventType, "invalid MembershipRevoked payload")
		return
	}
	eventID, ok := h.parseEventID(c, eventType, env.ID)
	if !ok {
		return
	}
	tenantID, ok := h.parseTenantID(c, eventType, env.TenantID)
	if !ok {
		return
	}
	userID, err := uuid.Parse(p.UserID)
	if err != nil {
		h.badPayload(c, eventType, "invalid user_id in payload")
		return
	}

	if h.alreadyProcessed(c, eventID, consumerMembership, eventType) {
		return
	}

	ctx := guCtx(c.Request.Context(), env.TenantID)
	if err := h.userSafetyNet.VacateAssignments(ctx, port.UserDeletedInput{
		TenantID:  tenantID,
		UserID:    userID,
		DeletedAt: env.Timestamp,
	}); err != nil {
		h.reconcilerError(c, eventType, err)
		return
	}

	h.respondOK(c, eventID, consumerMembership, eventType)
}

func (h *Handler) handleTenantMembershipsPurged(c *gin.Context, env events.Envelope[json.RawMessage]) {
	const eventType = eventTypeTenantMembershipsPurged
	eventID, ok := h.parseEventID(c, eventType, env.ID)
	if !ok {
		return
	}
	tenantID, ok := h.parseTenantID(c, eventType, env.TenantID)
	if !ok {
		return
	}

	if h.alreadyProcessed(c, eventID, consumerMembership, eventType) {
		return
	}

	ctx := guCtx(c.Request.Context(), env.TenantID)
	if err := h.tenantLifecycle.Apply(ctx, port.TenantLifecycleInput{
		TenantID:       tenantID,
		Status:         "offboarded",
		PreviousStatus: "active",
	}); err != nil {
		h.reconcilerError(c, eventType, err)
		return
	}

	h.respondOK(c, eventID, consumerMembership, eventType)
}

func (h *Handler) handleTenantStateChanged(c *gin.Context, env events.Envelope[json.RawMessage]) {
	const eventType = eventTypeTenantStateChanged
	var p tenantStateChangedPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		h.badPayload(c, eventType, "invalid TenantStateChanged payload")
		return
	}
	eventID, ok := h.parseEventID(c, eventType, env.ID)
	if !ok {
		return
	}
	tenantID, ok := h.parseTenantID(c, eventType, env.TenantID)
	if !ok {
		return
	}
	if _, err := uuid.Parse(p.TenantID); err != nil {
		h.badPayload(c, eventType, "invalid tenant_id in payload")
		return
	}
	changedAt, err := time.Parse(time.RFC3339, p.ChangedAt)
	if err != nil {
		h.badPayload(c, eventType, "invalid changed_at in payload")
		return
	}

	if h.alreadyProcessed(c, eventID, consumerMembership, eventType) {
		return
	}

	scopeKey := "tenant:" + tenantID.String()
	if err := h.applyTenantStateChangeLocked(c, env, p, tenantID, scopeKey, changedAt); err != nil {
		h.reconcilerError(c, eventType, err)
		return
	}

	h.respondOK(c, eventID, consumerMembership, eventType)
}

func (h *Handler) applyTenantStateChangeLocked(
	c *gin.Context, env events.Envelope[json.RawMessage], p tenantStateChangedPayload,
	tenantID uuid.UUID, scopeKey string, changedAt time.Time,
) error {
	return h.recency.WithLock(c.Request.Context(), scopeKey, func(ctx context.Context) error {
		if p.Status != "offboarded" {
			shouldApply, err := h.recency.ShouldApply(ctx, scopeKey, changedAt)
			if err != nil {
				return err
			}
			if !shouldApply {
				return nil
			}
		}

		applyCtx := guCtx(ctx, env.TenantID)
		if err := h.tenantLifecycle.Apply(applyCtx, port.TenantLifecycleInput{
			TenantID:       tenantID,
			Status:         p.Status,
			PreviousStatus: p.PreviousStatus,
			Plan:           p.Plan,
			PreviousPlan:   p.PreviousPlan,
			ChangedAt:      changedAt,
			Cause:          p.Cause,
		}); err != nil {
			return err
		}

		if err := h.recency.Commit(ctx, scopeKey, changedAt); err != nil {
			h.logWarn("internal events: recency Commit failed after Apply succeeded", map[string]any{
				"event_type": eventTypeTenantStateChanged, "scope_key": scopeKey, "error": err.Error(),
			})
		}
		return nil
	})
}
