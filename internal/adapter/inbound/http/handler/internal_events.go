package handler

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"
	pgdomain "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

// The three metric vars this file used to self-register via promauto
// (internal_events_ingest_total, delegation_reroute_duration_seconds,
// internal_events_last_received_timestamp) now live in internal/observability
// — nil until Register() runs at real process boot, hence the nil-guards
// below rather than a direct package-level var block here.

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
	consumerTemplate   = "template-sync-execution"
	consumerConnector  = "connector-stream-execution"
)

// parseEventID and parseTenantID share the "invalid event id"/"invalid
// tenant_id" 400 message every one of the six handlers needs for the
// envelope's own ID/TenantID fields, as opposed to a payload field.
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

func (h *Handler) HandleInternalEvent(c *gin.Context) {
	var env events.Envelope[json.RawMessage]
	if !bindJSON(c, &env) {
		incIngestTotal("unknown", "bad_payload")
		return
	}
	if env.Type == "" {
		h.badPayload(c, "unknown", "missing event type")
		return
	}

	if env.SchemaID != "" {
		decoded, ok := h.decodeSchemaRegistryPayload(c, env)
		if !ok {
			return
		}
		env.Payload = decoded
	}

	switch env.Type {
	case "delegation.started":
		h.handleDelegationStarted(c, env)
	case "delegation.ended":
		h.handleDelegationEnded(c, env)
	case "user.deleted":
		h.handleUserDeleted(c, env)
	case "user.availability.changed":
		h.handleUserAvailabilityChanged(c, env)
	case "tenant.state.changed":
		h.handleTenantStateChanged(c, env)
	case "workflow.template.published":
		h.handleTemplatePublished(c, env)
	case "workflow.task.created":
		h.handleWorkflowTaskCreated(c, env)
	default:
		h.logInfo("internal events: ignoring unhandled type", map[string]any{"event_type": env.Type})
		h.markOK(env.Type)
		c.Status(http.StatusOK)
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

// decodeFailed responds to a schema-registry decode failure with a 502: this
// is a retryable infra problem (missing/misconfigured decoder, or a registry
// hiccup reversing otherwise-valid encoded bytes), never a malformed client
// payload, so the shared event-delivery bridge that forwards these HTTP
// requests must retry it rather than treat it as permanently rejected like
// badPayload's 400 does.
func (h *Handler) decodeFailed(c *gin.Context, eventType, detail string) {
	incIngestTotal(eventType, "decode_failed")
	writeProblem(c, http.StatusBadGateway, CodeEventDecodeFailed, detail, nil)
}

// decodeSchemaRegistryPayload reverses what events.WithCodec does to an
// outbound envelope's Payload on the way out (base64-wrap the codec's native
// wire bytes as a JSON string, domain.WrapCodecPayload) so the typed
// per-event handlers below always see plain JSON. platform-events' own
// unwrap step (domain.UnwrapCodecPayload) isn't exported, so its two steps
// are replicated here.
func (h *Handler) decodeSchemaRegistryPayload(c *gin.Context, env events.Envelope[json.RawMessage]) (json.RawMessage, bool) {
	if h.eventDecoder == nil {
		h.decodeFailed(c, env.Type, "event carries a schema id but no event decoder is configured")
		return nil, false
	}

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

	decoded, err := h.eventDecoder.Decode(c.Request.Context(), env.SchemaID, raw)
	if err != nil {
		h.decodeFailed(c, env.Type, "schema-registry payload decode failed")
		return nil, false
	}
	return decoded, true
}

// respondOK finishes every successful dispatch path: dedup recorded last
// (LLD §6.3), then the standard ok metrics, then 200.
func (h *Handler) respondOK(c *gin.Context, eventID uuid.UUID, consumer, eventType string) {
	if _, err := h.processedEvents.RecordIfNew(c.Request.Context(), eventID, consumer, eventType); err != nil {
		h.logWarn("internal events: RecordIfNew failed after side effects committed", map[string]any{
			"event_type": eventType, "event_id": eventID, "error": err.Error(),
		})
	}
	h.markOK(eventType)
	c.Status(http.StatusOK)
}

// alreadyProcessed is the §6.3-permitted cheap up-front read: a genuine
// replay skips straight to ok/200 without calling the business port or
// RecordIfNew again.
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

func guCtx(c *gin.Context, tenantID string) context.Context {
	return pgcommon.WithGUCSet(c.Request.Context(), pgdomain.GUCSet{TenantID: tenantID})
}

// --- DelegationStarted ---

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
	const eventType = "delegation.started"
	var p delegationStartedPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		h.badPayload(c, eventType, "invalid delegation.started payload")
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

	ctx := guCtx(c, env.TenantID)
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

// --- DelegationEnded ---

type delegationEndedPayload struct {
	DelegationID string `json:"delegation_id"`
	DelegatorID  string `json:"delegator_id"`
	DelegateID   string `json:"delegate_id"`
	EndedReason  string `json:"ended_reason"`
}

func (h *Handler) handleDelegationEnded(c *gin.Context, env events.Envelope[json.RawMessage]) {
	const eventType = "delegation.ended"
	var p delegationEndedPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		h.badPayload(c, eventType, "invalid delegation.ended payload")
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

	// Unknown ended_reason values are logged and treated as a generic end
	// (LLD §6.2) — still processed, never an error.
	if !knownEndedReason(p.EndedReason) {
		h.logInfo("internal events: unknown DelegationEnded ended_reason, treating as generic end", map[string]any{
			"ended_reason": p.EndedReason,
		})
	}

	ctx := guCtx(c, env.TenantID)
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
	case "expired", "cancelled", "delegate_removed":
		return true
	default:
		return false
	}
}

// --- UserDeleted ---

type userDeletedPayload struct {
	UserID    string `json:"user_id"`
	DeletedAt string `json:"deleted_at"`
}

func (h *Handler) handleUserDeleted(c *gin.Context, env events.Envelope[json.RawMessage]) {
	const eventType = "user.deleted"
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

	ctx := guCtx(c, env.TenantID)
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

// --- UserAvailabilityChanged ---

type userAvailabilityChangedPayload struct {
	UserID         string  `json:"user_id"`
	Status         string  `json:"status"`
	OOOFrom        *string `json:"ooo_from"`
	OOOUntil       *string `json:"ooo_until"`
	DelegateUserID *string `json:"delegate_user_id"`
}

func (h *Handler) handleUserAvailabilityChanged(c *gin.Context, env events.Envelope[json.RawMessage]) {
	const eventType = "user.availability.changed"
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
	shouldApply, err := h.recency.ShouldApply(c.Request.Context(), scopeKey, env.Timestamp)
	if err != nil {
		h.reconcilerError(c, eventType, err)
		return
	}
	if !shouldApply {
		h.respondOK(c, eventID, consumerUser, eventType)
		return
	}

	ctx := guCtx(c, env.TenantID)
	if err := h.oooAvailability.Apply(ctx, port.UserAvailabilityInput{
		TenantID:       tenantID,
		UserID:         userID,
		Status:         p.Status,
		OOOFrom:        oooFrom,
		OOOUntil:       oooUntil,
		DelegateUserID: delegateUserID,
	}); err != nil {
		h.reconcilerError(c, eventType, err)
		return
	}

	// Recency commits only after Apply has actually succeeded — never
	// up front — so a transient Apply failure never leaves the guard
	// advanced past an event that was never really applied (which would
	// otherwise cause a legitimate retry to be silently skipped as stale).
	if err := h.recency.Commit(c.Request.Context(), scopeKey, env.Timestamp); err != nil {
		h.logWarn("internal events: recency Commit failed after Apply succeeded", map[string]any{
			"event_type": eventType, "scope_key": scopeKey, "error": err.Error(),
		})
	}

	h.respondOK(c, eventID, consumerUser, eventType)
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

// --- TenantStateChanged ---

type tenantStateChangedPayload struct {
	TenantID       string `json:"tenant_id"`
	Status         string `json:"status"`
	PreviousStatus string `json:"previous_status"`
	Plan           string `json:"plan"`
	PreviousPlan   string `json:"previous_plan"`
	ChangedAt      string `json:"changed_at"`
	Cause          string `json:"cause"`
}

func (h *Handler) handleTenantStateChanged(c *gin.Context, env events.Envelope[json.RawMessage]) {
	const eventType = "tenant.state.changed"
	var p tenantStateChangedPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		h.badPayload(c, eventType, "invalid tenant.state.changed payload")
		return
	}
	eventID, ok := h.parseEventID(c, eventType, env.ID)
	if !ok {
		return
	}
	// env.TenantID is authoritative for the RLS GUC; the payload's own
	// tenant_id is validated for shape but not cross-checked against it —
	// the LLD doesn't specify a mismatch behavior.
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

	// offboarded is terminal and never skipped, even if changed_at looks
	// stale (LLD §6.2 item 4, Appendix A #26) — every other transition goes
	// through the recency guard.
	scopeKey := "tenant:" + tenantID.String()
	if p.Status != "offboarded" {
		shouldApply, err := h.recency.ShouldApply(c.Request.Context(), scopeKey, changedAt)
		if err != nil {
			h.reconcilerError(c, eventType, err)
			return
		}
		if !shouldApply {
			h.respondOK(c, eventID, consumerMembership, eventType)
			return
		}
	}

	ctx := guCtx(c, env.TenantID)
	if err := h.tenantLifecycle.Apply(ctx, port.TenantLifecycleInput{
		TenantID:       tenantID,
		Status:         p.Status,
		PreviousStatus: p.PreviousStatus,
		Plan:           p.Plan,
		PreviousPlan:   p.PreviousPlan,
		ChangedAt:      changedAt,
		Cause:          p.Cause,
	}); err != nil {
		h.reconcilerError(c, eventType, err)
		return
	}

	// Recency commits once, after every sub-transaction Apply carried has
	// committed successfully — never per sub-transaction (LLD §6.2 item 4.3).
	if err := h.recency.Commit(c.Request.Context(), scopeKey, changedAt); err != nil {
		h.logWarn("internal events: recency Commit failed after Apply succeeded", map[string]any{
			"event_type": eventType, "scope_key": scopeKey, "error": err.Error(),
		})
	}

	h.respondOK(c, eventID, consumerMembership, eventType)
}

// --- workflow.template.published ---

type templatePublishedPayload struct {
	WorkflowID          string  `json:"workflow_id"`
	WorkflowKey         string  `json:"workflow_key"`
	VersionID           string  `json:"version_id"`
	VersionNumber       int     `json:"version_number"`
	ArtifactHash        string  `json:"artifact_hash"`
	PublishedBy         string  `json:"published_by"`
	PromotedFromVersion *string `json:"promoted_from_version_id"`
}

func (h *Handler) handleTemplatePublished(c *gin.Context, env events.Envelope[json.RawMessage]) {
	const eventType = "workflow.template.published"
	var p templatePublishedPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		h.badPayload(c, eventType, "invalid workflow.template.published payload")
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
	workflowID, err := uuid.Parse(p.WorkflowID)
	if err != nil {
		h.badPayload(c, eventType, "invalid workflow_id in payload")
		return
	}
	versionID, err := uuid.Parse(p.VersionID)
	if err != nil {
		h.badPayload(c, eventType, "invalid version_id in payload")
		return
	}
	publishedBy, err := uuid.Parse(p.PublishedBy)
	if err != nil {
		h.badPayload(c, eventType, "invalid published_by in payload")
		return
	}
	var promotedFromVersion *uuid.UUID
	if p.PromotedFromVersion != nil {
		v, err := uuid.Parse(*p.PromotedFromVersion)
		if err != nil {
			h.badPayload(c, eventType, "invalid promoted_from_version_id in payload")
			return
		}
		promotedFromVersion = &v
	}

	if h.alreadyProcessed(c, eventID, consumerTemplate, eventType) {
		return
	}

	// workflow_key is only unique per tenant (definition_service's workflow
	// table is UNIQUE(tenant_id, business_key)) — tenantID must be part of
	// the scope key, or two tenants publishing the same business key would
	// share one recency row and could skip each other's legitimate publishes.
	scopeKey := "template:" + tenantID.String() + ":" + p.WorkflowKey
	applied, err := h.recency.CheckAndCommit(c.Request.Context(), scopeKey, env.Timestamp)
	if err != nil {
		h.logWarn("internal events: recency check failed, proceeding fail-open", map[string]any{
			"event_type": eventType, "error": err.Error(),
		})
		applied = true
	}
	if applied {
		ctx := guCtx(c, env.TenantID)
		if err := h.templateCache.Prewarm(ctx, port.TemplatePublishedInput{
			TenantID:            tenantID,
			WorkflowID:          workflowID,
			WorkflowKey:         p.WorkflowKey,
			VersionID:           versionID,
			VersionNumber:       p.VersionNumber,
			ArtifactHash:        p.ArtifactHash,
			PublishedBy:         publishedBy,
			PromotedFromVersion: promotedFromVersion,
		}); err != nil {
			// Fail-open by design (LLD §6.2 item 5): a warm-fetch failure
			// logs and still returns 200 — the plan loads lazily on the next
			// instantiation anyway.
			h.logWarn("internal events: template cache prewarm failed, fail-open", map[string]any{
				"event_type": eventType, "error": err.Error(),
			})
		}
	}

	h.respondOK(c, eventID, consumerTemplate, eventType)
}
