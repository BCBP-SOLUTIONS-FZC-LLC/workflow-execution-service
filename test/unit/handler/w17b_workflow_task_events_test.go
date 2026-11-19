package handler_test

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	adapterhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

var (
	errW17bInboxReadConnector  = errors.New("w17b: connector inbox read failed")
	errW17bInboxWriteConnector = errors.New("w17b: connector inbox write failed")
)

const (
	w17bIngestMetric   = "workflow_execution_service_internal_events_ingest_total"
	w17bUpstreamMetric = "workflow_execution_service_upstream_dependency_errors_total"
)

// w17bConnectorRig is a handler wired with recording fakes for the workflow-task events route.
type w17bConnectorRig struct {
	inbox      *w17bInbox
	publisher  *w17bPublisher
	principals *w17bPrincipals
	gate       *w17bGate
	log        *w17bLog
	order      *w17bOrder
	router     *gin.Engine
}

// newW17bConnectorRig builds the rig; withPublisher false leaves the connector stream publisher unset, and a
// non-nil resolver replaces the recording principal fake.
func newW17bConnectorRig(withPublisher bool, resolver port.AutomationPrincipalResolver) *w17bConnectorRig {
	order := &w17bOrder{}
	rig := &w17bConnectorRig{
		inbox:      newW17bInbox(),
		publisher:  &w17bPublisher{order: order},
		principals: &w17bPrincipals{principal: port.AutomationPrincipal{ID: uuid.New(), Subject: "w17b-principal-sub"}},
		gate:       &w17bGate{},
		log:        &w17bLog{},
		order:      order,
	}
	rig.inbox.order = order
	services := handler.Services{
		ProcessedEvents:   rig.inbox,
		ConnectorDispatch: rig.gate,
		Log:               rig.log,
	}
	if withPublisher {
		services.ConnectorEvents = rig.publisher
	}
	if resolver != nil {
		services.AutomationPrincipals = resolver
	} else {
		services.AutomationPrincipals = rig.principals
	}
	rig.router = newInternalRouter(handler.New(services))
	return rig
}

func w17bTaskPayload(connectorType any) map[string]any {
	return map[string]any{
		"workflow_instance_id": uuid.NewString(),
		"task_id":              uuid.NewString(),
		"node_key":             "dept-1/nodeA",
		"department_id":        uuid.NewString(),
		"connector_type":       connectorType,
		"resolved_inputs":      map[string]any{"bucket": "b1"},
		"output_mapping":       []map[string]any{{"source": "contentRef", "target": "docRef"}},
	}
}

func w17bTaskEnvelope(eventID, tenantID uuid.UUID, payload map[string]any) map[string]any {
	return envelope("WorkflowTaskCreated", eventID, tenantID, w17bEventTime, payload)
}

// Without a stream publisher the event is dropped with a warning that names the task, and recorded as handled
// without asking the token service for a principal.
func TestWorkflowTaskEvents_NoPublisherConfigured_DropsWithAWarningAndRecordsTheEvent(t *testing.T) {
	rig := newW17bConnectorRig(false, nil)
	tenantID, eventID := uuid.New(), uuid.New()
	payload := w17bTaskPayload("storage")

	w := w17bPost(rig.router, w17bWorkflowTaskPath, w17bTaskEnvelope(eventID, tenantID, payload))

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.Empty(t, rig.publisher.events)
	var warned bool
	for _, line := range rig.log.warns {
		if line.Fields["task_id"] == uuid.MustParse(payload["task_id"].(string)) {
			warned = true
		}
	}
	assert.True(t, warned, "a warning names the dropped task: %v", rig.log.warns)
	assert.Equal(t, []w17bInboxCall{{eventID, w17bConnectorConsumer}}, rig.inbox.checked)
	assert.Equal(t, []w17bInboxCall{{eventID, w17bConnectorConsumer}}, rig.inbox.marked, "the dropped event is recorded as handled")
	assert.Empty(t, rig.principals.tenants, "no principal is looked up for an event nothing will queue")
	assert.Empty(t, rig.gate.tenants)
}

// A connector_type of "" (like null or absent) marks a task for people: nothing is queued and no principal is read.
func TestWorkflowTaskEvents_EmptyConnectorType_IsNotAConnectorTask(t *testing.T) {
	cases := []struct {
		name string
		mut  func(payload map[string]any)
	}{
		{"empty string", func(p map[string]any) { p["connector_type"] = "" }},
		{"null", func(p map[string]any) { p["connector_type"] = nil }},
		{"absent", func(p map[string]any) { delete(p, "connector_type") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := newW17bConnectorRig(true, nil)
			payload := w17bTaskPayload("storage")
			tc.mut(payload)
			labels := map[string]string{"event_type": "WorkflowTaskCreated", "result": "ok"}
			before := obstest.Sum(t, w17bIngestMetric, labels)

			w := w17bPost(rig.router, w17bWorkflowTaskPath, w17bTaskEnvelope(uuid.New(), uuid.New(), payload))

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			assert.Empty(t, rig.publisher.events, "nothing is queued")
			assert.Empty(t, rig.principals.tenants)
			assert.Empty(t, rig.gate.tasks)
			assert.Equal(t, before+1, obstest.Sum(t, w17bIngestMetric, labels), "it is counted as handled")
		})
	}
}

// The consumer giving up mid-request does not stop the work: the stream entry is written and the inbox row is
// recorded on a context that is still alive.
func TestWorkflowTaskEvents_CallerDisconnectsMidRequest_StillWritesTheEntryAndTheInboxRow(t *testing.T) {
	rig := newW17bConnectorRig(true, nil)
	tenantID, eventID := uuid.New(), uuid.New()
	payload := w17bTaskPayload("storage")
	r := internalReq(http.MethodPost, w17bWorkflowTaskPath, w17bTaskEnvelope(eventID, tenantID, payload))
	callerCtx, hangUp := context.WithCancel(r.Context())
	defer hangUp()
	rig.principals.onCall = hangUp // the principal lookup is where the caller hangs up

	w := do(rig.router, r.WithContext(callerCtx))

	require.Error(t, callerCtx.Err(), "the caller did hang up during the principal lookup")
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Len(t, rig.publisher.events, 1)
	assert.Equal(t, eventID, rig.publisher.events[0].EventID)
	assert.Equal(t, tenantID, rig.publisher.events[0].TenantID)
	assert.Equal(t, []bool{true}, rig.publisher.ctxLive, "the stream write ran on a live context")
	assert.Equal(t, []bool{true}, rig.principals.ctxLive)
	assert.Equal(t, []w17bInboxCall{{eventID, w17bConnectorConsumer}}, rig.inbox.marked)
	assert.Equal(t, []bool{true}, rig.inbox.markCtxOK, "the inbox row was written on a live context")
}

// A schema_id with a payload that cannot be decoded is refused with 502 and nothing is looked up, queued or recorded.
func TestWorkflowTaskEvents_SchemaIDWithUndecodablePayload_Returns502(t *testing.T) {
	valid := []byte(`{"task_id":"` + uuid.NewString() + `"}`)
	cases := []struct {
		name       string
		data       any
		wantDetail string
	}{
		{"an object", map[string]any{"not": "a string"}, "schema-registry payload is not a base64 JSON string"},
		{"a number", 42, "schema-registry payload is not a base64 JSON string"},
		{"not base64", "###", "schema-registry payload base64 decode failed"},
		{"a frame with the wrong header", glueFrame(0x01, 0x00, valid), "schema-registry payload decode failed"},
		{"a compressed frame", glueFrame(0x03, 0x05, valid), "schema-registry payload decode failed"},
		{"a frame shorter than its header", base64.StdEncoding.EncodeToString([]byte{0x03, 0x00}), "schema-registry payload decode failed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := newW17bConnectorRig(true, nil)
			failed := map[string]string{"event_type": "unknown", "result": "decode_failed"}
			before := obstest.Sum(t, w17bIngestMetric, failed)

			w := w17bPost(rig.router, w17bWorkflowTaskPath,
				envelopeWithSchemaID("WorkflowTaskCreated", uuid.New(), uuid.New(), w17bEventTime, uuid.NewString(), tc.data))

			p := w17bDecodeProblem(t, w, http.StatusBadGateway)
			assert.Equal(t, "EVENT_DECODE_FAILED", p.Code)
			assert.Equal(t, tc.wantDetail, p.Detail)
			assert.NotContains(t, w.Body.String(), "illegal base64")
			assert.Equal(t, before+1, obstest.Sum(t, w17bIngestMetric, failed))
			assert.Empty(t, rig.publisher.events)
			assert.Empty(t, rig.principals.tenants)
			assert.Empty(t, rig.inbox.checked)
			assert.Empty(t, rig.inbox.marked)
		})
	}
}

// A failing inbox read stops the event: nothing is queued, and an error names the event.
func TestWorkflowTaskEvents_InboxReadFails_Answers503AndQueuesNothing(t *testing.T) {
	rig := newW17bConnectorRig(true, nil)
	rig.inbox.isErr = errW17bInboxReadConnector
	tenantID, eventID := uuid.New(), uuid.New()
	payload := w17bTaskPayload("storage")

	w := w17bPost(rig.router, w17bWorkflowTaskPath, w17bTaskEnvelope(eventID, tenantID, payload))

	require.Equal(t, http.StatusServiceUnavailable, w.Code, w.Body.String())
	assert.Empty(t, rig.publisher.events, "the event waits for a redelivery")
	assert.Equal(t, []w17bInboxCall{{eventID, w17bConnectorConsumer}}, rig.inbox.checked)
	assert.Empty(t, rig.inbox.marked)
	line, ok := rig.log.errorWith(eventID, "claiming or recording")
	require.True(t, ok, "an error names the event whose inbox read failed: %v", rig.log.errs)
	assert.Equal(t, errW17bInboxReadConnector.Error(), line.Fields["error"])
}

// A failing inbox write after the stream write answers 503, with an error naming the event.
func TestWorkflowTaskEvents_InboxWriteFails_Answers503AfterTheStreamWrite(t *testing.T) {
	rig := newW17bConnectorRig(true, nil)
	rig.inbox.markErr = errW17bInboxWriteConnector
	tenantID, eventID := uuid.New(), uuid.New()
	payload := w17bTaskPayload("storage")

	w := w17bPost(rig.router, w17bWorkflowTaskPath, w17bTaskEnvelope(eventID, tenantID, payload))

	require.Equal(t, http.StatusServiceUnavailable, w.Code, w.Body.String())
	require.Len(t, rig.publisher.events, 1)
	assert.Equal(t, eventID, rig.publisher.events[0].EventID)
	assert.Equal(t, payload["task_id"], rig.publisher.events[0].TaskID.String())
	assert.Equal(t, []w17bInboxCall{{eventID, w17bConnectorConsumer}}, rig.inbox.marked, "the write was attempted")
	assert.Equal(t, []string{"publish", "mark"}, rig.order.events, "the inbox row is written after the stream entry")
	line, ok := rig.log.errorWith(eventID, "claiming or recording")
	require.True(t, ok, "an error names the event whose inbox write failed: %v", rig.log.errs)
	assert.Equal(t, errW17bInboxWriteConnector.Error(), line.Fields["error"])
}

// The envelope tenant is the tenant of the dispatch check, of the principal lookup and of the stream entry,
// and the context of each carries that tenant's setting.
func TestWorkflowTaskEvents_EnvelopeTenantReachesTheLookupsAndTheStreamEntry(t *testing.T) {
	rig := newW17bConnectorRig(true, nil)
	tenantID, eventID := uuid.New(), uuid.New()
	payload := w17bTaskPayload("storage")
	taskID := uuid.MustParse(payload["task_id"].(string))

	w := w17bPost(rig.router, w17bWorkflowTaskPath, w17bTaskEnvelope(eventID, tenantID, payload))

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.Equal(t, []uuid.UUID{tenantID}, rig.gate.tenants)
	assert.Equal(t, []uuid.UUID{taskID}, rig.gate.tasks)
	assert.Equal(t, []string{tenantID.String()}, rig.gate.ctxTenants)
	assert.Equal(t, []uuid.UUID{tenantID}, rig.principals.tenants)
	assert.Equal(t, []string{tenantID.String()}, rig.principals.ctxTenants)
	require.Len(t, rig.publisher.events, 1)
	assert.Equal(t, tenantID, rig.publisher.events[0].TenantID)
	assert.Equal(t, []string{tenantID.String()}, rig.publisher.ctxTenants)
	assert.Equal(t, rig.principals.principal.ID, rig.publisher.events[0].PrincipalID)
	assert.Equal(t, "w17b-principal-sub", rig.publisher.events[0].PrincipalSub)
}

// w17bTokenService answers every principal lookup with the given handler and records the requests it got.
type w17bTokenService struct {
	mu       sync.Mutex
	paths    []string
	tenants  []string
	callerID []string
	methods  []string
}

func newW17bTokenService(t *testing.T, answer http.HandlerFunc) (*httptest.Server, *w17bTokenService) {
	t.Helper()
	rec := &w17bTokenService{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.mu.Lock()
		rec.methods = append(rec.methods, r.Method)
		rec.paths = append(rec.paths, r.URL.Path)
		rec.tenants = append(rec.tenants, r.Header.Get("x-tenant-id"))
		rec.callerID = append(rec.callerID, r.Header.Get("x-user-id"))
		rec.mu.Unlock()
		answer(w, r)
	}))
	t.Cleanup(srv.Close)
	return srv, rec
}

// A token service that cannot give a usable principal makes the answer 503 UPSTREAM_UNAVAILABLE so the
// consumer retries: nothing is queued, the event is not recorded, and the failure is counted per dependency.
func TestWorkflowTaskEvents_TokenServiceCannotAnswer_Returns503AndQueuesNothing(t *testing.T) {
	principalID := uuid.NewString()
	cases := []struct {
		name   string
		answer http.HandlerFunc
	}{
		{"answers 500", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusInternalServerError) }},
		{"answers 502", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusBadGateway) }},
		{"answers 403", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusForbidden) }},
		{"answers without a principal id", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"principal_sub":"sub-1","status":"active"}`))
		}},
		{"answers with the all-zero principal id", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"principal_id":"00000000-0000-0000-0000-000000000000","principal_sub":"sub-1","status":"active"}`))
		}},
		{"answers without a principal sub", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{"principal_id":"` + principalID + `","status":"active"}`))
		}},
		{"answers with text that is not JSON", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("password=hunter2")) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, tokens := newW17bTokenService(t, tc.answer)
			client := adapterhttp.NewAutomationPrincipalClient(srv.URL, 5*time.Second, time.Minute)
			rig := newW17bConnectorRig(true, client)
			tenantID, eventID := uuid.New(), uuid.New()
			dependency := map[string]string{"dependency": "iam"}
			before := obstest.Sum(t, w17bUpstreamMetric, dependency)

			w := w17bPost(rig.router, w17bWorkflowTaskPath, w17bTaskEnvelope(eventID, tenantID, w17bTaskPayload("storage")))

			p := w17bDecodeProblem(t, w, http.StatusServiceUnavailable)
			assert.Equal(t, "UPSTREAM_UNAVAILABLE", p.Code)
			assert.Equal(t, "an upstream service could not serve the request", p.Detail)
			assert.NotContains(t, w.Body.String(), "hunter2")
			assert.NotContains(t, w.Body.String(), srv.URL)
			assert.Equal(t, before+1, obstest.Sum(t, w17bUpstreamMetric, dependency))
			assert.Empty(t, rig.publisher.events, "nothing is queued")
			assert.Empty(t, rig.inbox.marked, "the event is not recorded, so the retry is processed")
			assert.Equal(t, []string{"/api/v1/internal/tenants/" + tenantID.String() + "/service-accounts/platform-automation"}, tokens.paths)
			assert.Equal(t, []string{tenantID.String()}, tokens.tenants)
			assert.Equal(t, []string{"GET"}, tokens.methods)
			assert.Equal(t, []string{"00000000-0000-0000-0000-0000000000a1"}, tokens.callerID, "the lookup is made as the IAM system user")
		})
	}
}

// A token service that cannot be reached, or whose base URL cannot be turned into a request, is also a 503.
func TestWorkflowTaskEvents_TokenServiceUnreachableOrMisconfigured_Returns503AndQueuesNothing(t *testing.T) {
	cases := []struct {
		name    string
		baseURL string
	}{
		{"connection refused", "http://127.0.0.1:1"},
		{"unterminated host", "http://[::1"},
		{"bad scheme", "ht!tp://token-service.iam.svc"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := adapterhttp.NewAutomationPrincipalClient(tc.baseURL, 5*time.Second, time.Minute)
			rig := newW17bConnectorRig(true, client)
			dependency := map[string]string{"dependency": "iam"}
			before := obstest.Sum(t, w17bUpstreamMetric, dependency)

			w := w17bPost(rig.router, w17bWorkflowTaskPath, w17bTaskEnvelope(uuid.New(), uuid.New(), w17bTaskPayload("storage")))

			p := w17bDecodeProblem(t, w, http.StatusServiceUnavailable)
			assert.Equal(t, "UPSTREAM_UNAVAILABLE", p.Code)
			assert.Equal(t, "an upstream service could not serve the request", p.Detail)
			for _, leaked := range []string{"token-service", "127.0.0.1", "[::1", "dial tcp", "refused", "build token service request"} {
				assert.NotContains(t, w.Body.String(), leaked)
			}
			assert.Equal(t, before+1, obstest.Sum(t, w17bUpstreamMetric, dependency), "counted against the token service's dependency")
			assert.Empty(t, rig.publisher.events, "nothing is queued")
			assert.Empty(t, rig.inbox.marked, "the lookup failed closed, so the event is not recorded")
		})
	}
}
