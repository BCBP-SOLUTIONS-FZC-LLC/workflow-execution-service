package contract_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	httpadapter "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/contract"
)

type fakeProcessed struct{}

func (fakeProcessed) IsProcessed(context.Context, uuid.UUID, string) (bool, error) { return false, nil }

func (fakeProcessed) RecordIfNew(context.Context, uuid.UUID, string, string) (bool, error) {
	return true, nil
}

func (fakeProcessed) PruneOlderThan(context.Context, time.Duration) (int64, error) { return 0, nil }

type fakeDelegation struct{ err error }

func (f fakeDelegation) Reroute(context.Context, port.DelegationRerouteInput) error  { return f.err }
func (f fakeDelegation) Reverse(context.Context, port.DelegationReversalInput) error { return f.err }

type fakeConnectorTasks struct{ err error }

func (f fakeConnectorTasks) Complete(context.Context, uuid.UUID, uuid.UUID, map[string]any) error {
	return f.err
}

func (f fakeConnectorTasks) Fail(context.Context, uuid.UUID, uuid.UUID, string, map[string]any) error {
	return f.err
}

func ingestRouter(services handler.Services) http.Handler {
	gin.SetMode(gin.TestMode)
	services.ProcessedEvents = fakeProcessed{}
	return httpadapter.NewRouter(httpadapter.RouterConfig{
		GinConfig: gincommon.Config{ServiceName: "workflow-execution-service", BuildVersion: "test", Domain: "workflow", Environment: "test"}, AppEnv: "test", InternalAPIToken: "internal-secret",
		Handler: handler.New(services),
		DB:      upDB{}, Cache: upPinger{}, Temporal: upPinger{},
	}).Handler()
}

func envelope(typ, tenant, data string) string {
	return `{"id":"` + uuid.NewString() + `","type":"` + typ + `","source":"iam","subject":"x/1","specversion":"1",` +
		`"tenant_id":"` + tenant + `","time":"` + time.Now().UTC().Format(time.RFC3339) + `","data":` + data + `}`
}

func delegationStarted(tenant string) string {
	return envelope("DelegationStarted", tenant, `{"delegation_id":"`+uuid.NewString()+`","delegator_id":"`+uuid.NewString()+
		`","delegate_id":"`+uuid.NewString()+`","scope":"all","starts_at":"`+time.Now().UTC().Format(time.RFC3339)+`"}`)
}

func TestInternalContract_Events(t *testing.T) {
	spec := contract.Load(t, internalSpec)
	const token = "internal-secret"
	tenant := uuid.NewString()
	ok := ingestRouter(handler.Services{Delegation: fakeDelegation{}})
	failing := ingestRouter(handler.Services{Delegation: fakeDelegation{err: errors.New("db down")}})
	delegation := "/api/v1/internal/events/delegation"
	oversized := `{"data":"` + strings.Repeat("a", 11<<20) + `"}`
	noTime := `{"id":"` + uuid.NewString() + `","type":"DelegationStarted","tenant_id":"` + tenant + `","data":{}}`

	for _, tc := range []exchange{
		{name: "delegation started", method: http.MethodPost, path: delegation, body: delegationStarted(tenant), token: token, want: http.StatusOK, validReq: true},
		{name: "unhandled type on delegation", method: http.MethodPost, path: delegation, body: envelope("UserDeleted", tenant, `{}`), token: token, want: http.StatusOK, validReq: true},
		{name: "unhandled type on user-profile", method: http.MethodPost, path: "/api/v1/internal/events/user-profile", body: envelope("DelegationStarted", tenant, `{}`), token: token, want: http.StatusOK, validReq: true},
		{name: "unhandled type on tenant", method: http.MethodPost, path: "/api/v1/internal/events/tenant", body: envelope("DelegationStarted", tenant, `{}`), token: token, want: http.StatusOK, validReq: true},
		{name: "unhandled type on workflow-task", method: http.MethodPost, path: "/api/v1/internal/events/workflow-task", body: envelope("DelegationStarted", tenant, `{}`), token: token, want: http.StatusOK, validReq: true},
		{name: "not json", method: http.MethodPost, path: delegation, body: `{`, token: token, want: http.StatusBadRequest},
		{name: "bad payload", method: http.MethodPost, path: delegation, body: envelope("DelegationStarted", tenant, `{}`), token: token, want: http.StatusBadRequest, validReq: true},
		{name: "no envelope time", method: http.MethodPost, path: delegation, body: noTime, token: token, want: http.StatusBadRequest},
		{name: "schema id without a decoder", method: http.MethodPost, path: delegation, body: strings.Replace(delegationStarted(tenant), `"data"`, `"dataschema":"s","data"`, 1), token: token, want: http.StatusBadGateway, validReq: true},
		{name: "no token", method: http.MethodPost, path: delegation, body: delegationStarted(tenant), want: http.StatusUnauthorized, validReq: true},
		{name: "oversized body", method: http.MethodPost, path: delegation, body: oversized, token: token, want: http.StatusRequestEntityTooLarge},
	} {
		t.Run(tc.name, func(t *testing.T) { run(t, spec, ok, tc) })
	}
	t.Run("processing failure", func(t *testing.T) {
		run(t, spec, failing, exchange{method: http.MethodPost, path: delegation, body: delegationStarted(tenant), token: token, want: http.StatusInternalServerError, validReq: true})
	})
}

func TestInternalContract_ConnectorTasks(t *testing.T) {
	spec := contract.Load(t, internalSpec)
	const token = "internal-secret"
	tenantID, taskID := uuid.NewString(), uuid.NewString()
	complete := "/api/v1/internal/connector-tasks/" + taskID + "/complete"
	fail := "/api/v1/internal/connector-tasks/" + taskID + "/fail"
	completeBody := `{"tenant_id":"` + tenantID + `","output":{"id":"42"}}`
	failBody := `{"tenant_id":"` + tenantID + `","error_class":"upstream_timeout","output":{"status":504}}`
	ok := ingestRouter(handler.Services{ConnectorTasks: fakeConnectorTasks{}})

	for _, tc := range []exchange{
		{name: "complete", method: http.MethodPost, path: complete, body: completeBody, token: token, want: http.StatusNoContent, validReq: true},
		{name: "complete without output", method: http.MethodPost, path: complete, body: `{"tenant_id":"` + tenantID + `"}`, token: token, want: http.StatusNoContent, validReq: true},
		{name: "complete without tenant", method: http.MethodPost, path: complete, body: `{}`, token: token, want: http.StatusBadRequest},
		{name: "complete bad task id", method: http.MethodPost, path: "/api/v1/internal/connector-tasks/nope/complete", body: completeBody, token: token, want: http.StatusBadRequest},
		{name: "complete without token", method: http.MethodPost, path: complete, body: completeBody, want: http.StatusUnauthorized, validReq: true},
		{name: "fail", method: http.MethodPost, path: fail, body: failBody, token: token, want: http.StatusNoContent, validReq: true},
		{name: "fail without error class", method: http.MethodPost, path: fail, body: `{"tenant_id":"` + tenantID + `"}`, token: token, want: http.StatusBadRequest},
		{name: "fail without token", method: http.MethodPost, path: fail, body: failBody, want: http.StatusUnauthorized, validReq: true},
	} {
		t.Run(tc.name, func(t *testing.T) { run(t, spec, ok, tc) })
	}

	for _, tc := range []struct {
		name string
		err  error
		want int
	}{
		{"unknown task", port.ErrTaskNotFound, http.StatusNotFound},
		{"task already resolved", port.ErrInvalidTaskState, http.StatusConflict},
		{"store failure", errors.New("db down"), http.StatusInternalServerError},
	} {
		router := ingestRouter(handler.Services{ConnectorTasks: fakeConnectorTasks{err: tc.err}})
		t.Run("complete: "+tc.name, func(t *testing.T) {
			run(t, spec, router, exchange{method: http.MethodPost, path: complete, body: completeBody, token: token, want: tc.want, validReq: true})
		})
		t.Run("fail: "+tc.name, func(t *testing.T) {
			run(t, spec, router, exchange{method: http.MethodPost, path: fail, body: failBody, token: token, want: tc.want, validReq: true})
		})
	}
}
