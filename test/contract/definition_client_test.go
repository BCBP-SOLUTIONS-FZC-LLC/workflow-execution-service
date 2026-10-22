package contract_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	outboundhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/contract"
)

const definitionSpec = "../../api/contracts/definition-internal-openapi.yaml"

const compiledPlanPath = "/api/v1/internal/workflow-versions/{version_id}/compiled-plan"

// definitionServer answers each call with the next of replies, after holding
// the request to the copy of the Definition Service's spec.
func definitionServer(t *testing.T, spec *contract.Spec, replies ...func(w http.ResponseWriter)) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := int(calls.Add(1))
		spec.Request(t, r)
		if got := r.Header.Get("x-internal-token"); got != "definition-token" {
			t.Errorf("x-internal-token = %q, want the Definition Service's token", got)
		}
		reply := replies[len(replies)-1]
		if n <= len(replies) {
			reply = replies[n-1]
		}
		reply(w)
	}))
	t.Cleanup(srv.Close)
	return srv, &calls
}

func jsonReply(status int, body []byte) func(http.ResponseWriter) {
	return func(w http.ResponseWriter) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write(body)
	}
}

func definitionClient(url string) *outboundhttp.DefinitionClient {
	return outboundhttp.NewDefinitionClient(url, "definition-token", time.Second)
}

func TestDefinitionClient_ReadsTheSpecExample(t *testing.T) {
	spec := contract.Load(t, definitionSpec)
	example := spec.Example(t, http.MethodGet, compiledPlanPath, http.StatusOK)
	srv, _ := definitionServer(t, spec, jsonReply(http.StatusOK, example))

	got, err := definitionClient(srv.URL).GetCompiledWorkflow(context.Background(), uuid.New(), uuid.MustParse("11abcc22-0000-7000-8000-000000000001"))
	if err != nil {
		t.Fatalf("GetCompiledWorkflow: %v", err)
	}
	if got.WorkflowID != uuid.MustParse("8fa88cde-824c-47bc-836b-665cd42c2222") || got.VersionID != uuid.MustParse("11abcc22-0000-7000-8000-000000000001") ||
		got.VersionNumber != 3 || got.Status != "PUBLISHED" || !got.IsValid {
		t.Errorf("parsed %+v", got)
	}
	if got.CompiledPlanJSON == "" || got.CompiledPlanJSON[0] != '{' {
		t.Errorf("CompiledPlanJSON = %q, want the plan object's bytes", got.CompiledPlanJSON)
	}
}

func TestDefinitionClient_DraftHasNoPlan(t *testing.T) {
	spec := contract.Load(t, definitionSpec)
	draft := []byte(`{"workflow_id":"8fa88cde-824c-47bc-836b-665cd42c2222","version_id":"11abcc22-0000-7000-8000-000000000001","version_number":null,"status":"DRAFT","is_valid":true,"compiled_plan":null}`)
	srv, _ := definitionServer(t, spec, jsonReply(http.StatusOK, draft))

	got, err := definitionClient(srv.URL).GetCompiledWorkflow(context.Background(), uuid.New(), uuid.New())
	if err != nil {
		t.Fatalf("GetCompiledWorkflow: %v", err)
	}
	if got.Status != "DRAFT" || got.CompiledPlanJSON != "" || got.VersionNumber != 0 {
		t.Errorf("draft parsed as %+v", got)
	}
}

func TestDefinitionClient_Errors(t *testing.T) {
	spec := contract.Load(t, definitionSpec)
	problem := func(status int) func(http.ResponseWriter) {
		return jsonReply(status, []byte(`{"type":"https://api.workflow.platform/errors/x","title":"x","status":0,"detail":"x","instance":"/x","code":"X"}`))
	}
	unavailable := problem(http.StatusServiceUnavailable)
	ok := jsonReply(http.StatusOK, spec.Example(t, http.MethodGet, compiledPlanPath, http.StatusOK))

	for _, tc := range []struct {
		name      string
		replies   []func(http.ResponseWriter)
		want      error
		wantCalls int32
	}{
		{"not found is rejected at once", []func(http.ResponseWriter){problem(http.StatusNotFound)}, port.ErrDefinitionRejected, 1},
		{"a bad request is rejected at once", []func(http.ResponseWriter){problem(http.StatusBadRequest)}, port.ErrDefinitionRejected, 1},
		{"a refused token is rejected at once", []func(http.ResponseWriter){problem(http.StatusUnauthorized)}, port.ErrDefinitionRejected, 1},
		{"a 5xx is retried, then unavailable", []func(http.ResponseWriter){unavailable}, port.ErrDefinitionUnavailable, 3},
		{"a 5xx then success recovers", []func(http.ResponseWriter){unavailable, ok}, nil, 2},
		{"a body that is not JSON is rejected", []func(http.ResponseWriter){jsonReply(http.StatusOK, []byte(`<html>`))}, port.ErrDefinitionRejected, 1},
		{"identifiers that are not UUIDs are rejected", []func(http.ResponseWriter){jsonReply(http.StatusOK,
			[]byte(`{"workflow_id":"x","version_id":"y","version_number":1,"status":"PUBLISHED","is_valid":true,"compiled_plan":{}}`))}, port.ErrDefinitionRejected, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, calls := definitionServer(t, spec, tc.replies...)
			_, err := definitionClient(srv.URL).GetCompiledWorkflow(context.Background(), uuid.New(), uuid.New())
			if !errors.Is(err, tc.want) || (tc.want == nil && err != nil) {
				t.Errorf("err = %v, want %v", err, tc.want)
			}
			if calls.Load() != tc.wantCalls {
				t.Errorf("%d calls, want %d", calls.Load(), tc.wantCalls)
			}
		})
	}
}

func TestDefinitionClient_UnreachableIsUnavailable(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	srv.Close()
	if _, err := definitionClient(srv.URL).GetCompiledWorkflow(context.Background(), uuid.New(), uuid.New()); !errors.Is(err, port.ErrDefinitionUnavailable) {
		t.Errorf("err = %v, want ErrDefinitionUnavailable", err)
	}
}

func TestDefinitionClient_StopsWhenTheContextEnds(t *testing.T) {
	spec := contract.Load(t, definitionSpec)
	srv, calls := definitionServer(t, spec, jsonReply(http.StatusServiceUnavailable, []byte(`{}`)))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := definitionClient(srv.URL).GetCompiledWorkflow(ctx, uuid.New(), uuid.New()); !errors.Is(err, port.ErrDefinitionUnavailable) {
		t.Errorf("err = %v, want ErrDefinitionUnavailable", err)
	}
	if calls.Load() > 1 {
		t.Errorf("%d calls after the context ended, want at most 1", calls.Load())
	}
}
