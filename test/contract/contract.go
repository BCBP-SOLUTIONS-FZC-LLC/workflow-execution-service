// Package contract checks HTTP exchanges against an OpenAPI document, so a
// server's handlers and a client's requests are both held to the spec that
// governs them (execution LLD §5.3).
package contract

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers"
	"github.com/getkin/kin-openapi/routers/legacy"
)

// Spec is a loaded, valid OpenAPI document.
type Spec struct {
	router routers.Router
}

// Load reads and validates the document at path, failing the test if it is
// not a valid OpenAPI 3 document.
func Load(t testing.TB, path string) *Spec {
	t.Helper()
	loader := openapi3.NewLoader()
	doc, err := loader.LoadFromFile(path)
	if err != nil {
		t.Fatalf("load %s: %v", path, err)
	}
	if err := doc.Validate(loader.Context); err != nil {
		t.Fatalf("%s is not a valid OpenAPI document: %v", path, err)
	}
	doc.Servers = openapi3.Servers{{URL: "/"}}
	router, err := legacy.NewRouter(doc)
	if err != nil {
		t.Fatalf("route %s: %v", path, err)
	}
	return &Spec{router: router}
}

// options skips the security check: whether a token is right is the
// middleware's concern, tested on its own, not the document's.
var options = &openapi3filter.Options{AuthenticationFunc: openapi3filter.NoopAuthenticationFunc}

func (s *Spec) input(t testing.TB, req *http.Request) *openapi3filter.RequestValidationInput {
	t.Helper()
	route, params, err := s.router.FindRoute(req)
	if err != nil {
		t.Fatalf("%s %s is not in the spec: %v", req.Method, req.URL.Path, err)
	}
	return &openapi3filter.RequestValidationInput{Request: req, PathParams: params, Route: route, Options: options}
}

// Request fails the test unless req is one the spec accepts. req's body is
// read and restored.
func (s *Spec) Request(t testing.TB, req *http.Request) {
	t.Helper()
	body := readBody(t, req)
	restore(req, body)
	defer restore(req, body)
	if err := openapi3filter.ValidateRequest(context.Background(), s.input(t, req)); err != nil {
		t.Errorf("request %s %s breaks the spec: %v", req.Method, req.URL.Path, err)
	}
}

// Response fails the test unless the status, headers and body are a response
// the spec declares for req.
func (s *Spec) Response(t testing.TB, req *http.Request, status int, header http.Header, body []byte) {
	t.Helper()
	in := &openapi3filter.ResponseValidationInput{RequestValidationInput: s.input(t, req), Status: status, Header: header, Options: options}
	in.SetBodyBytes(body)
	if err := openapi3filter.ValidateResponse(context.Background(), in); err != nil {
		t.Errorf("response %d to %s %s breaks the spec: %v\n%s", status, req.Method, req.URL.Path, err, body)
	}
}

func readBody(t testing.TB, req *http.Request) []byte {
	t.Helper()
	if req.Body == nil {
		return nil
	}
	b, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatalf("read request body: %v", err)
	}
	return b
}

func restore(req *http.Request, body []byte) {
	if body == nil {
		return
	}
	req.Body = io.NopCloser(bytes.NewReader(body))
}
