package contract

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers"
	"github.com/getkin/kin-openapi/routers/legacy"
)

type Spec struct {
	doc    *openapi3.T
	router routers.Router
}

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
	return &Spec{doc: doc, router: router}
}

var options = &openapi3filter.Options{AuthenticationFunc: openapi3filter.NoopAuthenticationFunc, IncludeResponseStatus: true}

func (s *Spec) input(t testing.TB, req *http.Request) *openapi3filter.RequestValidationInput {
	t.Helper()
	route, params, err := s.router.FindRoute(req)
	if err != nil {
		t.Fatalf("%s %s is not in the spec: %v", req.Method, req.URL.Path, err)
	}
	return &openapi3filter.RequestValidationInput{Request: req, PathParams: params, Route: route, Options: options}
}

func (s *Spec) Request(t testing.TB, req *http.Request) {
	t.Helper()
	body := readBody(t, req)
	restore(req, body)
	defer restore(req, body)
	if err := openapi3filter.ValidateRequest(context.Background(), s.input(t, req)); err != nil {
		t.Errorf("request %s %s breaks the spec: %v", req.Method, req.URL.Path, err)
	}
}

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

func (s *Spec) Example(t testing.TB, method, path string, status int) []byte {
	t.Helper()
	item := s.doc.Paths.Find(path)
	if item == nil {
		t.Fatalf("%s is not in the spec", path)
		return nil
	}
	op := item.GetOperation(method)
	if op == nil {
		t.Fatalf("%s %s is not in the spec", method, path)
		return nil
	}
	resp := op.Responses.Status(status)
	if resp == nil || resp.Value == nil {
		t.Fatalf("%s %s declares no %d response", method, path, status)
		return nil
	}
	media := resp.Value.Content.Get("application/json")
	if media == nil {
		t.Fatalf("%s %s %d has no application/json body", method, path, status)
		return nil
	}
	example := media.Example
	if example == nil && media.Schema != nil && media.Schema.Value != nil {
		example = media.Schema.Value.Example
	}
	if example == nil {
		t.Fatalf("%s %s %d has no example", method, path, status)
	}
	b, err := json.Marshal(example)
	if err != nil {
		t.Fatalf("marshal example: %v", err)
	}
	return b
}

// Undeclared fails the test for each field of a JSON response body that the
// spec's schema for that status does not declare, so a response cannot grow a
// field the document omits.
func (s *Spec) Undeclared(t testing.TB, req *http.Request, status int, body []byte) {
	t.Helper()
	in := s.input(t, req)
	op := in.Route.Operation
	resp := op.Responses.Status(status)
	if resp == nil || resp.Value == nil {
		t.Errorf("%s %s declares no %d response", req.Method, req.URL.Path, status)
		return
	}
	media := resp.Value.Content.Get("application/json")
	if media == nil || media.Schema == nil || media.Schema.Value == nil {
		return
	}
	var data any
	if err := json.Unmarshal(body, &data); err != nil {
		t.Errorf("response is not JSON: %v", err)
		return
	}
	for _, path := range undeclaredFields(media.Schema.Value, data, "$") {
		t.Errorf("%s %s %d: %s is not declared in the spec", req.Method, req.URL.Path, status, path)
	}
}

func schemaProperties(sc *openapi3.Schema) openapi3.Schemas {
	props := openapi3.Schemas{}
	for k, v := range sc.Properties {
		props[k] = v
	}
	for _, group := range []openapi3.SchemaRefs{sc.AllOf, sc.OneOf, sc.AnyOf} {
		for _, ref := range group {
			if ref.Value == nil {
				continue
			}
			for k, v := range schemaProperties(ref.Value) {
				props[k] = v
			}
		}
	}
	return props
}

func undeclaredFields(sc *openapi3.Schema, data any, path string) []string {
	var out []string
	switch v := data.(type) {
	case map[string]any:
		props := schemaProperties(sc)
		open := sc.AdditionalProperties.Has != nil && *sc.AdditionalProperties.Has || sc.AdditionalProperties.Schema != nil
		for key, val := range v {
			child, declared := props[key]
			switch {
			case declared && child.Value != nil:
				out = append(out, undeclaredFields(child.Value, val, path+"."+key)...)
			case declared, open:
			default:
				out = append(out, path+"."+key)
			}
		}
	case []any:
		if sc.Items != nil && sc.Items.Value != nil {
			for i, item := range v {
				out = append(out, undeclaredFields(sc.Items.Value, item, path+"["+strconv.Itoa(i)+"]")...)
			}
		}
	}
	return out
}

// Operation is one method and path of the document.
type Operation struct {
	Method, Path string
	HasBody      bool
}

// Operations lists every operation of the document.
func (s *Spec) Operations() []Operation {
	var ops []Operation
	for path, item := range s.doc.Paths.Map() {
		for method, op := range item.Operations() {
			ops = append(ops, Operation{Method: method, Path: path, HasBody: op.RequestBody != nil})
		}
	}
	return ops
}
