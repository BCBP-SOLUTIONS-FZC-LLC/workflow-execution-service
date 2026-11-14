package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
	sdkworkflow "go.temporal.io/sdk/workflow"

	outboundtemporal "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

type wkbootRegistration struct {
	kind string
	name string
	fn   any
}

// wkbootRecordingWorker records every registration it receives. Any other
// worker method panics through the nil embedded interface, which fails a test
// that starts registering something unexpected.
type wkbootRecordingWorker struct {
	worker.Worker
	registrations []wkbootRegistration
}

func (w *wkbootRecordingWorker) RegisterWorkflowWithOptions(fn any, opts sdkworkflow.RegisterOptions) {
	w.registrations = append(w.registrations, wkbootRegistration{kind: "workflow", name: opts.Name, fn: fn})
}

func (w *wkbootRecordingWorker) RegisterActivityWithOptions(fn any, opts activity.RegisterOptions) {
	w.registrations = append(w.registrations, wkbootRegistration{kind: "activity", name: opts.Name, fn: fn})
}

func (w *wkbootRecordingWorker) activities() map[string]any {
	out := map[string]any{}
	for _, r := range w.registrations {
		if r.kind == "activity" {
			out[r.name] = r.fn
		}
	}
	return out
}

func wkbootFuncName(fn any) string {
	return runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
}

// wkbootExpectedActivityBindings lists the wire name of each activity with the
// Deps method that must serve it.
var wkbootExpectedActivityBindings = map[string]string{
	"GetCompiledPlanActivity":      "GetCompiledPlan",
	"CreateTaskActivity":           "CreateTask",
	"UpdateInstanceNodesActivity":  "UpdateInstanceNodes",
	"ClaimAssignmentActivity":      "ClaimAssignment",
	"CompleteAssignmentActivity":   "CompleteAssignment",
	"DeferTaskActivity":            "DeferTask",
	"UpdateInstanceStatusActivity": "UpdateInstanceStatus",
	"RecordForceRouteActivity":     "RecordForceRoute",
	"RecordSLAWarningActivity":     "RecordSLAWarning",
	"RecordSLABreachActivity":      "RecordSLABreach",
	"PauseInstanceActivity":        "PauseInstance",
	"ResumeInstanceActivity":       "ResumeInstance",
	"CancelInstanceActivity":       "CancelInstance",
	"ReassignAssignmentActivity":   "ReassignAssignment",
	"UpdateTaskStatusActivity":     "UpdateTaskStatus",
}

func TestRegisterAll_RegistersTheWorkflowInterpreterUnderItsTypeNameBeforeTheActivities(t *testing.T) {
	rec := &wkbootRecordingWorker{}

	registerAll(rec, &outboundtemporal.Deps{})

	if len(rec.registrations) != 1+len(wkbootExpectedActivityBindings) {
		t.Fatalf("registrations = %d, want 1 workflow + %d activities", len(rec.registrations), len(wkbootExpectedActivityBindings))
	}
	first := rec.registrations[0]
	if first.kind != "workflow" {
		t.Fatalf("first registration is a %s (%q), want the workflow before any activity", first.kind, first.name)
	}
	if first.name != "Execute" {
		t.Errorf("workflow registered under %q, want %q", first.name, "Execute")
	}
	if got := wkbootFuncName(first.fn); !strings.HasSuffix(got, "/internal/workflow.Execute") {
		t.Errorf("workflow function = %q, want the interpreter's Execute", got)
	}
	for _, r := range rec.registrations[1:] {
		if r.kind != "activity" {
			t.Errorf("registration %q after the workflow is a %s, want only activities", r.name, r.kind)
		}
	}
}

func TestRegisterActivities_BindsEachActivityNameToItsOwnDepsMethod(t *testing.T) {
	rec := &wkbootRecordingWorker{}

	registerActivities(rec, &outboundtemporal.Deps{})

	if len(rec.registrations) != len(wkbootExpectedActivityBindings) {
		t.Fatalf("registered %d activities, want %d", len(rec.registrations), len(wkbootExpectedActivityBindings))
	}
	got := rec.activities()
	if len(got) != len(rec.registrations) {
		t.Fatalf("%d registrations used only %d distinct names: a name is registered twice", len(rec.registrations), len(got))
	}
	for name, method := range wkbootExpectedActivityBindings {
		fn, ok := got[name]
		if !ok {
			t.Errorf("activity %q is not registered", name)
			continue
		}
		want := "/internal/adapter/outbound/temporal.(*Deps)." + method + "-fm"
		if have := wkbootFuncName(fn); !strings.HasSuffix(have, want) {
			t.Errorf("activity %q is bound to %q, want the Deps method %s", name, have, method)
		}
	}
}

func TestRegisterActivities_RegistersEveryActivityNameDeclaredInThePortPackage(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "../../internal/core/port/activities.go", nil, 0)
	if err != nil {
		t.Fatalf("parse the port activity names: %v", err)
	}
	declared := map[string]string{}
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			vs := spec.(*ast.ValueSpec)
			for i, id := range vs.Names {
				if !strings.HasPrefix(id.Name, "Activity") || i >= len(vs.Values) {
					continue
				}
				lit, ok := vs.Values[i].(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					continue
				}
				value, err := strconv.Unquote(lit.Value)
				if err != nil {
					t.Fatalf("unquote %s: %v", id.Name, err)
				}
				declared[id.Name] = value
			}
		}
	}
	if len(declared) < len(wkbootExpectedActivityBindings) {
		t.Fatalf("found %d Activity constants in the port package, want at least %d: the scan is not reading them", len(declared), len(wkbootExpectedActivityBindings))
	}
	rec := &wkbootRecordingWorker{}

	registerActivities(rec, &outboundtemporal.Deps{})

	registered := rec.activities()
	for constName, value := range declared {
		if _, ok := registered[value]; !ok {
			t.Errorf("%s (%q) has no registered activity: a workflow scheduling it would wait forever for a worker", constName, value)
		}
	}
}

func TestDynamicQueueWorkerOptions_AllowTwoHundredConcurrentTasksAndCarryTheMetricsInterceptor(t *testing.T) {
	opts := dynamicQueueWorkerOptions

	if opts.MaxConcurrentActivityExecutionSize != 200 {
		t.Errorf("MaxConcurrentActivityExecutionSize = %d, want 200", opts.MaxConcurrentActivityExecutionSize)
	}
	if opts.MaxConcurrentWorkflowTaskExecutionSize != 200 {
		t.Errorf("MaxConcurrentWorkflowTaskExecutionSize = %d, want 200", opts.MaxConcurrentWorkflowTaskExecutionSize)
	}
	if opts.WorkerStopTimeout != 25*time.Second {
		t.Errorf("WorkerStopTimeout = %v, want 25s", opts.WorkerStopTimeout)
	}
	if len(opts.Interceptors) != 1 {
		t.Fatalf("interceptors = %d, want exactly the metrics interceptor", len(opts.Interceptors))
	}
	if got, want := reflect.TypeOf(opts.Interceptors[0]), reflect.TypeOf(observability.NewTemporalMetricsInterceptor()); got != want {
		t.Errorf("interceptor type = %v, want the Temporal metrics interceptor %v", got, want)
	}
}
