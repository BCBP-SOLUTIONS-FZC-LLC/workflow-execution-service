//go:build compat

package compat_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/connectors/aliasconfig"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"

	outboundhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// minRunnableFixtures is how many of Definition's own BPMN fixtures publish
// standalone (without modules) and must run here; a fixture that stops
// compiling shrinks the set and fails this floor instead of skipping silently.
const minRunnableFixtures = 13

// newUser is a UUID v7, the form Definition requires for a candidate user.
func newUser() uuid.UUID { return uuid.Must(uuid.NewV7()) }

func twoStage(t *testing.T, assignee, department uuid.UUID) string {
	t.Helper()
	b := readFixture(t, filepath.Join("testdata", "two-stage.bpmn"))
	b = strings.ReplaceAll(b, "{{ASSIGNEE}}", assignee.String())
	return strings.ReplaceAll(b, "{{DEPARTMENT}}", department.String())
}

func TestCompat(t *testing.T) {
	e := setup(t)

	t.Run("every Definition fixture publishes and runs", func(t *testing.T) { e.everyFixtureRuns(t) })
	t.Run("a two-stage template runs to completion", func(t *testing.T) { e.roundTrip(t) })
	t.Run("a template calling a published library module runs through the module", func(t *testing.T) { e.libraryCall(t) })
	t.Run("archive waits for live instances", func(t *testing.T) { e.archiveGuard(t) })
	t.Run("a revoked membership invalidates the version and pauses the user's work", func(t *testing.T) { e.revocation(t) })
	t.Run("a revoked tenant membership invalidates the version and pauses the user's work", func(t *testing.T) { e.tenantRevocation(t) })
	t.Run("each service accepts only the other's configured token", func(t *testing.T) { e.tokens(t) })
	t.Run("a version that cannot start is refused with the documented code", func(t *testing.T) { e.unstartable(t) })
	t.Run("a connector alias written in Definition reaches the connector worker", func(t *testing.T) { e.aliases(t) })
}

func definitionFixtures(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	for _, dir := range []string{"test/contract/testdata", "test/unit/service/testdata"} {
		m, err := filepath.Glob(filepath.Join(root, dir, "*.bpmn"))
		if err != nil {
			t.Fatalf("glob: %v", err)
		}
		out = append(out, m...)
	}
	sort.Strings(out)
	return out
}

func (e *env) everyFixtureRuns(t *testing.T) {
	tn := newTenant()
	client := outboundhttp.NewDefinitionClient(e.definitionURL, definitionToken, 5*time.Second)
	ran := 0
	for _, path := range definitionFixtures(t, e.definitionDir) {
		name := filepath.Base(filepath.Dir(filepath.Dir(path))) + "/" + filepath.Base(path)
		p, a := e.createDraft(t, tn, readFixture(t, path))
		if a.status != http.StatusCreated {
			t.Logf("%s: not standalone (create %d %s %v)", name, a.status, a.code(t), a.reasons())
			continue
		}
		if a := e.publish(t, tn, p); a.status != http.StatusOK {
			t.Logf("%s: not standalone (publish %d %s %v)", name, a.status, a.code(t), a.reasons())
			continue
		}

		cw, err := client.GetCompiledWorkflow(context.Background(), tn.id, p.versionID)
		if err != nil {
			t.Errorf("%s: compiled-plan read: %v", name, err)
			continue
		}
		if cw.Status != "PUBLISHED" || !cw.IsValid || cw.WorkflowID != p.workflowID || cw.VersionID != p.versionID {
			t.Errorf("%s: compiled workflow = %+v", name, cw)
			continue
		}
		var collab dsl.CompiledCollaboration
		if err := json.Unmarshal([]byte(cw.CompiledPlanJSON), &collab); err != nil {
			t.Errorf("%s: plan does not decode: %v", name, err)
			continue
		}
		if collab.SchemaVersion != dsl.CurrentSchemaVersion {
			t.Errorf("%s: schema_version = %d, Execution runs %d", name, collab.SchemaVersion, dsl.CurrentSchemaVersion)
		}
		if _, err := dsl.ExpandCalls(&collab, domain.MaxPlanStages); err != nil {
			t.Errorf("%s: plan does not expand: %v", name, err)
			continue
		}

		a = e.startInstance(t, tn, p.versionID)
		if a.status != http.StatusCreated {
			t.Errorf("%s: POST /instances = %d %s", name, a.status, a.body)
			continue
		}
		var started struct {
			ID uuid.UUID `json:"id"`
		}
		a.decode(t, &started)
		d, ok := e.pollInstance(t, tn, started.ID, func(d instanceDetail) bool {
			_, ok := readyTask(d)
			return ok || len(d.CurrentNodeKeys) > 0 || d.Status == "COMPLETED" || d.Status == "FAILED"
		})
		if !ok || d.Status == "FAILED" {
			t.Errorf("%s: instance %s never reached a node; status %q, nodes %v, tasks %+v\n%s", name, started.ID, d.Status, d.CurrentNodeKeys, d.Tasks, e.describe(t, tn, started.ID))
			continue
		}
		t.Logf("%s: runs (status %s, nodes %v)", name, d.Status, d.CurrentNodeKeys)
		ran++
	}
	if ran < minRunnableFixtures {
		t.Fatalf("%d Definition fixtures ran in Execution, want at least %d", ran, minRunnableFixtures)
	}
	t.Logf("%d Definition fixtures published and ran in Execution", ran)
}

func (e *env) completeTask(t *testing.T, tn tenant, assignee uuid.UUID, tk taskSummary) {
	t.Helper()
	e.executionAs(t, tn, assignee, "").must(http.MethodPost, "/api/v1/tasks/"+tk.ID.String()+"/complete", map[string]any{
		"result_json":    json.RawMessage(`{"decision":"approved"}`),
		"record_version": tk.RecordVersion,
	}, http.StatusAccepted)
}

func (e *env) roundTrip(t *testing.T) {
	tn := newTenant()
	assignee := newUser()
	p := e.mustPublish(t, tn, twoStage(t, assignee, uuid.New()))
	id := e.mustStart(t, tn, p.versionID)

	for _, node := range []string{"Prepare", "Approve"} {
		d := e.waitInstance(t, tn, id, node+" READY", func(d instanceDetail) bool {
			tk, ok := readyTask(d)
			return ok && strings.Contains(tk.NodeKey, node)
		})
		tk, _ := readyTask(d)
		e.completeTask(t, tn, assignee, tk)
	}
	e.waitInstance(t, tn, id, "COMPLETED", func(d instanceDetail) bool { return d.Status == "COMPLETED" })
}

func (e *env) archive(t *testing.T, tn tenant, base string, workflowID uuid.UUID) answer {
	t.Helper()
	c := e.definitionAs(t, tn)
	c.baseURL = base
	return c.do(http.MethodPost, "/api/v1/workflows/"+workflowID.String()+"/archive", nil)
}

func (e *env) archiveGuard(t *testing.T) {
	tn := newTenant()
	assignee := newUser()
	p := e.mustPublish(t, tn, twoStage(t, assignee, uuid.New()))
	id := e.mustStart(t, tn, p.versionID)
	e.waitInstance(t, tn, id, "a READY task", func(d instanceDetail) bool { _, ok := readyTask(d); return ok })

	if a := e.archive(t, tn, e.definitionURL, p.workflowID); a.status != http.StatusConflict || a.code(t) != "ACTIVE_INSTANCES_EXIST" {
		t.Fatalf("archive with a live instance = %d %s, want 409 ACTIVE_INSTANCES_EXIST", a.status, a.body)
	}

	d := e.instance(t, tn, id)
	e.executionAs(t, tn, tn.admin, "tenant_admin").must(http.MethodPost, "/api/v1/instances/"+id.String()+"/cancel",
		map[string]any{"reason": "compat", "record_version": d.RecordVersion}, http.StatusAccepted)
	e.waitInstance(t, tn, id, "TERMINATED", func(d instanceDetail) bool { return d.Status == "TERMINATED" })

	if a := e.archive(t, tn, e.definitionURL, p.workflowID); a.status != http.StatusOK {
		t.Fatalf("archive after cancel = %d %s, want 200", a.status, a.body)
	}

	if a := e.startInstance(t, tn, p.versionID); a.status != http.StatusConflict || a.code(t) != "VERSION_NOT_PUBLISHED" {
		t.Fatalf("start on a version archived after an earlier start = %d %s, want 409 VERSION_NOT_PUBLISHED", a.status, a.body)
	}
}

func (e *env) revoke(t *testing.T, tn tenant, eventType string, user, department uuid.UUID) {
	t.Helper()
	body := map[string]string{"user_id": user.String(), "role": "member"}
	if department != uuid.Nil {
		body["department_id"] = department.String()
	}
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	env := events.Envelope[json.RawMessage]{
		ID: uuid.NewString(), Type: eventType, Source: "iam-org-membership",
		TenantID: tn.id.String(), Timestamp: time.Now().UTC(), Payload: payload,
	}
	caller{t: t, baseURL: e.definitionURL, token: definitionToken}.must(http.MethodPost, "/api/v1/internal/events", env, http.StatusOK)
}

func (e *env) version(t *testing.T, tn tenant, p published) (status string, valid bool) {
	t.Helper()
	var v struct {
		Status  string `json:"status"`
		IsValid bool   `json:"is_valid"`
	}
	e.definitionAs(t, tn).must(http.MethodGet, "/api/v1/workflows/"+p.workflowID.String()+"/versions/"+p.versionID.String(), nil, http.StatusOK).decode(t, &v)
	return v.Status, v.IsValid
}

func (e *env) revocation(t *testing.T) {
	tn := newTenant()
	assignee, department := newUser(), uuid.New()
	p := e.mustPublish(t, tn, twoStage(t, assignee, department))
	id := e.mustStart(t, tn, p.versionID)
	e.waitInstance(t, tn, id, "a READY task", func(d instanceDetail) bool { _, ok := readyTask(d); return ok })

	e.revoke(t, tn, "DepartmentMembershipRevoked", assignee, department)

	if _, valid := e.version(t, tn, p); valid {
		t.Fatal("version still valid after its default assignee's membership was revoked")
	}
	e.waitInstance(t, tn, id, "PAUSED", func(d instanceDetail) bool { return d.Status == "PAUSED" })

	t.Run("a new start on the invalidated version is refused", func(t *testing.T) {
		a := e.startInstance(t, tn, p.versionID)
		if a.status != http.StatusConflict || a.code(t) != "VERSION_INVALID" {
			t.Fatalf("start on a version Definition invalidated = %d %s, want 409 VERSION_INVALID", a.status, a.body)
		}
	})
}

func (e *env) tokens(t *testing.T) {
	tn := newTenant()
	p := e.mustPublish(t, tn, twoStage(t, newUser(), uuid.New()))

	t.Run("Execution's client with Definition's token reads the plan", func(t *testing.T) {
		c := outboundhttp.NewDefinitionClient(e.definitionURL, definitionToken, 5*time.Second)
		if _, err := c.GetCompiledWorkflow(context.Background(), tn.id, p.versionID); err != nil {
			t.Fatalf("read with the configured token: %v", err)
		}
	})
	t.Run("Execution's client with another token is rejected, not retried as an outage", func(t *testing.T) {
		c := outboundhttp.NewDefinitionClient(e.definitionURL, "not-the-token", 5*time.Second)
		_, err := c.GetCompiledWorkflow(context.Background(), tn.id, p.versionID)
		if !errors.Is(err, port.ErrDefinitionRejected) {
			t.Fatalf("read with a wrong token: %v, want ErrDefinitionRejected", err)
		}
	})
	t.Run("Definition with another Execution token cannot pass the archive guard", func(t *testing.T) {
		wrong := e.startDefinition(t, freePort(t), "not-the-token")
		if a := e.archive(t, tn, wrong, p.workflowID); a.status != http.StatusServiceUnavailable {
			t.Fatalf("archive through a Definition holding a wrong Execution token = %d %s, want 503", a.status, a.body)
		}
	})
}

func (e *env) unstartable(t *testing.T) {
	tn := newTenant()

	t.Run("a draft version is VERSION_NOT_PUBLISHED", func(t *testing.T) {
		p, a := e.createDraft(t, tn, twoStage(t, newUser(), uuid.New()))
		if a.status != http.StatusCreated {
			t.Fatalf("create = %d %s", a.status, a.body)
		}
		a = e.startInstance(t, tn, p.versionID)
		if a.status != http.StatusConflict || a.code(t) != "VERSION_NOT_PUBLISHED" {
			t.Fatalf("start on a draft = %d %s, want 409 VERSION_NOT_PUBLISHED", a.status, a.body)
		}
	})
	t.Run("an archived version is VERSION_NOT_PUBLISHED", func(t *testing.T) {
		p := e.mustPublish(t, tn, twoStage(t, newUser(), uuid.New()))
		if a := e.archive(t, tn, e.definitionURL, p.workflowID); a.status != http.StatusOK {
			t.Fatalf("archive = %d %s", a.status, a.body)
		}
		a := e.startInstance(t, tn, p.versionID)
		if a.status != http.StatusConflict || a.code(t) != "VERSION_NOT_PUBLISHED" {
			t.Fatalf("start on an archived version = %d %s, want 409 VERSION_NOT_PUBLISHED", a.status, a.body)
		}
	})
	t.Run("another tenant's version is not readable", func(t *testing.T) {
		other := newTenant()
		p := e.mustPublish(t, other, twoStage(t, newUser(), uuid.New()))
		a := e.startInstance(t, tn, p.versionID)
		if a.status == http.StatusCreated {
			t.Fatalf("tenant %s started tenant %s's version", tn.id, other.id)
		}
		if a.status != http.StatusServiceUnavailable || a.code(t) != "UPSTREAM_UNAVAILABLE" {
			t.Fatalf("start on another tenant's version = %d %s, want 503 UPSTREAM_UNAVAILABLE (LLD §10.2)", a.status, a.body)
		}
	})
}

func (e *env) aliases(t *testing.T) {
	internal := caller{t: t, baseURL: e.definitionURL, token: definitionToken}
	internal.must(http.MethodPost, "/api/v1/internal/connector-aliases/rest", map[string]any{
		"alias": "compat-partner", "method": "POST", "baseURL": "https://" + aliasHost,
		"pathTemplate": "/orders/{id}", "timeoutMs": 2500,
	}, http.StatusNoContent)

	a := internal.must(http.MethodGet, "/api/v1/internal/connector-aliases", nil, http.StatusOK)
	var cfg aliasconfig.Config
	if err := json.Unmarshal(a.body, &cfg); err != nil {
		t.Fatalf("the connector worker's decode of %s: %v", a.body, err)
	}
	for _, rc := range cfg.RestCall {
		if rc.Alias != "compat-partner" {
			continue
		}
		if rc.Method != "POST" || rc.BaseURL != "https://"+aliasHost || rc.PathTemplate != "/orders/{id}" || rc.Timeout != 2500*time.Millisecond {
			t.Fatalf("alias as the connector worker reads it = %+v", rc)
		}
		return
	}
	t.Fatalf("alias missing from %s", a.body)
}

func fill(t *testing.T, file string, values map[string]string) string {
	t.Helper()
	b := readFixture(t, filepath.Join("testdata", file))
	for k, v := range values {
		b = strings.ReplaceAll(b, "{{"+k+"}}", v)
	}
	return b
}

func (e *env) libraryCall(t *testing.T) {
	tn := newTenant()
	assignee := newUser()
	process := "Process_CompatReview_" + strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	values := map[string]string{"PROCESS": process, "ASSIGNEE": assignee.String(), "DEPARTMENT": uuid.NewString()}

	def := e.definitionAs(t, tn)
	var mod struct {
		ModuleID  uuid.UUID `json:"module_id"`
		VersionID uuid.UUID `json:"version_id"`
	}
	def.must(http.MethodPost, "/api/v1/modules", map[string]any{
		"name": "compat review", "bpmn_xml": fill(t, "review-module.bpmn", values),
	}, http.StatusCreated).decode(t, &mod)
	def.must(http.MethodPost, "/api/v1/modules/"+mod.ModuleID.String()+"/versions/"+mod.VersionID.String()+"/publish", nil, http.StatusOK)

	p := e.mustPublish(t, tn, fill(t, "calls-review.bpmn", values))
	id := e.mustStart(t, tn, p.versionID)

	for _, node := range []string{"Review_Task", "Approve"} {
		d := e.waitInstance(t, tn, id, node+" READY", func(d instanceDetail) bool {
			tk, ok := readyTask(d)
			return ok && strings.HasSuffix(tk.NodeKey, "/"+node)
		})
		tk, _ := readyTask(d)
		e.completeTask(t, tn, assignee, tk)
	}
	e.waitInstance(t, tn, id, "COMPLETED", func(d instanceDetail) bool { return d.Status == "COMPLETED" })
}

func (e *env) tenantRevocation(t *testing.T) {
	tn := newTenant()
	assignee := newUser()
	p := e.mustPublish(t, tn, twoStage(t, assignee, uuid.New()))
	id := e.mustStart(t, tn, p.versionID)
	e.waitInstance(t, tn, id, "a READY task", func(d instanceDetail) bool { _, ok := readyTask(d); return ok })

	e.revoke(t, tn, "MembershipRevoked", assignee, uuid.Nil)

	if _, valid := e.version(t, tn, p); valid {
		t.Fatal("version still valid after its default assignee left the tenant")
	}
	e.waitInstance(t, tn, id, "PAUSED", func(d instanceDetail) bool { return d.Status == "PAUSED" })
}
