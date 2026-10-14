//go:build compat

package compat_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
)

type uuidT = uuid.UUID

// caller is one identity against one service: the same tenant and user
// headers reach both services, as they do behind the gateway.
type caller struct {
	t        *testing.T
	baseURL  string
	tenantID uuid.UUID
	userID   uuid.UUID
	roles    string
	plan     string
	token    string
}

type answer struct {
	status int
	body   []byte
}

func (a answer) decode(t *testing.T, out any) {
	t.Helper()
	if err := json.Unmarshal(a.body, out); err != nil {
		t.Fatalf("decode %s: %v", a.body, err)
	}
}

func (a answer) code(t *testing.T) string {
	t.Helper()
	var p struct {
		Code string `json:"code"`
	}
	_ = json.Unmarshal(a.body, &p)
	return p.Code
}

func (a answer) reasons() []string {
	var p struct {
		InvalidParams []struct {
			Code   string `json:"code"`
			Reason string `json:"reason"`
		} `json:"invalid_params"`
	}
	_ = json.Unmarshal(a.body, &p)
	out := make([]string, 0, len(p.InvalidParams))
	for _, ip := range p.InvalidParams {
		out = append(out, ip.Code)
	}
	return out
}

func (c caller) do(method, path string, body any) answer {
	c.t.Helper()
	var r io.Reader = http.NoBody
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			c.t.Fatalf("marshal: %v", err)
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, c.baseURL+path, r)
	if err != nil {
		c.t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.tenantID != uuid.Nil {
		req.Header.Set("x-tenant-id", c.tenantID.String())
	}
	if c.userID != uuid.Nil {
		req.Header.Set("x-user-id", c.userID.String())
	}
	if c.roles != "" {
		req.Header.Set("x-tenant-roles", c.roles)
	}
	if c.plan != "" {
		req.Header.Set("x-plan", c.plan)
	}
	if c.token != "" {
		req.Header.Set("x-internal-token", c.token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		c.t.Fatalf("read %s %s: %v", method, path, err)
	}
	return answer{status: resp.StatusCode, body: b}
}

func (c caller) must(method, path string, body any, want int) answer {
	c.t.Helper()
	a := c.do(method, path, body)
	if a.status != want {
		c.t.Fatalf("%s %s%s = %d, want %d: %s", method, c.baseURL, path, a.status, want, a.body)
	}
	return a
}

type tenant struct {
	id    uuid.UUID
	admin uuid.UUID
}

func newTenant() tenant { return tenant{id: uuid.New(), admin: uuid.New()} }

func (e *env) definitionAs(t *testing.T, tn tenant) caller {
	return caller{t: t, baseURL: e.definitionURL, tenantID: tn.id, userID: tn.admin, roles: "tenant_admin", plan: "enterprise"}
}

func (e *env) executionAs(t *testing.T, tn tenant, user uuid.UUID, roles string) caller {
	return caller{t: t, baseURL: e.executionURL, tenantID: tn.id, userID: user, roles: roles}
}

type published struct {
	workflowID uuid.UUID
	versionID  uuid.UUID
}

func readFixture(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

// createDraft creates a workflow from the BPMN and answers the draft's
// identifiers; Definition answers 201 with the first draft version.
func (e *env) createDraft(t *testing.T, tn tenant, bpmn string) (published, answer) {
	t.Helper()
	a := e.definitionAs(t, tn).do(http.MethodPost, "/api/v1/workflows", map[string]any{
		"key": "compat-" + uuid.NewString(), "name": "compat", "bpmn_xml": bpmn,
	})
	if a.status != http.StatusCreated {
		return published{}, a
	}
	var created struct {
		WorkflowID uuid.UUID `json:"workflow_id"`
		VersionID  uuid.UUID `json:"version_id"`
	}
	a.decode(t, &created)
	return published{workflowID: created.WorkflowID, versionID: created.VersionID}, a
}

func (e *env) publish(t *testing.T, tn tenant, p published) answer {
	t.Helper()
	return e.definitionAs(t, tn).do(http.MethodPost,
		"/api/v1/workflows/"+p.workflowID.String()+"/versions/"+p.versionID.String()+"/publish", nil)
}

func (e *env) mustPublish(t *testing.T, tn tenant, bpmn string) published {
	t.Helper()
	p, a := e.createDraft(t, tn, bpmn)
	if a.status != http.StatusCreated {
		t.Fatalf("create workflow = %d: %s", a.status, a.body)
	}
	if a := e.publish(t, tn, p); a.status != http.StatusOK {
		t.Fatalf("publish = %d: %s", a.status, a.body)
	}
	return p
}

func (e *env) startInstance(t *testing.T, tn tenant, versionID uuid.UUID) answer {
	t.Helper()
	return e.executionAs(t, tn, tn.admin, "tenant_admin").do(http.MethodPost, "/api/v1/instances", map[string]any{
		"subject_id":          "compat-subject-" + uuid.NewString(),
		"business_key":        "compat-" + uuid.NewString(),
		"workflow_version_id": versionID,
	})
}

func (e *env) mustStart(t *testing.T, tn tenant, versionID uuid.UUID) uuid.UUID {
	t.Helper()
	a := e.startInstance(t, tn, versionID)
	if a.status != http.StatusCreated {
		t.Fatalf("POST /instances = %d: %s", a.status, a.body)
	}
	var started struct {
		ID uuid.UUID `json:"id"`
	}
	a.decode(t, &started)
	return started.ID
}

type taskSummary struct {
	ID            uuid.UUID `json:"id"`
	NodeKey       string    `json:"node_key"`
	Status        string    `json:"status"`
	RecordVersion int64     `json:"record_version"`
}

type instanceDetail struct {
	TemporalWorkflowID string        `json:"temporal_workflow_id"`
	Status             string        `json:"status"`
	CurrentNodeKeys    []string      `json:"current_node_keys"`
	RecordVersion      int64         `json:"record_version"`
	Tasks              []taskSummary `json:"tasks"`
}

func (e *env) instance(t *testing.T, tn tenant, id uuid.UUID) instanceDetail {
	t.Helper()
	var d instanceDetail
	e.executionAs(t, tn, tn.admin, "tenant_admin").must(http.MethodGet, "/api/v1/instances/"+id.String(), nil, http.StatusOK).decode(t, &d)
	return d
}

func (e *env) waitInstance(t *testing.T, tn tenant, id uuid.UUID, what string, ready func(instanceDetail) bool) instanceDetail {
	t.Helper()
	d, ok := e.pollInstance(t, tn, id, ready)
	if !ok {
		t.Fatalf("instance %s never reached %s; last status %q, nodes %v, tasks %+v", id, what, d.Status, d.CurrentNodeKeys, d.Tasks)
	}
	return d
}

func (e *env) pollInstance(t *testing.T, tn tenant, id uuid.UUID, ready func(instanceDetail) bool) (instanceDetail, bool) {
	t.Helper()
	deadline := time.Now().Add(pollTimeout)
	for {
		d := e.instance(t, tn, id)
		if ready(d) {
			return d, true
		}
		if time.Now().After(deadline) {
			return d, false
		}
		time.Sleep(pollInterval)
	}
}

func readyTask(d instanceDetail) (taskSummary, bool) {
	for _, tk := range d.Tasks {
		if tk.Status == "READY" {
			return tk, true
		}
	}
	return taskSummary{}, false
}

// describe reports what Temporal knows about an instance that did not move:
// its pending activities' last failures and any failed workflow or activity
// task in its history.
func (e *env) describe(t *testing.T, tn tenant, id uuid.UUID) string {
	t.Helper()
	d := e.instance(t, tn, id)
	ctx := context.Background()
	var b strings.Builder
	desc, err := e.temporal.DescribeWorkflowExecution(ctx, d.TemporalWorkflowID, "")
	if err != nil {
		return "describe: " + err.Error()
	}
	for _, pa := range desc.GetPendingActivities() {
		fmt.Fprintf(&b, "pending %s attempt %d: %s\n", pa.GetActivityType().GetName(), pa.GetAttempt(), pa.GetLastFailure().GetMessage())
	}
	it := e.temporal.GetWorkflowHistory(ctx, d.TemporalWorkflowID, "", false, enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for it.HasNext() {
		ev, err := it.Next()
		if err != nil {
			fmt.Fprintf(&b, "history: %v\n", err)
			break
		}
		if a := ev.GetWorkflowTaskFailedEventAttributes(); a != nil {
			fmt.Fprintf(&b, "workflow task failed: %s\n", a.GetFailure().GetMessage())
		}
		if a := ev.GetActivityTaskFailedEventAttributes(); a != nil {
			fmt.Fprintf(&b, "activity failed: %s\n", a.GetFailure().GetMessage())
		}
	}
	return b.String()
}
