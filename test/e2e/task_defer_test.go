//go:build e2e

package e2e_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/google/uuid"
)

// TestE2E_TaskDefer drives POST /tasks/:id/defer against a real Temporal
// server and Postgres. A defer sends the instance one completed step back:
// the deferred task is closed DEFERRED, the earlier step runs again, and the
// plan then carries on through the deferred step to the end. At the first
// step there is nothing to go back to, and the defer is refused.
func TestE2E_TaskDefer(t *testing.T) {
	f := newAdminOverrideFixture(t)

	resp := f.assignee.do(http.MethodPost, "/api/v1/tasks/"+f.firstTask.ID.String()+"/defer", map[string]any{
		"reason": "need more info", "record_version": f.firstTask.RecordVersion,
	}, nil)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("defer at the first step: status = %d, want 409", resp.StatusCode)
	}

	resp = f.assignee.do(http.MethodPost, "/api/v1/tasks/"+f.firstTask.ID.String()+"/complete", map[string]any{
		"result_json": json.RawMessage(`{"decision":"approved"}`), "record_version": f.firstTask.RecordVersion,
	}, nil)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("complete first task: status = %d, want 202", resp.StatusCode)
	}
	detail := pollInstance(t, f.admin, f.instanceID, func(d instanceDetail) bool { return len(d.Tasks) == 2 })
	var second taskSummary
	for _, task := range detail.Tasks {
		if task.ID != f.firstTask.ID {
			second = task
		}
	}

	resp = f.assignee.do(http.MethodPost, "/api/v1/tasks/"+second.ID.String()+"/defer", map[string]any{
		"reason": "need more info", "record_version": second.RecordVersion,
	}, nil)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("defer at the second step: status = %d, want 202", resp.StatusCode)
	}

	detail = pollInstance(t, f.admin, f.instanceID, func(d instanceDetail) bool { return len(d.Tasks) == 3 })
	var rerun *taskSummary
	for i := range detail.Tasks {
		task := &detail.Tasks[i]
		switch task.ID {
		case f.firstTask.ID:
		case second.ID:
			if task.Status != "DEFERRED" {
				t.Errorf("deferred task status = %q, want DEFERRED", task.Status)
			}
		default:
			rerun = task
		}
	}
	if rerun == nil || rerun.NodeKey != f.firstTask.NodeKey || rerun.Status != "READY" {
		t.Fatalf("want the first step's node run again and READY, got %+v", detail.Tasks)
	}

	resp = f.assignee.do(http.MethodPost, "/api/v1/tasks/"+rerun.ID.String()+"/complete", map[string]any{
		"result_json": json.RawMessage(`{}`), "record_version": rerun.RecordVersion,
	}, nil)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("complete the re-run task: status = %d, want 202", resp.StatusCode)
	}
	detail = pollInstance(t, f.admin, f.instanceID, func(d instanceDetail) bool { return len(d.Tasks) == 4 })
	seen := map[uuid.UUID]bool{f.firstTask.ID: true, second.ID: true, rerun.ID: true}
	for _, task := range detail.Tasks {
		if seen[task.ID] {
			continue
		}
		resp = f.assignee.do(http.MethodPost, "/api/v1/tasks/"+task.ID.String()+"/complete", map[string]any{
			"result_json": json.RawMessage(`{}`), "record_version": task.RecordVersion,
		}, nil)
		if resp.StatusCode != http.StatusAccepted {
			t.Fatalf("complete the second step again: status = %d, want 202", resp.StatusCode)
		}
	}
	pollInstance(t, f.admin, f.instanceID, func(d instanceDetail) bool { return d.Status == "COMPLETED" })
}
