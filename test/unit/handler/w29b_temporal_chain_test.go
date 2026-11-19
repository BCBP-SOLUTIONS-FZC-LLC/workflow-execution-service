package handler_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporalclient"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

// w29bSDK is the Temporal SDK client under the real temporalclient adapter. It records the order of
// calls in events so a test can tell what happened before what.
type w29bSDK struct {
	client.Client

	events      *[]string
	queryErr    error
	queryValue  converter.EncodedValue
	updateErr   error
	handleErr   error
	onHandleGet func()
	queries     []string
	signals     []string
	updates     []client.UpdateWorkflowOptions
}

type w29bHandle struct {
	client.WorkflowUpdateHandle
	sdk *w29bSDK
}

func (h *w29bHandle) Get(context.Context, any) error {
	*h.sdk.events = append(*h.sdk.events, "update-wait")
	if h.sdk.onHandleGet != nil {
		h.sdk.onHandleGet()
	}
	return h.sdk.handleErr
}

func (s *w29bSDK) QueryWorkflow(_ context.Context, workflowID, _, queryType string, _ ...any) (converter.EncodedValue, error) {
	*s.events = append(*s.events, "query")
	s.queries = append(s.queries, workflowID+" "+queryType)
	return s.queryValue, s.queryErr
}

func (s *w29bSDK) SignalWorkflow(_ context.Context, workflowID, _, name string, _ any) error {
	*s.events = append(*s.events, "signal")
	s.signals = append(s.signals, workflowID+" "+name)
	return nil
}

func (s *w29bSDK) UpdateWorkflow(_ context.Context, opts client.UpdateWorkflowOptions) (client.WorkflowUpdateHandle, error) {
	*s.events = append(*s.events, "update-call")
	s.updates = append(s.updates, opts)
	if s.updateErr != nil {
		return nil, s.updateErr
	}
	return &w29bHandle{sdk: s}, nil
}

// w29bEncoded decodes the JSON text it holds, as the SDK's default converter does.
type w29bEncoded struct {
	converter.EncodedValue
	raw string
}

func (e w29bEncoded) Get(out any) error {
	if err := json.Unmarshal([]byte(e.raw), out); err != nil {
		return fmt.Errorf("decode %s: %w", e.raw, err)
	}
	return nil
}

func w29bNewSDK() *w29bSDK { return &w29bSDK{events: &[]string{}} }

const w29bInternalText = "w29b frontend 10.1.2.3:7233 password=hunter2"

func w29bOutsidePlan(t *testing.T) string {
	t.Helper()
	return w18aPlanJSON(t,
		dsl.StageDef{Type: "receive_task", NodeID: "Receive_Bid", Extras: map[string]string{"message": "bid-received"}},
		dsl.StageDef{Type: "prep", NodeID: "Prepare", BoundaryMessage: &dsl.MessagePath{
			MessageName: "withdraw", BoundaryID: "Boundary_Withdraw", Interrupting: true, Terminates: true,
		}},
	)
}

// w29bQueryFailures are the answers a failed waiting-keys query gets once the real adapter and the real
// service sit between the SDK and the route.
var w29bQueryFailures = []struct {
	name       string
	queryErr   error
	queryValue converter.EncodedValue
	wantStatus int
	wantCode   string
	wantDetail string
}{
	{
		name: "the execution is gone", queryErr: serviceerror.NewNotFound(w29bInternalText),
		wantStatus: http.StatusConflict, wantCode: "INVALID_INSTANCE_STATE", wantDetail: "workflow execution not found",
	},
	{
		name: "the cluster is unavailable", queryErr: serviceerror.NewUnavailable(w29bInternalText),
		wantStatus: http.StatusServiceUnavailable, wantCode: "UPSTREAM_UNAVAILABLE", wantDetail: "the workflow engine could not take the request; retry it",
	},
	{
		name: "the query deadline passes", queryErr: fmt.Errorf("%s: %w", w29bInternalText, context.DeadlineExceeded),
		wantStatus: http.StatusServiceUnavailable, wantCode: "UPSTREAM_UNAVAILABLE", wantDetail: "request timed out",
	},
	{
		name: "the workflow refuses the query", queryErr: serviceerror.NewQueryFailed(w29bInternalText),
		wantStatus: http.StatusInternalServerError, wantCode: "INTERNAL_ERROR", wantDetail: "an unexpected error occurred",
	},
	{
		name: "the answer is not a list of keys", queryValue: w29bEncoded{raw: `{"waiting":"` + w29bInternalText + `"}`},
		wantStatus: http.StatusInternalServerError, wantCode: "INTERNAL_ERROR", wantDetail: "an unexpected error occurred",
	},
}

func TestOutsideWaitRoutes_WaitingKeysQueryFails_AnswersByTheKindOfFailureAndSignalsNothing(t *testing.T) {
	routes := []struct {
		name string
		path string
	}{
		{name: "complete a receive task", path: completePath("finance%2FReceive_Bid")},
		{name: "fire a boundary", path: firePath("finance%2FPrepare", "Boundary_Withdraw")},
	}
	for _, route := range routes {
		for _, tc := range w29bQueryFailures {
			t.Run(route.name+" when "+tc.name, func(t *testing.T) {
				world := w18aNewWorld(t, w29bOutsidePlan(t))
				world.seedInstance(domain.InstanceStatusRunning)
				sdk := w29bNewSDK()
				sdk.queryErr, sdk.queryValue = tc.queryErr, tc.queryValue
				world.svc.Temporal = temporalclient.New(sdk)

				rec := do(world.router, asAdmin(req(http.MethodPost, route.path, nil)))

				require.Equal(t, tc.wantStatus, rec.Code, rec.Body.String())
				var body problemBody
				decodeJSON(t, rec.Body, &body)
				assert.Equal(t, tc.wantCode, body.Code)
				assert.Equal(t, tc.wantDetail, body.Detail)
				assert.NotContains(t, rec.Body.String(), "10.1.2.3", "the cluster address is not echoed")
				assert.NotContains(t, rec.Body.String(), "hunter2")
				assert.Equal(t, []string{"tenant:subject:w18a " + port.QueryWaitingExternal}, sdk.queries, "the workflow was asked once")
				assert.Empty(t, sdk.signals, "nothing is signalled after a query failure")
			})
		}
	}
}

// w29bDeferTasks serves the one ready task, tells the service a step has completed (a defer needs one), and
// logs each task read in the same event list as the SDK calls.
type w29bDeferTasks struct {
	*w10aTasks
	events *[]string
}

func (r *w29bDeferTasks) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Task, error) {
	*r.events = append(*r.events, "read-task")
	return r.w10aTasks.GetByID(ctx, tenantID, id)
}

func (r *w29bDeferTasks) ListByInstance(_ context.Context, tenantID, instanceID uuid.UUID, _ port.PageRequest) ([]*domain.Task, *port.Cursor, error) {
	return []*domain.Task{{
		ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: instanceID, NodeKey: "finance/intake", Status: domain.TaskStatusCompleted,
	}}, nil, nil
}

// w29bDeferAssignments adds the assignee count the answer carries.
type w29bDeferAssignments struct{ *w10aAssignments }

func (r w29bDeferAssignments) CountActiveByTasks(_ context.Context, tenantID uuid.UUID, taskIDs []uuid.UUID) (map[uuid.UUID]int, error) {
	counts := map[uuid.UUID]int{}
	for _, a := range r.rows {
		if a.TenantID == tenantID && a.IsActive && slices.Contains(taskIDs, a.TaskID) {
			counts[a.TaskID]++
		}
	}
	return counts, nil
}

type w29bDeferWorld struct {
	*w10aWorld
	sdk *w29bSDK
}

func w29bNewDeferWorld() (*w29bDeferWorld, *gin.Engine) {
	w := &w29bDeferWorld{w10aWorld: newW10aWorld(), sdk: w29bNewSDK()}
	svc := &service.TaskService{
		Tasks:       &w29bDeferTasks{w10aTasks: w.tasks, events: w.sdk.events},
		Assignments: w29bDeferAssignments{w.assignments}, Instances: w.instances, Temporal: temporalclient.New(w.sdk),
		Cache: w.cache, Overrides: w.overrides, Transactor: w.tx, Signals: w.outbox,
	}
	return w, newRouter(handler.New(handler.Services{Tasks: svc}))
}

func w29bDeferRequest() *http.Request {
	return req(http.MethodPost, hreTaskPath(testTaskID.String(), "defer"), map[string]any{"reason": "waiting for documents", "record_version": w10aTaskVersion})
}

func TestDeferTask_WorkflowUpdateCompletes_RereadsTheTaskAfterTheUpdateFinished(t *testing.T) {
	world, router := w29bNewDeferWorld()
	world.sdk.onHandleGet = func() {
		world.task.Status = domain.TaskStatusDeferred
		world.task.RecordVersion += 2
	}

	rec := do(router, w29bDeferRequest())

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var body struct {
		ID            string `json:"id"`
		Status        string `json:"status"`
		RecordVersion int64  `json:"record_version"`
	}
	decodeJSON(t, rec.Body, &body)
	assert.Equal(t, testTaskID.String(), body.ID)
	assert.Equal(t, "DEFERRED", body.Status, "the answer is the task as the finished update left it")
	assert.EqualValues(t, w10aTaskVersion+2, body.RecordVersion)
	assert.Equal(t, []string{"read-task", "update-call", "update-wait", "read-task"}, *world.sdk.events,
		"the task is read once before the update and once after the update completed")
	require.Len(t, world.sdk.updates, 1)
	assert.Equal(t, "tenant:subject:key", world.sdk.updates[0].WorkflowID)
	assert.Equal(t, "stage-defer:"+testInstID.String(), world.sdk.updates[0].UpdateName)
	assert.Equal(t, client.WorkflowUpdateStageCompleted, world.sdk.updates[0].WaitForStage)
}

func TestDeferTask_WorkflowExecutionIsMissingOrClosed_Returns409InvalidInstanceState(t *testing.T) {
	cases := []struct {
		name       string
		setup      func(sdk *w29bSDK)
		wantEvents []string
	}{
		{
			name:       "found missing by the update call",
			setup:      func(sdk *w29bSDK) { sdk.updateErr = serviceerror.NewNotFound(w29bInternalText) },
			wantEvents: []string{"read-task", "update-call"},
		},
		{
			name:       "found missing while waiting for the result",
			setup:      func(sdk *w29bSDK) { sdk.handleErr = serviceerror.NewNotFound(w29bInternalText) },
			wantEvents: []string{"read-task", "update-call", "update-wait"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			world, router := w29bNewDeferWorld()
			tc.setup(world.sdk)

			rec := do(router, w29bDeferRequest())

			require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
			var body problemBody
			decodeJSON(t, rec.Body, &body)
			assert.Equal(t, "INVALID_INSTANCE_STATE", body.Code)
			assert.Equal(t, "workflow execution not found", body.Detail)
			assert.NotContains(t, rec.Body.String(), "10.1.2.3")
			assert.Equal(t, tc.wantEvents, *world.sdk.events, "the task is not re-read after a failed update")
			assert.Equal(t, domain.TaskStatusReady, world.task.Status)
		})
	}
}

func TestDeferTask_UpdateAcceptedButTheHandlerFails_Returns500AndHidesTheCause(t *testing.T) {
	world, router := w29bNewDeferWorld()
	world.sdk.handleErr = temporal.NewApplicationError("defer task: "+w29bInternalText, "ActivityFailure", errors.New("w29b db down"))

	rec := do(router, w29bDeferRequest())

	require.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
	var body problemBody
	decodeJSON(t, rec.Body, &body)
	assert.Equal(t, "INTERNAL_ERROR", body.Code)
	assert.Equal(t, "an unexpected error occurred", body.Detail)
	assert.NotContains(t, rec.Body.String(), "10.1.2.3")
	assert.NotContains(t, rec.Body.String(), "db down")
	assert.Equal(t, []string{"read-task", "update-call", "update-wait"}, *world.sdk.events, "the failed update is not followed by a re-read")
	assert.Equal(t, domain.TaskStatusReady, world.task.Status)
}
