package handler_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var (
	errW11bImpactRead = errors.New("w11b: reading the delegate's assignments failed: password=hunter2")
	errW11bPauseList  = errors.New("w11b: listing the user's assignments failed: password=hunter2")
	errW11bReverse    = errors.New("w11b: reversal read failed: password=hunter2")
)

type w11bProblem struct {
	Type     string `json:"type"`
	Title    string `json:"title"`
	Status   int    `json:"status"`
	Detail   string `json:"detail"`
	Instance string `json:"instance"`
	Code     string `json:"code"`
}

func w11bDecodeProblem(t *testing.T, w *httptest.ResponseRecorder, status int) w11bProblem {
	t.Helper()
	require.Equal(t, status, w.Code, w.Body.String())
	assert.Equal(t, "application/problem+json", w.Header().Get("Content-Type"))
	var p w11bProblem
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &p), w.Body.String())
	assert.Equal(t, status, p.Status)
	assert.NotContains(t, w.Body.String(), "hunter2", "an internal error text never reaches the caller")
	return p
}

func TestDelegateImpact_ReadFailure_Returns500WithTheGenericInternalProblem(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
	}{
		{
			name: "a task of an active assignment is missing",
			err:  fmt.Errorf("delegate impact: read task of assignment %s: %w", uuid.New(), domain.ErrNotFound),
		},
		{name: "reading the delegate's assignments fails", err: errW11bImpactRead},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var got []port.DelegateImpactInput
			fake := &fakeWorkflowClient{delegateImpact: func(_ context.Context, in port.DelegateImpactInput) (port.DelegateImpactResult, error) {
				got = append(got, in)
				return port.DelegateImpactResult{}, tc.err
			}}
			router := hhaInternalRouter(handler.New(handler.Services{WorkflowClient: fake}))
			target := "/api/v1/internal/workflows/delegate-impact?tenant_id=" + testTenantID.String() + "&delegate_user_id=" + testOldDelegateID.String()

			w := hhaServe(router, http.MethodGet, target, "", "")

			p := w11bDecodeProblem(t, w, http.StatusInternalServerError)
			assert.Equal(t, "INTERNAL_ERROR", p.Code)
			assert.Equal(t, "https://errors.bcbp.io/execution/internal-error", p.Type)
			assert.Equal(t, "Internal Server Error", p.Title)
			assert.Equal(t, "an unexpected error occurred", p.Detail)
			assert.NotContains(t, w.Body.String(), tc.err.Error(), "the error text is not echoed")
			assert.Equal(t, "/api/v1/internal/workflows/delegate-impact", p.Instance)
			require.Len(t, got, 1)
			assert.Equal(t, testTenantID, got[0].TenantID)
			assert.Equal(t, testOldDelegateID, got[0].DelegateUserID)
		})
	}
}

func TestDelegateImpact_LastPage_HasNoNextCursorKey(t *testing.T) {
	t.Parallel()

	ids := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	var inputs []port.DelegateImpactInput
	fake := &fakeWorkflowClient{delegateImpact: func(_ context.Context, in port.DelegateImpactInput) (port.DelegateImpactResult, error) {
		inputs = append(inputs, in)
		return port.DelegateImpactResult{
			ActiveWorkflows: 3, ActiveAssignments: 4,
			WorkflowIDs: port.PageResult[uuid.UUID]{Items: ids},
		}, nil
	}}
	router := hhaInternalRouter(handler.New(handler.Services{WorkflowClient: fake}))
	target := "/api/v1/internal/workflows/delegate-impact?tenant_id=" + testTenantID.String() + "&delegate_user_id=" + testOldDelegateID.String() + "&limit=3"

	w := hhaServe(router, http.MethodGet, target, "", "")

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Len(t, inputs, 1)
	assert.Equal(t, testTenantID, inputs[0].TenantID)
	assert.Equal(t, testOldDelegateID, inputs[0].DelegateUserID)
	assert.Equal(t, 3, inputs[0].Page.Limit)
	assert.Nil(t, inputs[0].Page.Cursor)
	assert.Nil(t, inputs[0].DelegationID)
	var body map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.NotContains(t, body, "next_cursor", "the last page carries no cursor")
	assert.Equal(t, float64(3), body["active_workflows"])
	assert.Equal(t, float64(4), body["active_assignments"])
	got, ok := body["workflow_ids"].([]any)
	require.True(t, ok)
	require.Len(t, got, 3)
	for i, id := range ids {
		assert.Equal(t, id.String(), got[i])
	}
}

type w11bPauseCall struct {
	tenantID     uuid.UUID
	userID       uuid.UUID
	departmentID *uuid.UUID
}

type w11bPauser struct {
	err   error
	calls []w11bPauseCall
}

func (p *w11bPauser) PauseUserTasks(_ context.Context, tenantID, userID uuid.UUID, departmentID *uuid.UUID) error {
	p.calls = append(p.calls, w11bPauseCall{tenantID: tenantID, userID: userID, departmentID: departmentID})
	return p.err
}

var _ port.UserTaskPauser = (*w11bPauser)(nil)

func TestPauseUserTasks_PauserOutcome_MapsToTheProblemBody(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
		wantDetail string
		wantType   string
		wantTitle  string
	}{
		{
			name:       "some instances could not be paused",
			err:        fmt.Errorf("%w: %w", port.ErrUserPauseIncomplete, errors.New("signal failed")),
			wantStatus: http.StatusServiceUnavailable,
			wantCode:   "UPSTREAM_UNAVAILABLE",
			wantDetail: "some of the user's instances could not be paused; retry the call",
			wantType:   "https://errors.bcbp.io/execution/upstream-unavailable",
			wantTitle:  "Service Unavailable",
		},
		{
			name:       "listing the user's assignments fails",
			err:        errW11bPauseList,
			wantStatus: http.StatusInternalServerError,
			wantCode:   "INTERNAL_ERROR",
			wantDetail: "an unexpected error occurred",
			wantType:   "https://errors.bcbp.io/execution/internal-error",
			wantTitle:  "Internal Server Error",
		},
		{
			name:       "listing the user's assignments runs out of time",
			err:        fmt.Errorf("list: %w", context.DeadlineExceeded),
			wantStatus: http.StatusServiceUnavailable,
			wantCode:   "UPSTREAM_UNAVAILABLE",
			wantDetail: "request timed out",
			wantType:   "https://errors.bcbp.io/execution/upstream-unavailable",
			wantTitle:  "Service Unavailable",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			pauser := &w11bPauser{err: tc.err}
			router := hhaInternalRouter(handler.New(handler.Services{UserTaskPauser: pauser}))
			userID := uuid.New()

			w := hhaPostJSON(router, "/api/v1/internal/users/"+userID.String()+"/pause-tasks", `{"tenant_id":"`+testTenantID.String()+`"}`)

			p := w11bDecodeProblem(t, w, tc.wantStatus)
			assert.Equal(t, tc.wantCode, p.Code)
			assert.Equal(t, tc.wantDetail, p.Detail)
			assert.Equal(t, tc.wantType, p.Type)
			assert.Equal(t, tc.wantTitle, p.Title)
			assert.NotContains(t, w.Body.String(), tc.err.Error(), "the error text is not echoed")
			assert.Equal(t, []w11bPauseCall{{tenantID: testTenantID, userID: userID}}, pauser.calls)
		})
	}
}

func TestPauseUserTasks_PauserSucceeds_Returns204WithNoBodyForTheBodyTenant(t *testing.T) {
	t.Parallel()

	pauser := &w11bPauser{}
	router := hhaInternalRouter(handler.New(handler.Services{UserTaskPauser: pauser}))
	userID, tenantNotOwningTheTasks := uuid.New(), uuid.New()

	for range 2 {
		w := hhaPostJSON(router, "/api/v1/internal/users/"+userID.String()+"/pause-tasks", `{"tenant_id":"`+tenantNotOwningTheTasks.String()+`"}`)

		require.Equal(t, http.StatusNoContent, w.Code, w.Body.String())
		assert.Empty(t, w.Body.String())
	}
	assert.Equal(t, []w11bPauseCall{
		{tenantID: tenantNotOwningTheTasks, userID: userID},
		{tenantID: tenantNotOwningTheTasks, userID: userID},
	}, pauser.calls, "every repeat reaches the pauser with the tenant of the body")
}

func TestHandleInternalEvent_DelegationEnded_ReconcilerFailure_Returns500AndIsRedelivered(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
	}{
		{name: "a plain read failure", err: errW11bReverse},
		{name: "an eligibility read inside the reversal", err: fmt.Errorf("delegation reversal: eligibility read: %w", errW11bReverse)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fakes := newEventsFakes()
			var calls []port.DelegationReversalInput
			failing := true
			fakes.delegation.reverse = func(_ context.Context, in port.DelegationReversalInput) error {
				calls = append(calls, in)
				if failing {
					return tc.err
				}
				return nil
			}
			router := newInternalRouter(newEventsHandler(fakes))
			eventID := uuid.New()
			body := envelope("DelegationEnded", eventID, testTenantID, time.Now(), map[string]any{
				"delegation_id": testDelegationID2,
				"delegator_id":  testDelegatorID,
				"delegate_id":   testDelegateID2,
				"ended_reason":  "expired",
			})

			w := postEvent(router, body)

			p := w11bDecodeProblem(t, w, http.StatusInternalServerError)
			assert.Equal(t, "INTERNAL_ERROR", p.Code)
			assert.Equal(t, "failed to process event", p.Detail)
			assert.Equal(t, "https://errors.bcbp.io/execution/internal-error", p.Type)
			assert.Equal(t, "Internal Server Error", p.Title)
			assert.NotContains(t, w.Body.String(), tc.err.Error(), "the error text is not echoed")
			processed, err := fakes.processedEvents.IsProcessed(context.Background(), eventID, "membership-execution")
			require.NoError(t, err)
			assert.False(t, processed, "no processed-event row is written, so the consumer redelivers")

			failing = false
			w = postEvent(router, body)

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			processed, err = fakes.processedEvents.IsProcessed(context.Background(), eventID, "membership-execution")
			require.NoError(t, err)
			assert.True(t, processed, "the redelivery is recorded")
			require.Len(t, calls, 2, "the redelivered event reaches the reconciler again")
			for _, in := range calls {
				assert.Equal(t, testTenantID, in.TenantID)
				assert.Equal(t, testDelegationID2, in.DelegationID)
				assert.Equal(t, testDelegatorID, in.DelegatorID)
				assert.Equal(t, testDelegateID2, in.DelegateID)
			}
		})
	}
}
