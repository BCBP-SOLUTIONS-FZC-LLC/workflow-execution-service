package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var errW16aAssignDown = errors.New("w16a: assignment insert failed")

// w16aFlakyAssignments fails the first failuresLeft inserts and records every attempt.
type w16aFlakyAssignments struct {
	*w11aAssignments
	failuresLeft int
}

func (r *w16aFlakyAssignments) Create(ctx context.Context, a *domain.TaskAssignment) error {
	if r.failuresLeft > 0 {
		r.failuresLeft--
		r.mu.Lock()
		r.attempts = append(r.attempts, *a)
		r.mu.Unlock()
		return errW16aAssignDown
	}
	return r.w11aAssignments.Create(ctx, a)
}

func TestConnectorTaskService_FailPath_AnAbsorbedRepeatNeitherAssignsNorSignals(t *testing.T) {
	cases := []struct {
		name     string
		seedKey  string
		seedVal  string
		wantErr  error
		wantDels int
	}{
		{"a fail already delivered is acknowledged", "fail", "delivered", nil, 0},
		{"a fail still reserved by another attempt is told to retry later", "fail", "reserved", port.ErrCallbackInFlight, 0},
		{"a complete already delivered acknowledges the fail and gives back the reservation it took", "complete", "delivered", nil, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := w11aNewConnector(`{}`)
			h.cache.put(h.key(tc.seedKey), tc.seedVal)

			err := h.fail("upstream_error", map[string]any{"detail": "x"})

			if tc.wantErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.wantErr)
			}
			assert.Empty(t, h.temporal.sent(), "no signal goes out")
			attempts, rows := h.assignments.counts()
			assert.Zero(t, attempts, "no assignment is attempted")
			assert.Zero(t, rows)
			v, _ := h.cache.value(h.key(tc.seedKey))
			assert.Equal(t, tc.seedVal, v, "the other attempt's reservation is left as it was")
			assert.Len(t, h.cache.opsNamed("del", h.key("fail")), tc.wantDels, "only a reservation this attempt took is released")
		})
	}
}

func TestConnectorTaskService_FailPath_AFailedAssignmentReleasesTheReservationAndTheRetrySignalsOnce(t *testing.T) {
	cases := []struct {
		name      string
		extras    string
		wantClass string
		call      func(h *w11aConnector) error
	}{
		{
			name: "the fail callback", extras: `{}`, wantClass: "upstream_error",
			call: func(h *w11aConnector) error { return h.fail("upstream_error", nil) },
		},
		{
			name: "a complete whose stored output mapping cannot be read", extras: `{not json`, wantClass: "output_mapping_invalid",
			call: func(h *w11aConnector) error { return h.complete(map[string]any{"k": "v"}) },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := w11aNewConnector(tc.extras)
			flaky := &w16aFlakyAssignments{w11aAssignments: h.assignments, failuresLeft: 1}
			h.svc.Assignments = flaky

			err := tc.call(h)

			require.ErrorIs(t, err, errW16aAssignDown)
			assert.NotEqual(t, errW16aAssignDown, err, "the insert failure is wrapped with what was being done")
			assert.Empty(t, h.temporal.sent(), "a task whose principal is not recorded is not signalled")
			require.Len(t, h.assignments.attempts, 1)
			assert.Equal(t, h.tenant, h.assignments.attempts[0].TenantID)
			assert.Equal(t, h.task.ID, h.assignments.attempts[0].TaskID)
			assert.Equal(t, w11aActor, h.assignments.attempts[0].UserID)
			_, held := h.cache.value(h.key("fail"))
			assert.False(t, held, "the reservation is released so the worker's retry can signal")
			assert.Len(t, h.cache.opsNamed("del", h.key("fail")), 1)
			assert.Empty(t, h.cache.opsNamed("set", h.key("fail")), "nothing is marked delivered")

			require.NoError(t, tc.call(h), "the retry goes through")

			sent := h.temporal.sent()
			require.Len(t, sent, 1)
			assert.Equal(t, "stage-fail", sent[0].Name)
			assert.Equal(t, h.inst.ID, sent[0].Instance)
			assert.Equal(t, h.inst.TemporalWorkflowID, sent[0].WorkflowID)
			wire := w11aDecode[w11aFailWire](t, sent[0].Payload)
			assert.Equal(t, tc.wantClass, wire.ErrorClass)
			assert.Equal(t, "dept-9", wire.DeptID)
			assert.Equal(t, "node-A", wire.NodeID)
			assert.Equal(t, "storage", wire.ConnectorType)
			assert.Equal(t, int64(6), wire.RecordVersion)
			v, _ := h.cache.value(h.key("fail"))
			assert.Equal(t, "delivered", v)
			_, rows := h.assignments.counts()
			assert.Equal(t, 1, rows, "one assignment row for the principal")
		})
	}
}
