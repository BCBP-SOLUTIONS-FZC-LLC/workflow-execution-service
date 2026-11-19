package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/connectors"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/registry"
)

const w27bCaptureType = "w27b-capture"

// w27bCaptureConnector records what its context carried at the call.
type w27bCaptureConnector struct {
	gucTenant   string
	gucFound    bool
	deadline    time.Time
	hasDeadline bool
	startedAt   time.Time
	// block makes Execute wait for its context to end and return that error.
	block bool
}

func (c *w27bCaptureConnector) Type() string { return w27bCaptureType }

func (c *w27bCaptureConnector) Execute(ctx context.Context, _ map[string]any) (map[string]any, error) {
	guc, found := pgcommon.GUCSetFromContext(ctx)
	c.gucTenant, c.gucFound = guc.TenantID, found
	c.deadline, c.hasDeadline = ctx.Deadline()
	c.startedAt = time.Now()
	if c.block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return map[string]any{}, nil
}

func w27bPool(conn connectors.Connector, timeout time.Duration) *typePool {
	return &typePool{connectorType: conn.Type(), sem: make(chan struct{}, 1), timeout: timeout, connector: conn}
}

func TestExecute_PutsTheTenantGUCOnTheConnectorContextAndBoundsTheRunByThePoolTimeout(t *testing.T) {
	conn := &w27bCaptureConnector{}
	job := dispatchJob{tenantID: uuid.New(), taskID: uuid.New(), departmentID: uuid.New(), principalSub: "w27b-principal", connectorType: w27bCaptureType}
	const poolTimeout = time.Hour
	parent, cancel := context.WithTimeout(context.Background(), 3*time.Hour)
	defer cancel()
	parentDeadline, _ := parent.Deadline()

	before := time.Now()
	outcome := execute(parent, &deps{retry: testBounds}, w27bPool(conn, poolTimeout), job, noopLogger{})
	after := time.Now()

	require.Empty(t, outcome.ErrorClass)
	assert.True(t, conn.gucFound, "the connector context carries a pg tenant setting")
	assert.Equal(t, job.tenantID.String(), conn.gucTenant, "the setting names the job's tenant")
	require.True(t, conn.hasDeadline, "the run is bounded")
	assert.False(t, conn.deadline.Before(before.Add(poolTimeout)), "the bound is the pool timeout from the start of the run")
	assert.False(t, conn.deadline.After(after.Add(poolTimeout)))
	assert.True(t, conn.deadline.Before(parentDeadline), "the pool timeout is tighter than the caller's own deadline")
}

func TestExecute_ConnectorRunningPastThePoolTimeout_EndsWithTheTimeoutClass(t *testing.T) {
	conn := &w27bCaptureConnector{block: true}
	job := dispatchJob{tenantID: uuid.New(), taskID: uuid.New(), departmentID: uuid.New(), connectorType: w27bCaptureType}

	synctest.Test(t, func(t *testing.T) {
		start := time.Now()

		outcome := execute(context.Background(), &deps{retry: testBounds}, w27bPool(conn, time.Hour), job, noopLogger{})

		assert.Equal(t, dispatchOutcome{ErrorClass: "timeout"}, outcome)
		assert.Equal(t, time.Hour, time.Since(start), "the run was cut off exactly at the pool timeout, on the fake clock")
	})
}

func TestExecute_ConnectorFails_ReturnsThePartialOutputWithTheErrorClass(t *testing.T) {
	cases := []struct {
		name      string
		steps     []step
		wantClass string
		wantOut   map[string]any
		wantCalls int
	}{
		{
			name:      "validation failure with partial output",
			steps:     []step{{out: map[string]any{"partial": "kept"}, err: fmt.Errorf("bad input: %w", connectors.ErrValidation)}},
			wantClass: "validation_error",
			wantOut:   map[string]any{"partial": "kept"},
			wantCalls: 1,
		},
		{
			name:      "transient failures that exhaust the attempts keep the last output",
			steps:     []step{{out: map[string]any{"attempt": "one"}, err: transient(connectors.ErrUpstream)}, {out: map[string]any{"attempt": "two"}, err: transient(connectors.ErrUpstream)}},
			wantClass: "upstream_error",
			wantOut:   map[string]any{"attempt": "two"},
			wantCalls: 2,
		},
		{
			name:      "an unclassified failure without output has no output",
			steps:     []step{{err: errors.New("boom")}},
			wantClass: "unknown_error",
			wantOut:   nil,
			wantCalls: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn := &scriptedConnector{typ: registry.TypeStorage, steps: tc.steps}
			pool := w27bPool(conn, time.Second)
			job := dispatchJob{tenantID: uuid.New(), taskID: uuid.New(), departmentID: uuid.New(), connectorType: registry.TypeStorage}
			d := &deps{retry: retryBounds{MaxAttempts: 2, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond}}

			outcome := execute(context.Background(), d, pool, job, noopLogger{})

			assert.Equal(t, dispatchOutcome{Output: tc.wantOut, ErrorClass: tc.wantClass}, outcome)
			assert.Equal(t, tc.wantCalls, conn.calls)
		})
	}
}

func TestExecuteWithRetry_AttemptBound(t *testing.T) {
	cases := []struct {
		name        string
		maxAttempts int
		wantCalls   int
	}{
		{name: "a bound of one gives a single attempt", maxAttempts: 1, wantCalls: 1},
		{name: "a bound of zero gives a single attempt", maxAttempts: 0, wantCalls: 1},
		{name: "a negative bound gives a single attempt", maxAttempts: -3, wantCalls: 1},
		{name: "a bound of three gives three attempts", maxAttempts: 3, wantCalls: 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn := &scriptedConnector{typ: registry.TypeStorage, steps: []step{{err: transient(connectors.ErrUpstream)}}}
			var observed []int
			bounds := retryBounds{MaxAttempts: tc.maxAttempts, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond}

			_, err := executeWithRetry(context.Background(), conn, "", nil, bounds, func(attempt int, d connectors.RetryDecision) {
				assert.True(t, d.Retry, "the failure itself is retryable; only the bound stops it")
				observed = append(observed, attempt)
			})

			require.ErrorIs(t, err, connectors.ErrUpstream)
			assert.Contains(t, err.Error(), "connector execute: ")
			assert.Equal(t, tc.wantCalls, conn.calls)
			assert.Len(t, observed, tc.wantCalls)
		})
	}
}

func TestDispatch_PastTheCap_DeadLetterWriteResult_DecidesTheDeadLetterAndTheMetric(t *testing.T) {
	cases := []struct {
		name          string
		deadErr       error
		wantDead      int
		wantMetricInc float64
	}{
		{name: "the dead-letter write fails", deadErr: assert.AnError, wantDead: 0, wantMetricInc: 0},
		{name: "the dead-letter write succeeds", deadErr: nil, wantDead: 1, wantMetricInc: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obstest.Init(t)
			const metric = "workflow_execution_service_connector_dispatch_dead_lettered_total"
			labels := map[string]string{"connector_type": dspfixConnType}
			before := obstest.Sum(t, metric, labels)
			conn := newDspfixGate(dspfixConnType, t)
			conn.open()
			w := newDspfixWorker(t, 204, map[string]*typePool{conn.typ: dspfixPool(conn, 1)})
			w.d.maxDeliveries = 2
			w.stream.deadErr = tc.deadErr
			entry, _, _ := dspfixEntry("w27b-5-0", conn.typ, nil)
			entry.Deliveries = 3

			w.dispatch(context.Background(), entry)

			assert.Len(t, w.stream.deadList(), tc.wantDead)
			assert.Empty(t, w.stream.ackList(), "the entry is not acknowledged here: a failed write leaves it pending, a successful dead-letter acknowledges inside the consumer")
			assert.Zero(t, conn.runs.Load(), "the connector never ran")
			assert.Empty(t, w.callbacks.received(), "no callback was sent")
			assert.Equal(t, before+tc.wantMetricInc, obstest.Sum(t, metric, labels))
		})
	}
}
