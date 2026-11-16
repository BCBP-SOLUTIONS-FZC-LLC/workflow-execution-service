package main

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type gatefixCall struct{ tenantID, taskID uuid.UUID }

// gatefixGate answers every question the same way and records who asked.
type gatefixGate struct {
	mu       sync.Mutex
	dispatch bool
	err      error
	asked    []gatefixCall
}

func (g *gatefixGate) Dispatchable(_ context.Context, tenantID, taskID uuid.UUID) (bool, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.asked = append(g.asked, gatefixCall{tenantID: tenantID, taskID: taskID})
	return g.dispatch, g.err
}

func (g *gatefixGate) calls() []gatefixCall {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]gatefixCall(nil), g.asked...)
}

var _ port.ConnectorDispatchGate = (*gatefixGate)(nil)

func newGatefixWorker(t *testing.T, gate *gatefixGate) (*dspfixWorker, *dspfixGate) {
	t.Helper()
	conn := newDspfixGate(dspfixConnType, t)
	conn.open()
	w := newDspfixWorker(t, http.StatusNoContent, map[string]*typePool{conn.typ: dspfixPool(conn, 1)})
	w.d.gate = gate
	return w, conn
}

// A task that is finished or whose instance has ended is acknowledged without running the connector: a side
// effect such as an email cannot be undone by the callback refusing the result.
func TestDispatch_TaskNoLongerOpen_AcksWithoutRunOrCallback(t *testing.T) {
	gate := &gatefixGate{dispatch: false}
	w, conn := newGatefixWorker(t, gate)
	entry, tenantID, taskID := dspfixEntry("61-0", dspfixConnType, nil)

	w.dispatch(context.Background(), entry)

	assert.Equal(t, []gatefixCall{{tenantID: tenantID, taskID: taskID}}, gate.calls())
	assert.Zero(t, conn.runs.Load(), "the connector ran for a task that is no longer open")
	assert.Empty(t, w.callbacks.received(), "a callback was sent for a skipped task")
	assert.Equal(t, []dspfixStreamCall{{stream: dspfixStream, group: dspfixGroup, id: "61-0"}}, w.stream.ackList())
	assert.Empty(t, w.stream.deadList())
	raw, err := w.cache.Get(context.Background(), outcomeKey(taskID))
	require.NoError(t, err)
	assert.Empty(t, raw, "an outcome was recorded for a skipped task")
}

func TestDispatch_TaskStillOpen_RunsAndReports(t *testing.T) {
	gate := &gatefixGate{dispatch: true}
	w, conn := newGatefixWorker(t, gate)
	entry, _, taskID := dspfixEntry("62-0", dspfixConnType, nil)

	w.dispatch(context.Background(), entry)

	assert.Len(t, gate.calls(), 1)
	assert.Equal(t, int32(1), conn.runs.Load())
	calls := w.callbacks.received()
	require.Len(t, calls, 1)
	assert.Equal(t, dspfixCallbackPath(taskID, "complete"), calls[0].path)
	assert.Equal(t, []dspfixStreamCall{{stream: dspfixStream, group: dspfixGroup, id: "62-0"}}, w.stream.ackList())
}

// When the task state cannot be read the connector may not run and the entry stays pending for a redelivery.
func TestDispatch_TaskStateUnreadable_LeavesEntryPendingWithoutRun(t *testing.T) {
	gate := &gatefixGate{err: errors.New("database unreachable")}
	w, conn := newGatefixWorker(t, gate)
	entry, _, taskID := dspfixEntry("63-0", dspfixConnType, nil)

	w.dispatch(context.Background(), entry)

	assert.Len(t, gate.calls(), 1)
	assert.Zero(t, conn.runs.Load(), "the connector ran although the task state was unreadable")
	assert.Empty(t, w.callbacks.received())
	assert.Empty(t, w.stream.ackList(), "the entry was acknowledged although the task state was unreadable")
	assert.Empty(t, w.stream.deadList())

	gate.mu.Lock()
	gate.err, gate.dispatch = nil, true
	gate.mu.Unlock()
	w.dispatch(context.Background(), entry)
	assert.Equal(t, int32(1), conn.runs.Load(), "the redelivery did not run once the state was readable")
	require.Len(t, w.callbacks.received(), 1)
	assert.Equal(t, dspfixCallbackPath(taskID, "complete"), w.callbacks.received()[0].path)
}
