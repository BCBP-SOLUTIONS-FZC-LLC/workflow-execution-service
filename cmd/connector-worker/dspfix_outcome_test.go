package main

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const dspfixCap = 3

func newCapWorker(t *testing.T, callbackStatus int) (*dspfixWorker, *dspfixGate) {
	t.Helper()
	conn := newDspfixGate(dspfixConnType, t)
	conn.open()
	w := newDspfixWorker(t, callbackStatus, map[string]*typePool{conn.typ: dspfixPool(conn, 1)})
	w.d.maxDeliveries = dspfixCap
	return w, conn
}

func (w *dspfixWorker) record(t *testing.T, taskID uuid.UUID, outcome dispatchOutcome) {
	t.Helper()
	raw, err := json.Marshal(outcome)
	require.NoError(t, err)
	require.NoError(t, w.cache.Set(context.Background(), outcomeKey(taskID), string(raw), outcomeTTL))
}

// A connector that has already produced an outcome is not run again, and the delivery cap is not a reason to
// discard that outcome: the completion callback is idempotent, so a redelivery past the cap resends it.
func TestDispatch_PastDeliveryCap_RecordedSuccessIsResentAndAcked(t *testing.T) {
	w, conn := newCapWorker(t, http.StatusNoContent)
	entry, tenantID, taskID := dspfixEntry("51-0", dspfixConnType, nil)
	entry.Deliveries = dspfixCap + 4
	w.record(t, taskID, dispatchOutcome{Output: map[string]any{"result": "kept"}})

	w.dispatch(context.Background(), entry)

	calls := w.callbacks.received()
	require.Len(t, calls, 1, "the recorded outcome was not resent")
	assert.Equal(t, dspfixCallbackPath(taskID, "complete"), calls[0].path)
	assert.Equal(t, tenantID.String(), calls[0].tenant)
	assert.Equal(t, map[string]any{"result": "kept"}, calls[0].body["output"])
	assert.Equal(t, []dspfixStreamCall{{stream: dspfixStream, group: dspfixGroup, id: "51-0"}}, w.stream.ackList())
	assert.Empty(t, w.stream.deadList(), "an entry with a recorded outcome was dead-lettered")
	assert.Zero(t, conn.runs.Load(), "the connector ran again")
}

func TestDispatch_PastDeliveryCap_RecordedFailureIsResentAsAFailure(t *testing.T) {
	w, conn := newCapWorker(t, http.StatusNoContent)
	entry, _, taskID := dspfixEntry("52-0", dspfixConnType, nil)
	entry.Deliveries = dspfixCap + 1
	w.record(t, taskID, dispatchOutcome{ErrorClass: "upstream_error"})

	w.dispatch(context.Background(), entry)

	calls := w.callbacks.received()
	require.Len(t, calls, 1)
	assert.Equal(t, dspfixCallbackPath(taskID, "fail"), calls[0].path)
	assert.Equal(t, "upstream_error", calls[0].body["error_class"])
	assert.Equal(t, []dspfixStreamCall{{stream: dspfixStream, group: dspfixGroup, id: "52-0"}}, w.stream.ackList())
	assert.Empty(t, w.stream.deadList())
	assert.Zero(t, conn.runs.Load())
}

// A callback the server cannot take yet (5xx) leaves the entry pending past the cap, with its outcome still
// recorded; it is neither acknowledged nor dead-lettered, and the next delivery resends again.
func TestDispatch_PastDeliveryCap_UnavailableResendStaysPending(t *testing.T) {
	w, conn := newCapWorker(t, http.StatusServiceUnavailable)
	entry, _, taskID := dspfixEntry("53-0", dspfixConnType, nil)
	entry.Deliveries = dspfixCap + 1
	w.record(t, taskID, dispatchOutcome{Output: map[string]any{"result": "kept"}})

	w.dispatch(context.Background(), entry)
	w.dispatch(context.Background(), entry)

	assert.Len(t, w.callbacks.received(), 2, "each redelivery must resend")
	assert.Empty(t, w.stream.ackList())
	assert.Empty(t, w.stream.deadList(), "a refused resend was dead-lettered")
	assert.Zero(t, conn.runs.Load())
}

// The cap still applies to an entry whose connector has produced no outcome: it is not run and is dead-lettered.
func TestDispatch_PastDeliveryCap_WithoutAnOutcomeIsStillDeadLettered(t *testing.T) {
	w, conn := newCapWorker(t, http.StatusNoContent)
	entry, _, _ := dspfixEntry("54-0", dspfixConnType, nil)
	entry.Deliveries = dspfixCap + 1

	w.dispatch(context.Background(), entry)

	dead := w.stream.deadList()
	require.Len(t, dead, 1)
	assert.Equal(t, "max_deliveries_exceeded", dead[0].reason)
	assert.Equal(t, dspfixDeadLetter, dead[0].deadKey)
	assert.Equal(t, "54-0", dead[0].entry.ID)
	assert.Zero(t, conn.runs.Load())
	assert.Empty(t, w.callbacks.received())
	assert.Empty(t, w.stream.ackList())
}

// An outcome that cannot be read is not "no outcome": the entry is left pending, not dead-lettered, because the
// connector may have run.
func TestDispatch_PastDeliveryCap_UnreadableOutcomeStaysPending(t *testing.T) {
	w, conn := newCapWorker(t, http.StatusNoContent)
	entry, _, taskID := dspfixEntry("55-0", dspfixConnType, nil)
	entry.Deliveries = dspfixCap + 1
	require.NoError(t, w.cache.Set(context.Background(), outcomeKey(taskID), "{not json", outcomeTTL))

	w.dispatch(context.Background(), entry)

	assert.Empty(t, w.stream.deadList(), "an entry whose outcome is unreadable was dead-lettered")
	assert.Empty(t, w.stream.ackList())
	assert.Empty(t, w.callbacks.received())
	assert.Zero(t, conn.runs.Load())
}
