package main

import (
	"context"
	"net/http"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/logger"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkeystream"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

const dspfixConnType = "dspfix-conn"

func dspfixDeadLetteredCount(t *testing.T, label string) float64 {
	t.Helper()
	log, err := logger.NewLogger("dev")
	require.NoError(t, err)
	_ = gincommon.ObservabilityMiddlewares(gincommon.Config{
		Logger: log, ServiceName: "workflow-execution-service", BuildVersion: "test", Domain: observability.Domain, Environment: "test",
	})
	observability.Register()
	return testutil.ToFloat64(observability.ConnectorDispatchDeadLetteredTotal.WithLabelValues(label))
}

func newMalformedWorker(t *testing.T) (*dspfixWorker, *dspfixGate) {
	t.Helper()
	conn := newDspfixGate(dspfixConnType, t)
	conn.open()
	return newDspfixWorker(t, http.StatusNoContent, map[string]*typePool{conn.typ: dspfixPool(conn, 1)}), conn
}

// An entry the worker cannot read is neither run nor dropped: it is copied to the dead-letter stream with a
// reason, which raises the alert, and its task stays open for an operator.
func TestDispatch_MalformedEntry_IsDeadLetteredNotDropped(t *testing.T) {
	cases := map[string]map[string]string{
		"tenant id is not a uuid":             {"tenant_id": "tenant-one"},
		"task id is not a uuid":               {"task_id": "7"},
		"department id is not a uuid":         {"department_id": "dept"},
		"department id is empty":              {"department_id": ""},
		"department id is the all-zero uuid":  {"department_id": uuid.Nil.String()},
		"inputs are a json array":             {"resolved_inputs": `[1,2]`},
		"inputs are a json string":            {"resolved_inputs": `"text"`},
		"inputs are not json":                 {"resolved_inputs": `{"broken"`},
		"inputs are a number":                 {"resolved_inputs": `42`},
		"tenant id missing from the entry":    {"tenant_id": ""},
		"all-zero department with good input": {"department_id": uuid.Nil.String(), "resolved_inputs": `{"a":1}`},
	}
	for name, override := range cases {
		t.Run(name, func(t *testing.T) {
			w, conn := newMalformedWorker(t)
			entry, _, _ := dspfixEntry("41-0", dspfixConnType, override)
			before := dspfixDeadLetteredCount(t, dspfixConnType)

			w.dispatch(context.Background(), entry)

			dead := w.stream.deadList()
			require.Len(t, dead, 1, "the entry was not moved to the dead-letter stream")
			assert.Equal(t, dspfixStream, dead[0].stream)
			assert.Equal(t, dspfixGroup, dead[0].group)
			assert.Equal(t, dspfixDeadLetter, dead[0].deadKey)
			assert.Equal(t, "malformed_entry", dead[0].reason)
			assert.Equal(t, "41-0", dead[0].entry.ID)
			assert.Equal(t, entry.Fields, dead[0].entry.Fields, "the dead-letter copy must carry the entry unmodified")
			assert.Empty(t, w.stream.ackList(), "the dead-letter move already acknowledges; a second plain ack means the entry was dropped")
			assert.Zero(t, conn.runs.Load(), "a malformed entry ran a connector")
			assert.Empty(t, w.callbacks.received(), "a malformed entry sent a callback")
			assert.InDelta(t, before+1, dspfixDeadLetteredCount(t, dspfixConnType), 0, "the dead-letter metric did not count the entry")
		})
	}
}

// The metric label must not take an arbitrary string from an unreadable entry.
func TestDispatch_MalformedEntry_UnknownConnectorTypeIsCountedUnderAFixedLabel(t *testing.T) {
	w, _ := newMalformedWorker(t)
	entry, _, _ := dspfixEntry("42-0", "made-up-by-an-attacker", map[string]string{"tenant_id": "bad"})
	beforeFixed := dspfixDeadLetteredCount(t, "unknown")
	beforeRaw := dspfixDeadLetteredCount(t, "made-up-by-an-attacker")

	w.dispatch(context.Background(), entry)

	require.Len(t, w.stream.deadList(), 1)
	assert.InDelta(t, beforeFixed+1, dspfixDeadLetteredCount(t, "unknown"), 0)
	assert.InDelta(t, beforeRaw, dspfixDeadLetteredCount(t, "made-up-by-an-attacker"), 0)
}

// A dead-letter that fails leaves the entry pending, so the next delivery tries again and the entry is not lost.
func TestDispatch_MalformedEntry_DeadLetterFailureLeavesItPending(t *testing.T) {
	w, conn := newMalformedWorker(t)
	w.stream.deadErr = assert.AnError
	entry, _, _ := dspfixEntry("43-0", dspfixConnType, map[string]string{"department_id": uuid.Nil.String()})
	before := dspfixDeadLetteredCount(t, dspfixConnType)

	w.dispatch(context.Background(), entry)

	assert.Empty(t, w.stream.deadList())
	assert.Empty(t, w.stream.ackList(), "an entry that could not be dead-lettered was acknowledged")
	assert.Zero(t, conn.runs.Load())
	assert.InDelta(t, before, dspfixDeadLetteredCount(t, dspfixConnType), 0, "a failed dead-letter was counted")
}

// An entry with a real department and no inputs at all is a valid task: a connector may need none.
func TestDispatch_EntryWithoutInputsOrOneWithAnObject_IsStillRun(t *testing.T) {
	for name, inputs := range map[string]map[string]string{
		"no inputs field":       nil,
		"empty object":          {"resolved_inputs": `{}`},
		"object with one field": {"resolved_inputs": `{"label":"x"}`},
	} {
		t.Run(name, func(t *testing.T) {
			w, conn := newMalformedWorker(t)
			entry, tenantID, taskID := dspfixEntry("44-0", dspfixConnType, inputs)

			w.dispatch(context.Background(), entry)

			assert.Equal(t, int32(1), conn.runs.Load())
			assert.Empty(t, w.stream.deadList())
			assert.Equal(t, []dspfixStreamCall{{stream: dspfixStream, group: dspfixGroup, id: "44-0"}}, w.stream.ackList())
			calls := w.callbacks.received()
			require.Len(t, calls, 1)
			assert.Equal(t, dspfixCallbackPath(taskID, "complete"), calls[0].path)
			assert.Equal(t, tenantID.String(), calls[0].tenant)
		})
	}
}

// A malformed entry in a batch does not stop the readable entries beside it.
func TestDispatchBatch_MalformedEntryBesideAGoodOne_GoodOneStillRuns(t *testing.T) {
	w, conn := newMalformedWorker(t)
	bad, _, _ := dspfixEntry("45-0", dspfixConnType, map[string]string{"task_id": "x"})
	good, _, goodTask := dspfixEntry("46-0", dspfixConnType, nil)
	var wg sync.WaitGroup

	dispatchBatch(context.Background(), w.d, &wg, []valkeystream.Entry{bad, good}, noopLogger{})
	wg.Wait()

	require.Len(t, w.stream.deadList(), 1)
	assert.Equal(t, "45-0", w.stream.deadList()[0].entry.ID)
	assert.Equal(t, int32(1), conn.runs.Load())
	calls := w.callbacks.received()
	require.Len(t, calls, 1)
	assert.Equal(t, dspfixCallbackPath(goodTask, "complete"), calls[0].path)
}
