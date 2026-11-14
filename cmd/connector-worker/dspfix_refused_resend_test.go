package main

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Past the cap, a recorded outcome the server refuses for good (a 4xx other than 408 and 429) is moved to the
// dead-letter stream instead of being resent for ever; below the cap the entry stays pending for redelivery.
func TestDispatch_PermanentlyRefusedResend_IsDeadLetteredOnlyPastTheCap(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusConflict, http.StatusUnprocessableEntity} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			w, conn := newCapWorker(t, status)
			entry, _, taskID := dspfixEntry("61-0", dspfixConnType, nil)
			w.record(t, taskID, dispatchOutcome{Output: map[string]any{"result": "kept"}})

			entry.Deliveries = dspfixCap
			w.dispatch(context.Background(), entry)
			assert.Empty(t, w.stream.deadList(), "at the cap the entry is still redelivered")

			entry.Deliveries = dspfixCap + 1
			w.dispatch(context.Background(), entry)

			dead := w.stream.deadList()
			require.Len(t, dead, 1)
			assert.Equal(t, "callback_refused", dead[0].reason)
			assert.Equal(t, "61-0", dead[0].entry.ID)
			assert.Len(t, w.callbacks.received(), 2, "the outcome was resent before giving up")
			assert.Empty(t, w.stream.ackList())
			assert.Zero(t, conn.runs.Load(), "the connector never ran again")
		})
	}
}

// Throttling and request timeouts are not permanent: past the cap the entry stays pending.
func TestDispatch_ThrottledResendPastTheCap_StaysPending(t *testing.T) {
	for _, status := range []int{http.StatusRequestTimeout, http.StatusTooManyRequests} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			w, _ := newCapWorker(t, status)
			entry, _, taskID := dspfixEntry("62-0", dspfixConnType, nil)
			entry.Deliveries = dspfixCap + 1
			w.record(t, taskID, dispatchOutcome{Output: map[string]any{"result": "kept"}})

			w.dispatch(context.Background(), entry)

			assert.Empty(t, w.stream.deadList())
			assert.Empty(t, w.stream.ackList())
		})
	}
}
