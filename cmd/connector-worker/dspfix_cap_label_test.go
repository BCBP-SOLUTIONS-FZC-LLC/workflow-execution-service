package main

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// An entry past its delivery cap whose connector type the worker does not run is counted under "unknown": the
// entry's own text never becomes a metric label value, the same as for an unreadable entry.
func TestDispatch_PastDeliveryCap_UnknownConnectorTypeIsCountedAsUnknown(t *testing.T) {
	w, _ := newCapWorker(t, http.StatusNoContent)
	entry, _, _ := dspfixEntry("55-0", "made-up-by-a-template", nil)
	entry.Deliveries = dspfixCap + 1
	beforeUnknown := dspfixDeadLetteredCount(t, "unknown")
	beforeRaw := dspfixDeadLetteredCount(t, "made-up-by-a-template")

	w.dispatch(context.Background(), entry)

	require.Len(t, w.stream.deadList(), 1)
	assert.InDelta(t, beforeUnknown+1, dspfixDeadLetteredCount(t, "unknown"), 0)
	assert.InDelta(t, beforeRaw, dspfixDeadLetteredCount(t, "made-up-by-a-template"), 0, "the raw type became a label")
}
