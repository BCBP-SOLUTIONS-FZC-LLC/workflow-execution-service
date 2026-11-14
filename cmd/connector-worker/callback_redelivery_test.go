package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	cwcliAckFailedMsg      = "connector-worker: ack failed"
	cwcliCallbackFailedMsg = "connector-worker: callback failed, leaving entry unacked for redelivery"
)

func cwcliCompletePath(taskID uuid.UUID) string {
	return "/api/v1/internal/connector-tasks/" + taskID.String() + "/complete"
}

// A failed XACK after an accepted callback only costs a redelivery: the entry comes back, the recorded outcome is
// found, and the same callback is posted again without running the connector a second time.
func TestDispatch_AckFailureAfterAcceptedCallback_RedeliveryResendsTheSameCallback(t *testing.T) {
	callbacks := newCwcliUpstream(t, http.StatusNoContent)
	consumer := &cwcliConsumer{ackErr: errors.New("xack: connection reset by peer")}
	conn := &cwcliConnector{typ: "cwcli-ack"}
	log := &cwcliLogger{}
	d := &deps{
		consumer: consumer, dedup: newCwcliCache(), pools: cwcliPoolFor(conn),
		completion: newCompletionClient(callbacks.URL, time.Second),
		streamKey:  cwcliStream, streamGroup: cwcliGroup, consumerName: cwcliConsumerName,
	}
	tenantID, taskID := uuid.New(), uuid.New()

	cwcliDispatch(d, cwcliEntry(tenantID, taskID, conn.typ, 1), log)

	require.Equal(t, 1, conn.runs())
	first := callbacks.requests()
	require.Len(t, first, 1)
	assert.Equal(t, http.MethodPost, first[0].method)
	assert.Equal(t, cwcliCompletePath(taskID), first[0].path)
	assert.Equal(t, tenantID.String(), first[0].header.Get("x-tenant-id"))
	assert.Equal(t, "iam-system", first[0].header.Get("x-user-id"))
	body := cwcliDecodeCallback(t, first[0].body)
	assert.Equal(t, tenantID, body.TenantID)
	assert.Equal(t, map[string]any{"result": "ok"}, body.Output)
	assert.Equal(t, []cwcliStreamCall{{stream: cwcliStream, group: cwcliGroup, entryID: "1-0"}}, consumer.ackCalls())
	warns := log.withMessage("warn", cwcliAckFailedMsg)
	require.Len(t, warns, 1)
	assert.Equal(t, "1-0", warns[0].fields["entry_id"])
	assert.Equal(t, "xack: connection reset by peer", warns[0].fields["error"])

	cwcliDispatch(d, cwcliEntry(tenantID, taskID, conn.typ, 2), log)

	assert.Equal(t, 1, conn.runs(), "the redelivery must not run the connector again")
	second := callbacks.requests()
	require.Len(t, second, 2)
	assert.Equal(t, first[0].path, second[1].path)
	assert.Equal(t, first[0].body, second[1].body, "the same callback is posted again")
	assert.Len(t, consumer.ackCalls(), 2, "the redelivered entry is acked again")
	assert.Len(t, log.withMessage("warn", cwcliAckFailedMsg), 2)
}

// A transport failure of the callback (nothing listening) is not a status error: the warn names the task and the
// error only, nothing is acked, and the redelivery re-sends the recorded outcome once the server is back.
func TestDispatch_CallbackTransportFailure_LeavesTheEntryUnackedAndRedeliveryResendsTheRecordedOutcome(t *testing.T) {
	consumer := &cwcliConsumer{}
	conn := &cwcliConnector{typ: "cwcli-transport"}
	log := &cwcliLogger{}
	d := &deps{
		consumer: consumer, dedup: newCwcliCache(), pools: cwcliPoolFor(conn),
		completion: newCompletionClient("http://127.0.0.1:1", time.Second),
		streamKey:  cwcliStream, streamGroup: cwcliGroup, consumerName: cwcliConsumerName,
	}
	tenantID, taskID := uuid.New(), uuid.New()

	cwcliDispatch(d, cwcliEntry(tenantID, taskID, conn.typ, 1), log)

	require.Equal(t, 1, conn.runs())
	assert.Empty(t, consumer.ackCalls(), "an unreported outcome must stay pending")
	warns := log.withMessage("warn", cwcliCallbackFailedMsg)
	require.Len(t, warns, 1)
	assert.Equal(t, taskID, warns[0].fields["task_id"])
	assert.Contains(t, warns[0].fields["error"], "call "+cwcliCompletePath(taskID)+": ")
	assert.NotContains(t, warns[0].fields, "status")
	assert.NotContains(t, warns[0].fields, "body")
	assert.Empty(t, log.withMessage("warn", cwcliAckFailedMsg))

	callbacks := newCwcliUpstream(t, http.StatusNoContent)
	d.completion = newCompletionClient(callbacks.URL, time.Second)
	cwcliDispatch(d, cwcliEntry(tenantID, taskID, conn.typ, 2), log)

	assert.Equal(t, 1, conn.runs(), "the redelivery re-sends, it does not re-run")
	reqs := callbacks.requests()
	require.Len(t, reqs, 1)
	assert.Equal(t, cwcliCompletePath(taskID), reqs[0].path)
	assert.Equal(t, map[string]any{"result": "ok"}, cwcliDecodeCallback(t, reqs[0].body).Output)
	assert.Len(t, consumer.ackCalls(), 1)
}

func TestCompletionClient_TransportFailure_IsACallErrorWithoutAStatus(t *testing.T) {
	tenantID, taskID := uuid.New(), uuid.New()

	t.Run("nothing listening", func(t *testing.T) {
		err := newCompletionClient("http://127.0.0.1:1", time.Second).Complete(context.Background(), tenantID, taskID, nil)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "call "+cwcliCompletePath(taskID)+": ")
		var statusErr *callbackStatusError
		assert.False(t, errors.As(err, &statusErr), "a transport failure carries no status")
	})

	t.Run("server slower than the completion timeout", func(t *testing.T) {
		arrived := make(chan cwcliRequest, 1)
		release := make(chan struct{})
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			arrived <- cwcliRequest{method: r.Method, path: r.URL.Path, header: r.Header.Clone()}
			select {
			case <-r.Context().Done():
			case <-release:
			}
		}))
		t.Cleanup(srv.Close)
		t.Cleanup(func() { close(release) })

		err := newCompletionClient(srv.URL, 250*time.Millisecond).Fail(context.Background(), tenantID, taskID, "timeout", nil)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "call /api/v1/internal/connector-tasks/"+taskID.String()+"/fail: ")
		var netErr net.Error
		require.ErrorAs(t, err, &netErr)
		assert.True(t, netErr.Timeout(), "the failure is the client timeout")
		var statusErr *callbackStatusError
		assert.False(t, errors.As(err, &statusErr))
		got := cwcliAwaitRequest(t, arrived)
		assert.Equal(t, http.MethodPost, got.method)
		assert.Equal(t, "/api/v1/internal/connector-tasks/"+taskID.String()+"/fail", got.path)
		assert.Equal(t, tenantID.String(), got.header.Get("x-tenant-id"))
	})

	t.Run("unparsable address", func(t *testing.T) {
		err := newCompletionClient("http://[::1", time.Second).Complete(context.Background(), tenantID, taskID, nil)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "build request: ")
		var statusErr *callbackStatusError
		assert.False(t, errors.As(err, &statusErr))
	})
}
