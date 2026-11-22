package handler_test

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkey"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

var (
	errW29bGet   = errors.New("w29b get: i/o timeout")
	errW29bSet   = errors.New("w29b set: connection reset")
	errW29bSetNX = errors.New("w29b setnx: connection refused")
)

type w29bCacheWrite struct {
	op, key, value string
	ttl            time.Duration
}

// w29bCmdable is the Valkey client under the real valkey.Cache: an in-memory store whose reads, writes
// and reservations each fail on demand, with every write recorded.
type w29bCmdable struct {
	redis.Cmdable
	values   map[string]string
	getErr   error
	setErr   error
	setNXErr error
	writes   []w29bCacheWrite
}

func (c *w29bCmdable) Get(_ context.Context, key string) *redis.StringCmd {
	if c.getErr != nil {
		return redis.NewStringResult("", c.getErr)
	}
	val, ok := c.values[key]
	if !ok {
		return redis.NewStringResult("", redis.Nil)
	}
	return redis.NewStringResult(val, nil)
}

func (c *w29bCmdable) Set(_ context.Context, key string, value any, ttl time.Duration) *redis.StatusCmd {
	c.writes = append(c.writes, w29bCacheWrite{"set", key, value.(string), ttl})
	if c.setErr != nil {
		return redis.NewStatusResult("", c.setErr)
	}
	c.values[key] = value.(string)
	return redis.NewStatusResult("OK", nil)
}

func (c *w29bCmdable) SetNX(_ context.Context, key string, value any, ttl time.Duration) *redis.BoolCmd {
	c.writes = append(c.writes, w29bCacheWrite{"setnx", key, value.(string), ttl})
	if c.setNXErr != nil {
		return redis.NewBoolResult(false, c.setNXErr)
	}
	if _, held := c.values[key]; held {
		return redis.NewBoolResult(false, nil)
	}
	c.values[key] = value.(string)
	return redis.NewBoolResult(true, nil)
}

func (c *w29bCmdable) Del(_ context.Context, keys ...string) *redis.IntCmd {
	for _, k := range keys {
		c.writes = append(c.writes, w29bCacheWrite{op: "del", key: k})
		delete(c.values, k)
	}
	return redis.NewIntResult(int64(len(keys)), nil)
}

// w29bFailCallbacks serves the connector callback routes from the real service over the real Valkey cache
// adapter, whose client is the scripted w29bCmdable.
func w29bFailCallbacks(cmd *w29bCmdable) *w11aCallbacks {
	h := w11aNewCallbacks(`{}`)
	inst := &domain.Instance{
		ID: h.task.WorkflowInstanceID, TenantID: h.tenant, TemporalWorkflowID: "w29b:workflow", Status: domain.InstanceStatusRunning,
	}
	svc := &service.ConnectorTaskService{
		Instances: &w11aInstances{rows: []*domain.Instance{inst}}, Tasks: h.tasks, Assignments: h.assignments,
		Temporal: h.temporal, Cache: valkey.NewCache(cmd), Principals: h.principals, Log: h.log,
	}
	h.router = newInternalRouter(handler.New(handler.Services{ConnectorTasks: svc}))
	return h
}

func TestFailConnectorTask_DedupCacheFails_Returns204AndTheSignalIsSent(t *testing.T) {
	cases := []struct {
		name       string
		configure  func(c *w29bCmdable)
		wantWarn   string
		wantErrIn  string
		wantCause  error
		wantWrites func(t *testing.T, h *w11aCallbacks, c *w29bCmdable)
	}{
		{
			name:      "the reservation fails",
			configure: func(c *w29bCmdable) { c.setNXErr = errW29bSetNX },
			wantWarn:  "connector-task signal dedup check failed, proceeding fail-open",
			wantErrIn: "valkey setnx: ", wantCause: errW29bSetNX,
			wantWrites: func(t *testing.T, h *w11aCallbacks, c *w29bCmdable) {
				assert.Equal(t, "delivered", c.values[h.key("fail")], "the delivered mark is still written after the signal")
			},
		},
		{
			name:      "the earlier-outcome read fails",
			configure: func(c *w29bCmdable) { c.getErr = errW29bGet },
			wantWarn:  "connector-task signal dedup read failed, proceeding fail-open",
			wantErrIn: "valkey get: ", wantCause: errW29bGet,
			wantWrites: func(t *testing.T, h *w11aCallbacks, c *w29bCmdable) {
				assert.Equal(t, "delivered", c.values[h.key("fail")], "the reservation is taken and marked delivered although the reads failed")
			},
		},
		{
			name:      "marking the signal delivered fails",
			configure: func(c *w29bCmdable) { c.setErr = errW29bSet },
			wantWarn:  "connector-task signal dedup could not be marked delivered",
			wantErrIn: "valkey set: ", wantCause: errW29bSet,
			wantWrites: func(t *testing.T, h *w11aCallbacks, c *w29bCmdable) {
				assert.Equal(t, []w29bCacheWrite{
					{op: "setnx", key: h.key("fail"), value: "reserved", ttl: 30 * time.Second},
					{op: "set", key: h.key("fail"), value: "delivered", ttl: 5 * time.Minute},
				}, c.writes, "the reservation was taken for 30 s and the delivered mark was tried once")
				assert.Equal(t, "reserved", c.values[h.key("fail")], "the key stays reserved until its 30 s expire")
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := &w29bCmdable{values: map[string]string{}}
			tc.configure(cmd)
			h := w29bFailCallbacks(cmd)

			w := h.fail("")

			require.Equal(t, http.StatusNoContent, w.Code, w.Body.String())
			assert.Empty(t, w.Body.String())
			require.Len(t, h.temporal.signals, 1, "the signal is sent")
			assert.Equal(t, "w29b:workflow", h.temporal.signals[0].WorkflowID)
			assert.Equal(t, "stage-fail", h.temporal.signals[0].Name)
			var warned []w11aWarn
			for _, e := range h.log.warns {
				if e.Msg == tc.wantWarn {
					warned = append(warned, e)
				}
			}
			require.NotEmpty(t, warned, "the cache failure is logged: %v", h.log.warns)
			for _, e := range warned {
				assert.Equal(t, h.task.ID, e.Fields["task_id"])
				assert.Contains(t, e.Fields["error"], tc.wantErrIn)
				assert.Contains(t, e.Fields["error"], tc.wantCause.Error())
			}
			tc.wantWrites(t, h, cmd)
		})
	}
}
