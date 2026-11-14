package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

// cbfixCache is a cache whose release and reserve calls can be made to fail.
type cbfixCache struct {
	*fakeCacheStore
	delErr   error
	setNXErr error
	ttls     map[string]time.Duration
}

func (c *cbfixCache) recordTTL(key string, ttl time.Duration) {
	if c.ttls == nil {
		c.ttls = map[string]time.Duration{}
	}
	c.ttls[key] = ttl
}

// expire drops a key the way its TTL passing would.
func (c *cbfixCache) expire(key string) { delete(c.values, key) }

func (c *cbfixCache) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	c.recordTTL(key, ttl)
	return c.fakeCacheStore.Set(ctx, key, value, ttl)
}

func (c *cbfixCache) Del(ctx context.Context, keys ...string) error {
	if c.delErr != nil {
		return c.delErr
	}
	return c.fakeCacheStore.Del(ctx, keys...)
}

func (c *cbfixCache) SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	if c.setNXErr != nil {
		return false, c.setNXErr
	}
	c.recordTTL(key, ttl)
	return c.fakeCacheStore.SetNX(ctx, key, value, ttl)
}

func cbfixService(task *domain.Task, temporal *fakeTemporalClient, cache port.CacheStore, assignments *fakeAssignmentRepo) *service.ConnectorTaskService {
	svc := newConnectorTaskServiceWith(task, instanceWithStatus(task, domain.InstanceStatusRunning), temporal, newFakeCacheStore(), assignments)
	svc.Cache = cache
	return svc
}

func cbfixResultOf(t *testing.T, call signalCall) string {
	t.Helper()
	b, err := json.Marshal(call.Payload)
	require.NoError(t, err)
	var wire struct{ ResultJSON string }
	require.NoError(t, json.Unmarshal(b, &wire))
	return wire.ResultJSON
}

func cbfixSignalFailsOnce() (*fakeTemporalClient, func()) {
	temporal := &fakeTemporalClient{}
	temporal.signalFunc = func(context.Context, string, uuid.UUID, string, any) error {
		return errors.New("transient temporal error")
	}
	return temporal, func() { temporal.signalFunc = nil }
}

// A failed release leaves the reservation standing; the retry must still signal,
// never be acknowledged as if the first attempt had delivered.
func TestConnectorTaskService_Complete_RetryAfterFailedSignalAndFailedReleaseStillSignals(t *testing.T) {
	t.Parallel()

	task := connectorTask("storage", domain.TaskStatusReady, `{}`)
	temporal, signalRecovers := cbfixSignalFailsOnce()
	cache := &cbfixCache{fakeCacheStore: newFakeCacheStore(), delErr: errors.New("cache unreachable")}
	svc := cbfixService(task, temporal, cache, newFakeAssignmentRepo())

	require.ErrorContains(t, svc.Complete(context.Background(), task.TenantID, task.ID, map[string]any{"x": 1}),
		"transient temporal error", "the callback answers with the failure even when the release fails")

	signalRecovers()
	cache.expire(cbfixKey("complete", task))
	require.NoError(t, svc.Complete(context.Background(), task.TenantID, task.ID, map[string]any{"x": 1}))
	require.Len(t, temporal.signals, 2, "the retry must send the signal the first attempt never delivered")
}

func TestConnectorTaskService_Fail_RetryAfterFailedSignalAndFailedReleaseStillSignals(t *testing.T) {
	t.Parallel()

	task := connectorTask("send-email", domain.TaskStatusReady, `{}`)
	temporal, signalRecovers := cbfixSignalFailsOnce()
	cache := &cbfixCache{fakeCacheStore: newFakeCacheStore(), delErr: errors.New("cache unreachable")}
	svc := cbfixService(task, temporal, cache, newFakeAssignmentRepo())

	require.ErrorContains(t, svc.Fail(context.Background(), task.TenantID, task.ID, "upstream_error", nil), "transient temporal error")

	signalRecovers()
	cache.expire(cbfixKey("fail", task))
	require.NoError(t, svc.Fail(context.Background(), task.TenantID, task.ID, "upstream_error", nil))
	require.Len(t, temporal.signals, 2)
	assert.Equal(t, "stage-fail", temporal.signals[1].SignalName)
}

func cbfixKey(outcome string, task *domain.Task) string {
	return "connector-signal:" + outcome + ":" + task.ID.String()
}

// The task has one outcome: once the other callback delivered its signal, the
// second is acknowledged and ignored.
func TestConnectorTaskService_Fail_AfterDeliveredCompleteIsIgnored(t *testing.T) {
	t.Parallel()

	task := connectorTask("storage", domain.TaskStatusReady, `{}`)
	temporal := &fakeTemporalClient{}
	svc := cbfixService(task, temporal, newFakeCacheStore(), newFakeAssignmentRepo())

	require.NoError(t, svc.Complete(context.Background(), task.TenantID, task.ID, map[string]any{}))
	require.NoError(t, svc.Fail(context.Background(), task.TenantID, task.ID, "upstream_error", nil))

	require.Len(t, temporal.signals, 1)
	assert.Equal(t, "stage-transition", temporal.signals[0].SignalName)
}

func TestConnectorTaskService_Complete_AfterDeliveredFailIsIgnored(t *testing.T) {
	t.Parallel()

	task := connectorTask("storage", domain.TaskStatusReady, `{}`)
	temporal := &fakeTemporalClient{}
	svc := cbfixService(task, temporal, newFakeCacheStore(), newFakeAssignmentRepo())

	require.NoError(t, svc.Fail(context.Background(), task.TenantID, task.ID, "upstream_error", nil))
	require.NoError(t, svc.Complete(context.Background(), task.TenantID, task.ID, map[string]any{}))

	require.Len(t, temporal.signals, 1)
	assert.Equal(t, "stage-fail", temporal.signals[0].SignalName)
}

// The other outcome still in flight is not a resolution yet: retry later, and
// leave no reservation of our own behind.
func TestConnectorTaskService_OtherOutcomeInFlight_IsRetryable(t *testing.T) {
	t.Parallel()

	task := connectorTask("storage", domain.TaskStatusReady, `{}`)
	temporal := &fakeTemporalClient{}
	cache := &cbfixCache{fakeCacheStore: newFakeCacheStore()}
	cache.values[cbfixKey("fail", task)] = "reserved"
	svc := cbfixService(task, temporal, cache, newFakeAssignmentRepo())

	err := svc.Complete(context.Background(), task.TenantID, task.ID, map[string]any{})
	require.ErrorIs(t, err, port.ErrCallbackInFlight, "the handler maps this to 503 UPSTREAM_UNAVAILABLE")
	assert.Empty(t, temporal.signals)
	assert.NotContains(t, cache.values, cbfixKey("complete", task), "a backed-off callback must not block its own retry")

	cache.expire(cbfixKey("fail", task))
	require.NoError(t, svc.Complete(context.Background(), task.TenantID, task.ID, map[string]any{}))
	assert.Len(t, temporal.signals, 1)
}

// Two identical callbacks racing: exactly one signal; the loser is told to retry.
func TestConnectorTaskService_ConcurrentIdenticalCallbacks_OneSignal(t *testing.T) {
	t.Parallel()

	for _, kind := range []string{"complete", "fail", "complete then fail"} {
		t.Run(kind, func(t *testing.T) {
			task := connectorTask("storage", domain.TaskStatusReady, `{}`)
			entered := make(chan struct{})
			release := make(chan struct{})
			temporal := &fakeTemporalClient{signalFunc: func(context.Context, string, uuid.UUID, string, any) error {
				close(entered)
				<-release
				return nil
			}}
			svc := cbfixService(task, temporal, newFakeCacheStore(), newFakeAssignmentRepo())
			ctx := context.Background()

			first := make(chan error, 1)
			go func() {
				if kind == "fail" {
					first <- svc.Fail(ctx, task.TenantID, task.ID, "upstream_error", nil)
					return
				}
				first <- svc.Complete(ctx, task.TenantID, task.ID, map[string]any{})
			}()
			<-entered

			var loser error
			switch kind {
			case "complete":
				loser = svc.Complete(ctx, task.TenantID, task.ID, map[string]any{})
			case "fail":
				loser = svc.Fail(ctx, task.TenantID, task.ID, "upstream_error", nil)
			default:
				loser = svc.Fail(ctx, task.TenantID, task.ID, "upstream_error", nil)
			}
			require.ErrorIs(t, loser, port.ErrCallbackInFlight, "an attempt still in flight is retried later, never acknowledged as done")

			close(release)
			require.NoError(t, <-first)
			assert.Len(t, temporal.signals, 1)
		})
	}
}

// A reservation nobody delivered expires on its own, so the retry signals.
func TestConnectorTaskService_StaleReservation_ExpiresAndRetrySignals(t *testing.T) {
	t.Parallel()

	task := connectorTask("storage", domain.TaskStatusReady, `{}`)
	temporal, signalRecovers := cbfixSignalFailsOnce()
	cache := &cbfixCache{fakeCacheStore: newFakeCacheStore(), delErr: errors.New("cache unreachable")}
	svc := cbfixService(task, temporal, cache, newFakeAssignmentRepo())
	key := cbfixKey("complete", task)

	require.Error(t, svc.Complete(context.Background(), task.TenantID, task.ID, map[string]any{}))
	assert.Equal(t, 30*time.Second, cache.ttls[key], "an undelivered reservation lives 30 seconds")

	signalRecovers()
	require.ErrorIs(t, svc.Complete(context.Background(), task.TenantID, task.ID, map[string]any{}), port.ErrCallbackInFlight,
		"until it expires the retry is asked to come back later")
	assert.Len(t, temporal.signals, 1)

	cache.expire(key)
	require.NoError(t, svc.Complete(context.Background(), task.TenantID, task.ID, map[string]any{}))
	assert.Len(t, temporal.signals, 2)
	assert.Equal(t, "delivered", cache.values[key])
	assert.Equal(t, 5*time.Minute, cache.ttls[key], "a delivered signal is remembered for 5 minutes")
}

// A repeat of the same callback after the signal went out is still absorbed.
func TestConnectorTaskService_Fail_RepeatAfterDeliveryIsAbsorbed(t *testing.T) {
	t.Parallel()

	task := connectorTask("send-email", domain.TaskStatusReady, `{}`)
	temporal := &fakeTemporalClient{}
	svc := cbfixService(task, temporal, newFakeCacheStore(), newFakeAssignmentRepo())

	require.NoError(t, svc.Fail(context.Background(), task.TenantID, task.ID, "upstream_error", nil))
	require.NoError(t, svc.Fail(context.Background(), task.TenantID, task.ID, "upstream_error", nil))
	assert.Len(t, temporal.signals, 1)
}

// With the cache down the reservation fails open.
func TestConnectorTaskService_CacheDown_CallbackProceeds(t *testing.T) {
	t.Parallel()

	task := connectorTask("storage", domain.TaskStatusReady, `{}`)
	temporal := &fakeTemporalClient{}
	cache := &cbfixCache{fakeCacheStore: newFakeCacheStore(), setNXErr: errors.New("cache unreachable")}
	svc := cbfixService(task, temporal, cache, newFakeAssignmentRepo())

	require.NoError(t, svc.Complete(context.Background(), task.TenantID, task.ID, map[string]any{}))
	assert.Len(t, temporal.signals, 1)
}

// cbfixLogger keeps the fields of every Error call.
type cbfixLogger struct{ errorFields []map[string]any }

func (l *cbfixLogger) Debug(string, map[string]any) {}
func (l *cbfixLogger) Info(string, map[string]any)  {}
func (l *cbfixLogger) Warn(string, map[string]any)  {}
func (l *cbfixLogger) Error(_ string, fields map[string]any) {
	l.errorFields = append(l.errorFields, fields)
}

// A stored mapping that cannot be read never becomes readable, so the task is
// failed (acknowledged with nil, so the worker acks the entry) and nothing raw
// reaches the workflow.
func TestConnectorTaskService_Complete_UnreadableStoredMappingFailsTheTask(t *testing.T) {
	t.Parallel()

	for name, extras := range map[string]string{
		"not json":              `{not json`,
		"mapping is not a list": `{"output_mapping":"oops"}`,
		"entry is not an item":  `{"output_mapping":[7]}`,
	} {
		t.Run(name, func(t *testing.T) {
			task := connectorTask("storage", domain.TaskStatusReady, extras)
			temporal := &fakeTemporalClient{}
			assignments := newFakeAssignmentRepo()
			svc := cbfixService(task, temporal, newFakeCacheStore(), assignments)
			log := &cbfixLogger{}
			svc.Log = log

			require.NoError(t, svc.Complete(context.Background(), task.TenantID, task.ID, map[string]any{"secret": "raw"}))

			require.Len(t, temporal.signals, 1)
			assert.Equal(t, "stage-fail", temporal.signals[0].SignalName)
			b, err := json.Marshal(temporal.signals[0].Payload)
			require.NoError(t, err)
			var wire struct {
				ErrorClass    string `json:"error_class"`
				ConnectorType string `json:"connector_type"`
				RecordVersion int64  `json:"record_version"`
			}
			require.NoError(t, json.Unmarshal(b, &wire))
			assert.Equal(t, "output_mapping_invalid", wire.ErrorClass)
			assert.Equal(t, "storage", wire.ConnectorType)
			assert.Equal(t, task.RecordVersion, wire.RecordVersion)
			assert.NotContains(t, string(b), "raw", "raw connector output must not be written into the workflow")

			active, listErr := assignments.ListActiveByTask(context.Background(), task.TenantID, task.ID)
			require.NoError(t, listErr)
			assert.Len(t, active, 1, "a failed task carries the acting principal, as on the fail callback")

			require.Len(t, log.errorFields, 1)
			assert.Equal(t, task.ID, log.errorFields[0]["task_id"])
			assert.Equal(t, task.TenantID, log.errorFields[0]["tenant_id"])
			assert.NotEmpty(t, log.errorFields[0]["error"])
		})
	}
}

// An absent output is an empty object on every path, never null.
func TestConnectorTaskService_Complete_AbsentOutputIsAnEmptyObject(t *testing.T) {
	t.Parallel()

	for name, extras := range map[string]string{
		"no extras":        ``,
		"empty extras":     `{}`,
		"empty mapping":    `{"output_mapping":[]}`,
		"mapping declared": `{"output_mapping":[{"source":"a","target":"b"}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			task := connectorTask("storage", domain.TaskStatusReady, extras)
			temporal := &fakeTemporalClient{}
			svc := cbfixService(task, temporal, newFakeCacheStore(), newFakeAssignmentRepo())

			require.NoError(t, svc.Complete(context.Background(), task.TenantID, task.ID, nil))
			require.Len(t, temporal.signals, 1)
			assert.JSONEq(t, `{}`, cbfixResultOf(t, temporal.signals[0]))
		})
	}
}

// Only a task still open (READY or IN_PROGRESS) is resolved by a callback.
func TestConnectorTaskService_Callback_ActedOnlyForAnOpenTask(t *testing.T) {
	t.Parallel()

	for status, wantSignals := range map[domain.TaskStatus]int{
		domain.TaskStatusReady:      1,
		domain.TaskStatusInProgress: 1,
		domain.TaskStatusSuperseded: 0,
		domain.TaskStatusDeferred:   0,
		domain.TaskStatusCompleted:  0,
		domain.TaskStatusFailed:     0,
	} {
		t.Run(string(status), func(t *testing.T) {
			for _, kind := range []string{"complete", "fail"} {
				task := connectorTask("storage", status, `{}`)
				temporal := &fakeTemporalClient{}
				assignments := newFakeAssignmentRepo()
				svc := cbfixService(task, temporal, newFakeCacheStore(), assignments)

				var err error
				if kind == "complete" {
					err = svc.Complete(context.Background(), task.TenantID, task.ID, map[string]any{})
				} else {
					err = svc.Fail(context.Background(), task.TenantID, task.ID, "upstream_error", nil)
				}
				require.NoError(t, err, kind)
				assert.Len(t, temporal.signals, wantSignals, kind)
				active, listErr := assignments.ListActiveByTask(context.Background(), task.TenantID, task.ID)
				require.NoError(t, listErr)
				assert.Len(t, active, wantSignals, "%s: an ignored callback writes no assignment", kind)
			}
		})
	}
}
