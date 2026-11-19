package glue

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsglue "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

const goodVersionID = "8fa88cde-824c-47bc-836b-665cd42c2222"

type scriptedGetter struct {
	mu    sync.Mutex
	calls int
	steps []func(name string) (*awsglue.GetSchemaVersionOutput, error)
}

func (g *scriptedGetter) GetSchemaVersion(_ context.Context, in *awsglue.GetSchemaVersionInput, _ ...func(*awsglue.Options)) (*awsglue.GetSchemaVersionOutput, error) {
	g.mu.Lock()
	step := g.steps[min(g.calls, len(g.steps)-1)]
	g.calls++
	g.mu.Unlock()
	return step(*in.SchemaId.SchemaName)
}

func answer(versionID string) func(string) (*awsglue.GetSchemaVersionOutput, error) {
	return func(string) (*awsglue.GetSchemaVersionOutput, error) {
		return &awsglue.GetSchemaVersionOutput{SchemaVersionId: aws.String(versionID), Status: types.SchemaVersionStatusAvailable}, nil
	}
}

func fail(err error) func(string) (*awsglue.GetSchemaVersionOutput, error) {
	return func(string) (*awsglue.GetSchemaVersionOutput, error) { return nil, err }
}

func newTestCodec(getter schemaVersionGetter) *Codec {
	return &Codec{client: getter, registryName: "wf-workflow-events", cache: make(map[string]cacheEntry), cacheTTL: time.Minute}
}

type apiCodeError struct{ code string }

func (e apiCodeError) Error() string     { return e.code }
func (e apiCodeError) ErrorCode() string { return e.code }

func TestCodec_Encode_ATransientRegistryFailureIsMarkedRetryable(t *testing.T) {
	transient := map[string]error{
		"throttling":        apiCodeError{"ThrottlingException"},
		"operation timeout": &types.OperationTimeoutException{},
		"internal service":  &types.InternalServiceException{},
		"transport":         errors.New("dial tcp: connection refused"),
		"deadline":          context.DeadlineExceeded,
	}
	for name, cause := range transient {
		t.Run(name, func(t *testing.T) {
			c := newTestCodec(&scriptedGetter{steps: []func(string) (*awsglue.GetSchemaVersionOutput, error){fail(cause)}})

			_, _, err := c.Encode(context.Background(), "WorkflowInstanceStarted", []byte(`{}`))

			require.ErrorIs(t, err, events.ErrRetryable)
			assert.ErrorIs(t, err, cause)
		})
	}
}

func TestCodec_Encode_APermanentRegistryFailureIsNotMarkedRetryable(t *testing.T) {
	permanent := map[string]error{
		"schema not found": &types.EntityNotFoundException{},
		"access denied":    &types.AccessDeniedException{},
		"invalid input":    &types.InvalidInputException{},
	}
	for name, cause := range permanent {
		t.Run(name, func(t *testing.T) {
			c := newTestCodec(&scriptedGetter{steps: []func(string) (*awsglue.GetSchemaVersionOutput, error){fail(cause)}})

			_, _, err := c.Encode(context.Background(), "WorkflowInstanceStarted", []byte(`{}`))

			require.ErrorIs(t, err, cause)
			assert.NotErrorIs(t, err, events.ErrRetryable)
		})
	}
}

func TestCodec_Encode_AMalformedVersionIDIsNotRememberedAfterTheRegistryIsCorrected(t *testing.T) {
	getter := &scriptedGetter{steps: []func(string) (*awsglue.GetSchemaVersionOutput, error){answer("not-a-uuid"), answer(goodVersionID)}}
	c := newTestCodec(getter)

	_, _, err := c.Encode(context.Background(), "WorkflowInstanceStarted", []byte(`{}`))
	require.Error(t, err)
	assert.NotErrorIs(t, err, events.ErrRetryable)

	_, versionID, err := c.Encode(context.Background(), "WorkflowInstanceStarted", []byte(`{}`))

	require.NoError(t, err)
	assert.Equal(t, goodVersionID, versionID)
	assert.Equal(t, 2, getter.calls)
}

type blockingGetter struct {
	release map[string]chan struct{}
	calls   map[string]*atomic.Int32
}

func newBlockingGetter(names ...string) *blockingGetter {
	g := &blockingGetter{release: map[string]chan struct{}{}, calls: map[string]*atomic.Int32{}}
	for _, n := range names {
		g.release[n] = make(chan struct{})
		g.calls[n] = &atomic.Int32{}
	}
	return g
}

func (g *blockingGetter) GetSchemaVersion(ctx context.Context, in *awsglue.GetSchemaVersionInput, _ ...func(*awsglue.Options)) (*awsglue.GetSchemaVersionOutput, error) {
	name := *in.SchemaId.SchemaName
	g.calls[name].Add(1)
	select {
	case <-g.release[name]:
		return &awsglue.GetSchemaVersionOutput{SchemaVersionId: aws.String(goodVersionID), Status: types.SchemaVersionStatusAvailable}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestCodec_Encode_ASlowLookupForOneSchemaDoesNotDelayAnother(t *testing.T) {
	getter := newBlockingGetter("slow_event", "fast_event")
	c := newTestCodec(getter)
	slowCtx, cancelSlow := context.WithCancel(context.Background())
	slowDone := make(chan struct{})
	go func() {
		defer close(slowDone)
		_, _, _ = c.Encode(slowCtx, "SlowEvent", []byte(`{}`))
	}()
	require.Eventually(t, func() bool { return getter.calls["slow_event"].Load() == 1 }, 5*time.Second, time.Millisecond)

	close(getter.release["fast_event"])
	_, _, err := c.Encode(context.Background(), "FastEvent", []byte(`{}`))

	require.NoError(t, err)
	cancelSlow()
	<-slowDone
}

func TestCodec_Encode_AWaiterLeavesWhenItsContextIsCancelled(t *testing.T) {
	getter := newBlockingGetter("shared_event")
	c := newTestCodec(getter)
	leaderDone := make(chan error, 1)
	go func() {
		_, _, err := c.Encode(context.Background(), "SharedEvent", []byte(`{}`))
		leaderDone <- err
	}()
	require.Eventually(t, func() bool { return getter.calls["shared_event"].Load() == 1 }, 5*time.Second, time.Millisecond)

	waiterCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := c.Encode(waiterCtx, "SharedEvent", []byte(`{}`))

	require.ErrorIs(t, err, context.Canceled)
	assert.EqualValues(t, 1, getter.calls["shared_event"].Load(), "the waiter did not start a second lookup")
	close(getter.release["shared_event"])
	require.NoError(t, <-leaderDone)
}

func TestCodec_Encode_ConcurrentCallsForOneSchemaShareOneLookup(t *testing.T) {
	getter := newBlockingGetter("shared_event")
	c := newTestCodec(getter)
	var wg sync.WaitGroup
	errs := make(chan error, 5)
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := c.Encode(context.Background(), "SharedEvent", []byte(`{}`))
			errs <- err
		}()
	}
	require.Eventually(t, func() bool { return getter.calls["shared_event"].Load() >= 1 }, 5*time.Second, time.Millisecond)

	close(getter.release["shared_event"])
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
	assert.EqualValues(t, 1, getter.calls["shared_event"].Load())
}

type staleClock struct{ now time.Time }

func (s *staleClock) Now() time.Time { return s.now }

func TestCodec_Encode_ARegistryOutageServesTheLastKnownIdWithinTheStalenessBound(t *testing.T) {
	obstest.Init(t)
	getter := &scriptedGetter{steps: []func(string) (*awsglue.GetSchemaVersionOutput, error){answer(goodVersionID), fail(errors.New("glue down"))}}
	clock := &staleClock{now: time.Unix(1_000_000, 0)}
	c := newTestCodec(getter)
	c.now = clock.Now
	c.MaxStaleness = time.Hour
	before := testutil.ToFloat64(observability.GlueSchemaStaleServedTotal)

	_, _, err := c.Encode(context.Background(), "WorkflowInstanceStarted", []byte(`{}`))
	require.NoError(t, err)
	clock.now = clock.now.Add(time.Minute + 30*time.Minute)
	_, versionID, err := c.Encode(context.Background(), "WorkflowInstanceStarted", []byte(`{}`))

	require.NoError(t, err)
	assert.Equal(t, goodVersionID, versionID)
	assert.Equal(t, before+1, testutil.ToFloat64(observability.GlueSchemaStaleServedTotal))
}

func TestCodec_Encode_ARegistryOutagePastTheStalenessBoundFails(t *testing.T) {
	getter := &scriptedGetter{steps: []func(string) (*awsglue.GetSchemaVersionOutput, error){answer(goodVersionID), fail(errors.New("glue down"))}}
	clock := &staleClock{now: time.Unix(1_000_000, 0)}
	c := newTestCodec(getter)
	c.now = clock.Now
	c.MaxStaleness = time.Hour

	_, _, err := c.Encode(context.Background(), "WorkflowInstanceStarted", []byte(`{}`))
	require.NoError(t, err)
	clock.now = clock.now.Add(time.Minute + time.Hour + time.Second)
	_, _, err = c.Encode(context.Background(), "WorkflowInstanceStarted", []byte(`{}`))

	require.ErrorIs(t, err, events.ErrRetryable)
}

func TestCodec_Encode_WithoutAStalenessBoundAnOutageFailsOnceTheCachedIdExpires(t *testing.T) {
	getter := &scriptedGetter{steps: []func(string) (*awsglue.GetSchemaVersionOutput, error){answer(goodVersionID), fail(errors.New("glue down"))}}
	clock := &staleClock{now: time.Unix(1_000_000, 0)}
	c := newTestCodec(getter)
	c.now = clock.Now

	_, _, err := c.Encode(context.Background(), "WorkflowInstanceStarted", []byte(`{}`))
	require.NoError(t, err)
	clock.now = clock.now.Add(2 * time.Minute)
	_, _, err = c.Encode(context.Background(), "WorkflowInstanceStarted", []byte(`{}`))

	require.Error(t, err)
}

func TestCodec_Encode_ARefusedRefreshIsNotRetriedOnEveryCallWhileTheStaleIdIsServed(t *testing.T) {
	getter := &scriptedGetter{steps: []func(string) (*awsglue.GetSchemaVersionOutput, error){answer(goodVersionID), fail(errors.New("glue down"))}}
	clock := &staleClock{now: time.Unix(1_000_000, 0)}
	c := newTestCodec(getter)
	c.now = clock.Now
	c.MaxStaleness = time.Hour

	_, _, err := c.Encode(context.Background(), "WorkflowInstanceStarted", []byte(`{}`))
	require.NoError(t, err)
	clock.now = clock.now.Add(2 * time.Minute)
	for range 3 {
		_, _, err = c.Encode(context.Background(), "WorkflowInstanceStarted", []byte(`{}`))
		require.NoError(t, err)
	}

	assert.Equal(t, 2, getter.calls, "one refresh attempt, then the stale id is served until the retry time")
}

func primedExpiredCodec(t *testing.T, steps ...func(string) (*awsglue.GetSchemaVersionOutput, error)) (*Codec, *scriptedGetter) {
	t.Helper()
	getter := &scriptedGetter{steps: append([]func(string) (*awsglue.GetSchemaVersionOutput, error){answer(goodVersionID)}, steps...)}
	clock := &staleClock{now: time.Unix(1_000_000, 0)}
	c := newTestCodec(getter)
	c.now = clock.Now
	c.MaxStaleness = time.Hour
	_, _, err := c.Encode(context.Background(), "WorkflowInstanceStarted", []byte(`{}`))
	require.NoError(t, err)
	clock.now = clock.now.Add(2 * time.Minute)
	return c, getter
}

func TestCodec_Encode_ALeaderWhoseOwnContextEndedDoesNotServeTheStaleId(t *testing.T) {
	obstest.Init(t)
	c, getter := primedExpiredCodec(t, func(string) (*awsglue.GetSchemaVersionOutput, error) {
		return nil, context.Canceled
	})
	before := testutil.ToFloat64(observability.GlueSchemaStaleServedTotal)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := c.Encode(ctx, "WorkflowInstanceStarted", []byte(`{}`))

	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, before, testutil.ToFloat64(observability.GlueSchemaStaleServedTotal))
	_, versionID, err := c.Encode(context.Background(), "WorkflowInstanceStarted", []byte(`{}`))
	require.NoError(t, err)
	assert.Equal(t, goodVersionID, versionID)
	assert.Equal(t, 3, getter.calls, "no retry time was set by the cancelled attempt, so the next call asks the registry again")
}

func TestCodec_Encode_APermanentRegistryFailureIsNeverServedStale(t *testing.T) {
	for name, cause := range map[string]error{
		"access denied":    &types.AccessDeniedException{},
		"schema not found": &types.EntityNotFoundException{},
		"invalid input":    &types.InvalidInputException{},
	} {
		t.Run(name, func(t *testing.T) {
			c, _ := primedExpiredCodec(t, fail(cause))

			_, _, err := c.Encode(context.Background(), "WorkflowInstanceStarted", []byte(`{}`))

			require.ErrorIs(t, err, cause)
			assert.NotErrorIs(t, err, events.ErrRetryable)
		})
	}
}

func TestCodec_Encode_EveryEventEncodedWithAStaleIdIsCounted(t *testing.T) {
	obstest.Init(t)
	c, _ := primedExpiredCodec(t, fail(errors.New("glue down")))
	before := testutil.ToFloat64(observability.GlueSchemaStaleServedTotal)

	for range 3 {
		_, _, err := c.Encode(context.Background(), "WorkflowInstanceStarted", []byte(`{}`))
		require.NoError(t, err)
	}

	assert.Equal(t, before+3, testutil.ToFloat64(observability.GlueSchemaStaleServedTotal))
}

func TestAwaitFlight_AWaiterRetriesWhenTheLeaderEndedOnItsOwnContext(t *testing.T) {
	for name, cause := range map[string]error{"cancelled": context.Canceled, "timed out": context.DeadlineExceeded} {
		t.Run(name, func(t *testing.T) {
			flight := &schemaFlight{done: make(chan struct{}), err: markTransient(cause), leaderContextEnded: true}
			close(flight.done)

			_, retry, err := awaitFlight(context.Background(), flight)

			assert.True(t, retry)
			assert.NoError(t, err)
		})
	}
}

func TestAwaitFlight_AWaiterSharesARegistryFailureThatIsNotTheLeadersContext(t *testing.T) {
	flight := &schemaFlight{done: make(chan struct{}), err: markTransient(errors.New("glue down"))}
	close(flight.done)

	_, retry, err := awaitFlight(context.Background(), flight)

	assert.False(t, retry)
	assert.ErrorIs(t, err, events.ErrRetryable)
}
