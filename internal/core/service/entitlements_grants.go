package service

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"runtime/debug"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

var _ port.FeatureGrants = (*EntitlementsGrants)(nil)

const (
	entitlementsRetryBackoff       = 5 * time.Second
	entitlementsCacheCap           = 10_000
	entitlementsMaxConcurrentReads = 32
	// entitlementsJitter spreads the TTL and the backoff by up to this fraction either way, so tenants read at the same moment do not expire together.
	entitlementsJitter = 0.2
)

type EntitlementsGrantsDeps struct {
	Reader         port.EntitlementsReader
	TTL            time.Duration
	FreshnessBound time.Duration
	Now            func() time.Time
	Log            port.Logger
	// Rand returns a number in [0, 1) for the jitter; nil uses math/rand. It is only called under the cache's lock.
	Rand func() float64
}

type grantsEntry struct {
	has       bool
	flags     map[string]struct{}
	fetchedAt time.Time
	ttl       time.Duration
	retryAt   time.Time
	lastErr   error
}

type grantsRead struct {
	done  chan struct{}
	flags map[string]struct{}
	err   error
	// held is the set the entry still holds within the freshness bound when the read failed; a caller that waited
	// for the read falls back to it. heldAge is that set's age at the failure.
	held    map[string]struct{}
	heldAge time.Duration
}

// EntitlementsGrants caches each tenant's whole feature-flag set (execution LLD §3.2). A set older than its jittered
// TTL but younger than twice that TTL (and within the freshness bound) is served as it is while one background read
// renews it. An older set makes the caller wait for the read; only if that read fails is the set still within the
// freshness bound served, and then it counts as a stale serve.
type EntitlementsGrants struct {
	reader port.EntitlementsReader
	ttl    time.Duration
	bound  time.Duration
	now    func() time.Time
	rand   func() float64
	log    port.Logger
	slots  chan struct{}

	mu       sync.Mutex
	entries  map[uuid.UUID]*grantsEntry
	inflight map[uuid.UUID]*grantsRead
	// servedPeak is the oldest age served stale in the current window of one TTL; the age gauge shows it.
	servedPeak  time.Duration
	servedUntil time.Time
}

func NewEntitlementsGrants(d EntitlementsGrantsDeps) (*EntitlementsGrants, error) {
	switch {
	case d.Reader == nil:
		return nil, errors.New("entitlements grants: a reader is required")
	case d.TTL <= 0:
		return nil, fmt.Errorf("entitlements grants: the TTL must be positive, got %s", d.TTL)
	case d.FreshnessBound < d.TTL:
		return nil, fmt.Errorf("entitlements grants: the freshness bound %s is below the TTL %s", d.FreshnessBound, d.TTL)
	}
	now := d.Now
	if now == nil {
		now = time.Now
	}
	var log port.Logger = noopLogger{}
	if d.Log != nil {
		log = d.Log
	}
	random := d.Rand
	if random == nil {
		random = rand.Float64
	}
	return &EntitlementsGrants{
		reader:   d.Reader,
		ttl:      d.TTL,
		bound:    d.FreshnessBound,
		now:      now,
		rand:     random,
		log:      log,
		slots:    make(chan struct{}, entitlementsMaxConcurrentReads),
		entries:  map[uuid.UUID]*grantsEntry{},
		inflight: map[uuid.UUID]*grantsRead{},
	}, nil
}

func (g *EntitlementsGrants) HasFeature(ctx context.Context, tenantID uuid.UUID, feature string) (bool, error) {
	flags, err := g.flagsOf(ctx, tenantID)
	if err != nil {
		return false, err
	}
	_, ok := flags[feature]
	return ok, nil
}

// flagsOf decides under the lock and acts after it: nothing is logged and no error is built while the lock is held.
func (g *EntitlementsGrants) flagsOf(ctx context.Context, tenantID uuid.UUID) (map[string]struct{}, error) {
	g.mu.Lock()
	now := g.now()
	e := g.entries[tenantID]
	switch {
	case e != nil && e.has && now.Sub(e.fetchedAt) < e.ttl:
		flags := e.flags
		gauge, setGauge := g.noteServedLocked(now, 0)
		g.mu.Unlock()
		g.publishAge(gauge, setGauge)
		return flags, nil
	case e != nil && e.has && now.Sub(e.fetchedAt) < g.staleWindow(e):
		g.refreshInBackgroundLocked(ctx, tenantID, e, now)
		return g.serveStale(e.flags, now.Sub(e.fetchedAt), now), nil
	case e != nil && now.Before(e.retryAt):
		// A read failed a moment ago: no new read, so the set held within the bound is all there is to serve.
		if e.has && now.Sub(e.fetchedAt) < g.bound {
			return g.serveStale(e.flags, now.Sub(e.fetchedAt), now), nil
		}
		cause := e.lastErr
		g.mu.Unlock()
		return nil, g.unavailable(tenantID, cause)
	}
	read := g.startReadLocked(ctx, tenantID, false)
	g.mu.Unlock()
	return g.await(ctx, tenantID, read)
}

// staleWindow is the age below which a set past its TTL is served while a background read renews it: twice the
// entry's TTL, never beyond the freshness bound.
func (g *EntitlementsGrants) staleWindow(e *grantsEntry) time.Duration {
	return min(2*e.ttl, g.bound)
}

// serveStale is called with the lock held and releases it; it counts the serve and publishes the age gauge.
func (g *EntitlementsGrants) serveStale(flags map[string]struct{}, age time.Duration, now time.Time) map[string]struct{} {
	gauge, setGauge := g.noteServedLocked(now, age)
	g.mu.Unlock()
	observability.IncCounter(observability.EntitlementsStaleServedTotal)
	g.publishAge(gauge, setGauge)
	return flags
}

// refreshInBackgroundLocked starts at most one read for the tenant; with no free slot it does nothing and a later call retries.
func (g *EntitlementsGrants) refreshInBackgroundLocked(ctx context.Context, tenantID uuid.UUID, e *grantsEntry, now time.Time) {
	if g.inflight[tenantID] != nil || now.Before(e.retryAt) {
		return
	}
	select {
	case g.slots <- struct{}{}:
		g.startReadLocked(ctx, tenantID, true)
	default:
	}
}

// startReadLocked joins the tenant's running read or starts one. The read outlives the caller that started it,
// so one caller giving up does not fail the others; a blocking read queues for its slot inside the read.
func (g *EntitlementsGrants) startReadLocked(ctx context.Context, tenantID uuid.UUID, slotHeld bool) *grantsRead {
	if read := g.inflight[tenantID]; read != nil {
		return read
	}
	read := &grantsRead{done: make(chan struct{})}
	g.inflight[tenantID] = read
	go g.runRead(context.WithoutCancel(ctx), tenantID, read, slotHeld)
	return read
}

func (g *EntitlementsGrants) runRead(ctx context.Context, tenantID uuid.UUID, read *grantsRead, slotHeld bool) {
	if !slotHeld {
		g.slots <- struct{}{}
	}
	defer func() { <-g.slots }()
	flags, err := g.callReader(ctx, tenantID)
	g.settleRead(tenantID, read, flags, err)
}

// callReader turns a reader panic into a failed read, logged at Error with its stack.
func (g *EntitlementsGrants) callReader(ctx context.Context, tenantID uuid.UUID) (flags []string, err error) {
	defer func() {
		if r := recover(); r != nil {
			flags = nil
			err = fmt.Errorf("%w: reading entitlements panicked: %v", port.ErrEntitlementsUnavailable, r)
			observability.IncCounter(observability.EntitlementsReadPanicsTotal)
			g.log.Error("entitlements reader panicked", map[string]any{
				"tenant_id": tenantID.String(),
				"panic":     fmt.Sprint(r),
				"stack":     string(debug.Stack()),
			})
		}
	}()
	return g.reader.FeatureFlags(ctx, tenantID)
}

func (g *EntitlementsGrants) settleRead(tenantID uuid.UUID, read *grantsRead, flags []string, err error) {
	g.mu.Lock()
	delete(g.inflight, tenantID)
	now := g.now()
	e := g.entries[tenantID]
	if e == nil {
		e = g.newEntryLocked(tenantID, now)
	}
	var heldAge time.Duration
	held := false
	if err == nil {
		read.flags = make(map[string]struct{}, len(flags))
		for _, f := range flags {
			read.flags[f] = struct{}{}
		}
		e.has, e.flags, e.fetchedAt = true, read.flags, now
		e.ttl = min(g.jittered(g.ttl), g.bound)
		e.retryAt, e.lastErr = time.Time{}, nil
	} else {
		read.err = err
		e.retryAt, e.lastErr = now.Add(g.jittered(entitlementsRetryBackoff)), err
		heldAge = now.Sub(e.fetchedAt)
		held = e.has && heldAge < g.bound
		if held {
			read.held, read.heldAge = e.flags, heldAge
		}
	}
	close(read.done)
	g.mu.Unlock()

	if err == nil {
		return
	}
	fields := map[string]any{"tenant_id": tenantID.String(), "error": err.Error()}
	if held {
		fields["held_age_seconds"] = heldAge.Seconds()
		g.log.Warn("entitlements refresh failed, serving the held feature flags", fields)
		return
	}
	g.log.Error("entitlements read failed and no feature flags are held within the freshness bound", fields)
}

func (g *EntitlementsGrants) await(ctx context.Context, tenantID uuid.UUID, read *grantsRead) (map[string]struct{}, error) {
	select {
	case <-read.done:
	case <-ctx.Done():
		return nil, fmt.Errorf("%w: waiting for the entitlements read: %w", port.ErrEntitlementsUnavailable, ctx.Err())
	}
	if read.err != nil {
		if read.held != nil {
			g.mu.Lock()
			return g.serveStale(read.held, read.heldAge, g.now()), nil
		}
		return nil, g.unavailable(tenantID, read.err)
	}
	return read.flags, nil
}

func (g *EntitlementsGrants) unavailable(tenantID uuid.UUID, cause error) error {
	if cause == nil {
		cause = errors.New("no read has succeeded")
	}
	return fmt.Errorf("%w: the feature flags of tenant %s have not been read within %s: %w",
		port.ErrEntitlementsUnavailable, tenantID, g.bound, cause)
}

func (g *EntitlementsGrants) jittered(d time.Duration) time.Duration {
	return time.Duration(float64(d) * (1 - entitlementsJitter + 2*entitlementsJitter*g.rand()))
}

// noteServedLocked tracks the oldest age served stale in the current TTL window and says whether the gauge changes.
// Every call counts, so the gauge falls back to 0 once a window passes with no stale serve.
func (g *EntitlementsGrants) noteServedLocked(now time.Time, staleAge time.Duration) (gauge float64, changed bool) {
	if !now.Before(g.servedUntil) {
		changed = g.servedPeak != 0
		g.servedPeak, g.servedUntil = 0, now.Add(g.ttl)
	}
	if staleAge > g.servedPeak {
		g.servedPeak, changed = staleAge, true
	}
	return g.servedPeak.Seconds(), changed
}

func (g *EntitlementsGrants) publishAge(gauge float64, changed bool) {
	if changed {
		observability.SetGauge(observability.EntitlementsOldestServedAgeSeconds, gauge)
	}
}

func (g *EntitlementsGrants) newEntryLocked(tenantID uuid.UUID, now time.Time) *grantsEntry {
	if len(g.entries) >= entitlementsCacheCap {
		g.makeRoomLocked(now)
	}
	e := &grantsEntry{}
	g.entries[tenantID] = e
	return e
}

func (g *EntitlementsGrants) makeRoomLocked(now time.Time) {
	for id, e := range g.entries {
		if g.expiredLocked(e, now) {
			delete(g.entries, id)
		}
	}
	if len(g.entries) < entitlementsCacheCap {
		return
	}
	var oldestID uuid.UUID
	var oldest time.Time
	first := true
	for id, e := range g.entries {
		if first || e.fetchedAt.Before(oldest) {
			oldestID, oldest, first = id, e.fetchedAt, false
		}
	}
	delete(g.entries, oldestID)
}

func (g *EntitlementsGrants) expiredLocked(e *grantsEntry, now time.Time) bool {
	if e.has {
		return now.Sub(e.fetchedAt) >= g.bound
	}
	return !now.Before(e.retryAt)
}
