// Package wflclock schedules delayed callbacks in the Temporal test environment.
// The environment does not define the order of callbacks due at the same
// instant, so After refuses a second callback at an instant already taken and
// Together delivers several in a fixed order from one callback.
package wflclock

import (
	"fmt"
	"sync"
	"time"

	"go.temporal.io/sdk/testsuite"
)

type clock struct {
	mu    sync.Mutex
	due   map[time.Duration]bool
	nowAt time.Duration
}

var (
	mu     sync.Mutex
	clocks = map[*testsuite.TestWorkflowEnvironment]*clock{}
)

func clockOf(env *testsuite.TestWorkflowEnvironment) *clock {
	mu.Lock()
	defer mu.Unlock()
	c, ok := clocks[env]
	if !ok {
		c = &clock{due: map[time.Duration]bool{}}
		clocks[env] = c
	}
	return c
}

func (c *clock) claim(delay time.Duration) (at time.Duration, taken bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	at = c.nowAt + delay
	taken = c.due[at]
	c.due[at] = true
	return at, taken
}

func (c *clock) run(at time.Duration, f func()) {
	c.mu.Lock()
	prev := c.nowAt
	c.nowAt = at
	c.mu.Unlock()
	f()
	c.mu.Lock()
	c.nowAt = prev
	c.mu.Unlock()
}

func After(env *testsuite.TestWorkflowEnvironment, f func(), delay time.Duration) {
	c := clockOf(env)
	at, taken := c.claim(delay)
	if taken {
		panic(fmt.Sprintf("wflclock: two callbacks are due %v after the start; "+
			"move one by a tick or deliver them with Together", at))
	}
	env.RegisterDelayedCallback(func() { c.run(at, f) }, delay)
}

func Together(env *testsuite.TestWorkflowEnvironment, delay time.Duration, fs ...func()) {
	After(env, func() {
		for _, f := range fs {
			f()
		}
	}, delay)
}

func Queued(env *testsuite.TestWorkflowEnvironment, f func()) {
	env.RegisterDelayedCallback(f, 0)
}
