package main

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func cfgfixHostname(name string, err error) func() (string, error) {
	return func() (string, error) { return name, err }
}

func TestResolveConsumerName_HostNameUnreadableGivesEachPodItsOwnName(t *testing.T) {
	cases := map[string]func() (string, error){
		"host name lookup fails": cfgfixHostname("", errors.New("no host")),
		"host name is empty":     cfgfixHostname("", nil),
	}
	for name, hostname := range cases {
		t.Run(name, func(t *testing.T) {
			first := resolveConsumerName("", hostname)
			second := resolveConsumerName("", hostname)

			assert.True(t, strings.HasPrefix(first, "connector-worker-"), "got %q, want the fixed name plus a suffix", first)
			assert.Greater(t, len(first), len("connector-worker-"), "the suffix must not be empty")
			assert.NotEqual(t, first, second, "two pods without a host name must not share a consumer name")
		})
	}
}

func TestResolveConsumerName_ConfiguredNameAndHostNameAreUnchanged(t *testing.T) {
	assert.Equal(t, "replica-a", resolveConsumerName("replica-a", cfgfixHostname("host-1", nil)))
	assert.Equal(t, "host-1", resolveConsumerName("", cfgfixHostname("host-1", nil)))
}
