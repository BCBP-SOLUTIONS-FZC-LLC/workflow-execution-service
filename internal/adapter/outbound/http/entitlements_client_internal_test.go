package http

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEntitlementsClient_UsesItsOwnTransportWith32IdleConnectionsPerHost(t *testing.T) {
	transport, ok := NewEntitlementsClient("http://entitlements", time.Second).client.Transport.(*http.Transport)

	require.True(t, ok, "the client carries its own *http.Transport")
	assert.Equal(t, 32, transport.MaxIdleConnsPerHost)
	assert.NotSame(t, http.DefaultTransport, transport, "a clone, so the process default is untouched")
	assert.Zero(t, http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost)
}
