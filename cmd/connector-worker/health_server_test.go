package main

import (
	"bufio"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

func cwcliGet(h http.Handler, path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

// cwcliRespServer answers PING with PONG over the Redis protocol and records the commands it was sent.
type cwcliRespServer struct {
	ln       net.Listener
	mu       sync.Mutex
	conns    []net.Conn
	commands []string
	wg       sync.WaitGroup
}

func newCwcliRespServer(t *testing.T) *cwcliRespServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	s := &cwcliRespServer{ln: ln}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			s.mu.Lock()
			s.conns = append(s.conns, conn)
			s.mu.Unlock()
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				s.serve(conn)
			}()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		s.mu.Lock()
		for _, c := range s.conns {
			_ = c.Close()
		}
		s.mu.Unlock()
		s.wg.Wait()
	})
	return s
}

func (s *cwcliRespServer) serve(conn net.Conn) {
	r := bufio.NewReader(conn)
	for {
		header, err := r.ReadString('\n')
		if err != nil || !strings.HasPrefix(header, "*") {
			return
		}
		n, err := strconv.Atoi(strings.TrimSpace(header[1:]))
		if err != nil {
			return
		}
		args := make([]string, 0, n)
		for range n {
			sizeLine, err := r.ReadString('\n')
			if err != nil {
				return
			}
			size, err := strconv.Atoi(strings.TrimSpace(sizeLine[1:]))
			if err != nil {
				return
			}
			buf := make([]byte, size+2)
			if _, err := io.ReadFull(r, buf); err != nil {
				return
			}
			args = append(args, string(buf[:size]))
		}
		s.mu.Lock()
		s.commands = append(s.commands, strings.ToLower(args[0]))
		s.mu.Unlock()
		reply := "-ERR unsupported\r\n"
		switch strings.ToLower(args[0]) {
		case "ping":
			reply = "+PONG\r\n"
		case "client":
			reply = "+OK\r\n"
		}
		if _, err := conn.Write([]byte(reply)); err != nil {
			return
		}
	}
}

func (s *cwcliRespServer) seen() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.commands...)
}

func TestHealthServer_HealthzIsOKWithAnEmptyBodyWhileValkeyIsDown(t *testing.T) {
	obstest.Init(t)
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", Protocol: 2, DialTimeout: 100 * time.Millisecond, MaxRetries: -1})
	t.Cleanup(func() { _ = client.Close() })
	h := newHealthServer(":0", &deps{valkeyClient: client}).Handler

	healthz := cwcliGet(h, "/healthz")
	readyz := cwcliGet(h, "/readyz")
	metrics := cwcliGet(h, "/metrics")

	assert.Equal(t, http.StatusServiceUnavailable, readyz.Code, "Valkey really is down")
	assert.Equal(t, http.StatusOK, healthz.Code)
	assert.Empty(t, healthz.Body.String())
	assert.Equal(t, http.StatusOK, metrics.Code, "the metrics are served on the same server")
	assert.NotEmpty(t, metrics.Body.String())
}

// Readiness asks Valkey and nothing else: a worker with no Postgres pool, vault reader, completion client or stream
// consumer is still ready when Valkey answers PING.
func TestHealthServer_ReadyzIsOKWithAnEmptyBodyWhenValkeyAnswersPing(t *testing.T) {
	valkey := newCwcliRespServer(t)
	client := redis.NewClient(&redis.Options{Addr: valkey.ln.Addr().String(), Protocol: 2, MaxRetries: -1})
	t.Cleanup(func() { _ = client.Close() })
	h := newHealthServer(":0", &deps{valkeyClient: client}).Handler

	readyz := cwcliGet(h, "/readyz")

	assert.Equal(t, http.StatusOK, readyz.Code)
	assert.Empty(t, readyz.Body.String())
	assert.Contains(t, valkey.seen(), "ping")
}
