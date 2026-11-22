package http_test

import (
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	inboundhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http"
)

var w27aPoolFigures = inboundhttp.DBHealth{Healthy: true, Utilization: 0.25, AcquiredConns: 5, MaxConns: 20}

func TestReadyz_AllDependenciesHealthy_Answers200WithStatusOKAndThePoolFigures(t *testing.T) {
	rig := w27aNewRig(t, "test")
	rig.db.health = w27aPoolFigures

	w := rig.send(w27aRequest{method: http.MethodGet, path: "/readyz"})

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.True(t, strings.HasPrefix(w.Header().Get("Content-Type"), "application/json"), w.Header().Get("Content-Type"))
	assert.Equal(t, map[string]any{
		"status":         "OK",
		"db_utilization": 0.25,
		"db_conns":       float64(5),
		"db_max_conns":   float64(20),
	}, w27aBody(t, w), "no identity headers were sent, and the body names the database pool and nothing about the cache")
	assert.Equal(t, []string{"db", "temporal", "cache"}, rig.order.seen(), "database first, then Temporal, then the cache")
}

func TestReadyz_CacheDown_StaysReadyAndMarksTheCacheDegradedBesideThePoolFigures(t *testing.T) {
	rig := w27aNewRig(t, "test")
	rig.db.health = w27aPoolFigures
	rig.cachePg.err = errors.New("valkey down")

	w := rig.send(w27aRequest{method: http.MethodGet, path: "/readyz"})

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.Equal(t, map[string]any{
		"status":         "OK",
		"db_utilization": 0.25,
		"db_conns":       float64(5),
		"db_max_conns":   float64(20),
		"cache":          "degraded",
	}, w27aBody(t, w))
	assert.Equal(t, []string{"db", "temporal", "cache"}, rig.order.seen())
}

func TestReadyz_NotReady_Answers503AsPlainJSONAndStopsAtTheFirstFailure(t *testing.T) {
	cases := []struct {
		name      string
		arrange   func(rig *w27aRig)
		wantBody  map[string]any
		wantProbe []string
	}{
		{
			name:      "shutting down answers before any dependency is probed",
			arrange:   func(rig *w27aRig) { rig.setShuttingDown(true) },
			wantBody:  map[string]any{"status": "unavailable", "shutting_down": true},
			wantProbe: nil,
		},
		{
			name:      "database failing does not probe Temporal or the cache",
			arrange:   func(rig *w27aRig) { rig.db.health = inboundhttp.DBHealth{Healthy: false} },
			wantBody:  map[string]any{"status": "unavailable", "db": "unreachable"},
			wantProbe: []string{"db"},
		},
		{
			name:      "Temporal failing does not probe the cache",
			arrange:   func(rig *w27aRig) { rig.temporal.err = errors.New("temporal down") },
			wantBody:  map[string]any{"status": "unavailable", "temporal": "unreachable"},
			wantProbe: []string{"db", "temporal"},
		},
		{
			name: "database failing while the cache is also down still names only the database",
			arrange: func(rig *w27aRig) {
				rig.db.health = inboundhttp.DBHealth{Healthy: false}
				rig.cachePg.err = errors.New("valkey down")
			},
			wantBody:  map[string]any{"status": "unavailable", "db": "unreachable"},
			wantProbe: []string{"db"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w27aNewRig(t, "test")
			tc.arrange(rig)

			w := rig.send(w27aRequest{method: http.MethodGet, path: "/readyz"})

			require.Equal(t, http.StatusServiceUnavailable, w.Code, w.Body.String())
			contentType := w.Header().Get("Content-Type")
			assert.True(t, strings.HasPrefix(contentType, "application/json"), contentType)
			assert.NotContains(t, contentType, "problem")
			assert.Equal(t, tc.wantBody, w27aBody(t, w), "only status and the one reason key: no type, title, code, detail or instance")
			assert.Equal(t, tc.wantProbe, rig.order.seen())
		})
	}
}
