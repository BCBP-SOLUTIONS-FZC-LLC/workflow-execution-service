package http_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	inboundhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
)

func TestNewRouter_AsyncAPIRoute_IsMountedOnlyForTheExactNamesDevAndLocal(t *testing.T) {
	cases := []struct {
		name    string
		appEnv  string
		mounted bool
	}{
		{name: "dev", appEnv: "dev", mounted: true},
		{name: "local", appEnv: "local", mounted: true},
		{name: "test", appEnv: "test", mounted: false},
		{name: "staging", appEnv: "staging", mounted: false},
		{name: "prod", appEnv: "prod", mounted: false},
		{name: "production", appEnv: "production", mounted: false},
		{name: "empty", appEnv: "", mounted: false},
		{name: "upper case DEV", appEnv: "DEV", mounted: false},
		{name: "mixed case Local", appEnv: "Local", mounted: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w27aNewRig(t, tc.appEnv)

			w := rig.send(w27aRequest{method: http.MethodGet, path: "/asyncapi"})

			if tc.mounted {
				require.Equal(t, http.StatusOK, w.Code, "the page is served: "+w.Body.String())
				assert.True(t, strings.HasPrefix(w.Header().Get("Content-Type"), "text/html"))
				assert.True(t, strings.HasPrefix(w.Body.String(), "<!DOCTYPE html>"))
				return
			}
			require.Equal(t, http.StatusNotFound, w.Code, w.Body.String())
			w27aAssertNotFoundProblem(t, w, w27aBody(t, w), "/asyncapi")
		})
	}
}

func TestNewRouter_AppEnv_SwitchesGinToReleaseModeExceptForDevAndLocal(t *testing.T) {
	cases := []struct {
		name     string
		appEnv   string
		wantMode string
	}{
		{name: "dev leaves the mode alone", appEnv: "dev", wantMode: gin.TestMode},
		{name: "local leaves the mode alone", appEnv: "local", wantMode: gin.TestMode},
		{name: "test goes to release", appEnv: "test", wantMode: gin.ReleaseMode},
		{name: "staging goes to release", appEnv: "staging", wantMode: gin.ReleaseMode},
		{name: "prod goes to release", appEnv: "prod", wantMode: gin.ReleaseMode},
		{name: "an upper case DEV goes to release", appEnv: "DEV", wantMode: gin.ReleaseMode},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			previous := gin.Mode()
			t.Cleanup(func() { gin.SetMode(previous) })
			gin.SetMode(gin.TestMode)

			inboundhttp.NewRouter(inboundhttp.RouterConfig{
				GinConfig: gincommon.Config{ServiceName: "workflow-execution-service", BuildVersion: "test", Domain: "workflow", Environment: "test"},
				AppEnv:    tc.appEnv,
				Handler:   handler.New(handler.Services{}),
			})

			assert.Equal(t, tc.wantMode, gin.Mode())
		})
	}
}
