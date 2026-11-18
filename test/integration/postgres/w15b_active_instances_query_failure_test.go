//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestActiveInstances_CountQueryFails_Returns500WithTheGenericInternalProblem(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	guard := &service.ArchiveGuard{Instances: postgres.NewInstanceRepo(appPool)}
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.GET("/api/v1/internal/workflows/:workflow_id/active-instances", handler.New(handler.Services{ArchiveGuard: guard}).ActiveInstances)
	tenantID, workflowID := uuid.New(), uuid.New()
	target := "/api/v1/internal/workflows/" + workflowID.String() + "/active-instances?tenant_id=" + tenantID.String()
	get := func() *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, target, nil))
		return w
	}

	before := get()
	require.Equal(t, http.StatusOK, before.Code, before.Body.String())
	assert.JSONEq(t, `{"has_active":false,"count":0}`, before.Body.String(), "the query works until its table is gone")

	require.NoError(t, superPool.WithConn(context.Background(), func(ctx context.Context, conn *pgxpool.Conn) error {
		_, err := conn.Exec(ctx, "DROP TABLE workflow_instance CASCADE")
		return err
	}))
	w := get()

	require.Equal(t, http.StatusInternalServerError, w.Code, w.Body.String())
	assert.Equal(t, "application/problem+json", w.Header().Get("Content-Type"))
	var p struct {
		Type   string `json:"type"`
		Status int    `json:"status"`
		Detail string `json:"detail"`
		Code   string `json:"code"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &p), w.Body.String())
	assert.Equal(t, "INTERNAL_ERROR", p.Code)
	assert.Equal(t, "https://errors.bcbp.io/execution/internal-error", p.Type)
	assert.Equal(t, http.StatusInternalServerError, p.Status)
	assert.Equal(t, "an unexpected error occurred", p.Detail)
	for _, leak := range []string{"workflow_instance", "does not exist", "SQLSTATE"} {
		assert.NotContains(t, w.Body.String(), leak, "the database's own message is not echoed")
	}
}
