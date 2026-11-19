package handler_test

import (
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListInstances_NonAdminWithNoMatchingDepartmentOrAssignment_Returns200WithAnEmptyItemsArray(t *testing.T) {
	world := w18aStartWorld(t)
	otherDepartment := uuid.New()
	r := req(http.MethodGet, instancesPath(), nil)
	r.Header.Set("x-departments", otherDepartment.String()+":member")

	rec := do(world.router, r)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.JSONEq(t, `{"items":[]}`, rec.Body.String(), "an empty list is an empty array, not null")
	require.Len(t, world.instances.lists, 1)
	got := world.instances.lists[0]
	assert.Equal(t, testTenantID, got.tenantID)
	require.NotNil(t, got.filter.Scope, "a non-admin reads inside a scope")
	assert.Equal(t, testUserID, got.filter.Scope.CallerUserID)
	assert.Equal(t, []uuid.UUID{otherDepartment}, got.filter.Scope.DepartmentIDs)
}

func TestListInstances_DatabaseQueryFails_Returns500WithTheGenericProblem(t *testing.T) {
	world := w18aStartWorld(t)
	world.instances.listErr = errW18aDatabase

	rec := do(world.router, asAdmin(req(http.MethodGet, instancesPath(), nil)))

	require.Equal(t, http.StatusInternalServerError, rec.Code, rec.Body.String())
	var body struct {
		Type   string `json:"type"`
		Detail string `json:"detail"`
		Code   string `json:"code"`
		Status int    `json:"status"`
	}
	decodeJSON(t, rec.Body, &body)
	assert.Equal(t, "INTERNAL_ERROR", body.Code)
	assert.Equal(t, http.StatusInternalServerError, body.Status)
	assert.Equal(t, "an unexpected error occurred", body.Detail)
	assert.Regexp(t, `/internal-error$`, body.Type)
	assert.NotContains(t, rec.Body.String(), "hunter2", "the database error text is not echoed")
	require.Len(t, world.instances.lists, 1)
	assert.Equal(t, testTenantID, world.instances.lists[0].tenantID)
	assert.Nil(t, world.instances.lists[0].filter.Scope, "an admin reads without a scope")
}
