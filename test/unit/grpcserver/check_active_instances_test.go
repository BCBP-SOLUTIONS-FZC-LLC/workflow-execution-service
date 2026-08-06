package grpcserver_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	executionv1 "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/gen/proto/workflow/execution/v1"
	grpcadapter "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/grpc"
)

func TestServer_CheckActiveInstances(t *testing.T) {
	tenantID := uuid.New()
	workflowID := uuid.New()

	tests := []struct {
		name         string
		req          *executionv1.CheckActiveInstancesRequest
		guard        *fakeArchiveGuard
		wantCode     codes.Code
		wantActive   bool
		wantCount    int32
		wantLoggedOn bool
	}{
		{
			name:     "missing tenant_id",
			req:      &executionv1.CheckActiveInstancesRequest{WorkflowId: workflowID.String()},
			guard:    &fakeArchiveGuard{},
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "missing workflow_id",
			req:      &executionv1.CheckActiveInstancesRequest{TenantId: tenantID.String()},
			guard:    &fakeArchiveGuard{},
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "invalid tenant_id",
			req:      &executionv1.CheckActiveInstancesRequest{TenantId: "not-a-uuid", WorkflowId: workflowID.String()},
			guard:    &fakeArchiveGuard{},
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "invalid workflow_id",
			req:      &executionv1.CheckActiveInstancesRequest{TenantId: tenantID.String(), WorkflowId: "not-a-uuid"},
			guard:    &fakeArchiveGuard{},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "active instances",
			req:  &executionv1.CheckActiveInstancesRequest{TenantId: tenantID.String(), WorkflowId: workflowID.String()},
			guard: &fakeArchiveGuard{checkActiveInstances: func(context.Context, uuid.UUID, uuid.UUID) (bool, int32, error) {
				return true, 4, nil
			}},
			wantActive: true,
			wantCount:  4,
		},
		{
			name: "no active instances",
			req:  &executionv1.CheckActiveInstancesRequest{TenantId: tenantID.String(), WorkflowId: workflowID.String()},
			guard: &fakeArchiveGuard{checkActiveInstances: func(context.Context, uuid.UUID, uuid.UUID) (bool, int32, error) {
				return false, 0, nil
			}},
		},
		{
			name: "guard error",
			req:  &executionv1.CheckActiveInstancesRequest{TenantId: tenantID.String(), WorkflowId: workflowID.String()},
			guard: &fakeArchiveGuard{checkActiveInstances: func(context.Context, uuid.UUID, uuid.UUID) (bool, int32, error) {
				return false, 0, errors.New("db down")
			}},
			wantCode:     codes.Internal,
			wantLoggedOn: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var loggedErr bool
			log := &fakeLogger{errorFn: func(string, map[string]any) { loggedErr = true }}
			srv := grpcadapter.NewServer(log, tt.guard, &fakeUserTaskPauser{})

			resp, err := srv.CheckActiveInstances(context.Background(), tt.req)

			if tt.wantCode != codes.OK {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, status.Code(err))
				assert.Equal(t, tt.wantLoggedOn, loggedErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantActive, resp.GetHasActive())
			assert.Equal(t, tt.wantCount, resp.GetCount())
		})
	}
}
