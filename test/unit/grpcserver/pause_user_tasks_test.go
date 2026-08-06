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

func TestServer_PauseUserTasks(t *testing.T) {
	tenantID := uuid.New()
	userID := uuid.New()

	tests := []struct {
		name         string
		req          *executionv1.PauseUserTasksRequest
		pauser       *fakeUserTaskPauser
		wantCode     codes.Code
		wantLoggedOn bool
	}{
		{
			name:     "missing tenant_id",
			req:      &executionv1.PauseUserTasksRequest{UserId: userID.String()},
			pauser:   &fakeUserTaskPauser{},
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "missing user_id",
			req:      &executionv1.PauseUserTasksRequest{TenantId: tenantID.String()},
			pauser:   &fakeUserTaskPauser{},
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "invalid tenant_id",
			req:      &executionv1.PauseUserTasksRequest{TenantId: "not-a-uuid", UserId: userID.String()},
			pauser:   &fakeUserTaskPauser{},
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "invalid user_id",
			req:      &executionv1.PauseUserTasksRequest{TenantId: tenantID.String(), UserId: "not-a-uuid"},
			pauser:   &fakeUserTaskPauser{},
			wantCode: codes.InvalidArgument,
		},
		{
			name:   "success",
			req:    &executionv1.PauseUserTasksRequest{TenantId: tenantID.String(), UserId: userID.String()},
			pauser: &fakeUserTaskPauser{},
		},
		{
			name: "pauser error",
			req:  &executionv1.PauseUserTasksRequest{TenantId: tenantID.String(), UserId: userID.String()},
			pauser: &fakeUserTaskPauser{pauseUserTasks: func(context.Context, uuid.UUID, uuid.UUID) error {
				return errors.New("db down")
			}},
			wantCode:     codes.Internal,
			wantLoggedOn: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var loggedErr bool
			log := &fakeLogger{errorFn: func(string, map[string]any) { loggedErr = true }}
			srv := grpcadapter.NewServer(log, &fakeArchiveGuard{}, tt.pauser)

			resp, err := srv.PauseUserTasks(context.Background(), tt.req)

			if tt.wantCode != codes.OK {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, status.Code(err))
				assert.Equal(t, tt.wantLoggedOn, loggedErr)
				return
			}
			require.NoError(t, err)
			assert.NotNil(t, resp)
		})
	}
}
