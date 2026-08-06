package grpc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestBackoff(t *testing.T) {
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 50 * time.Millisecond},
		{2, 100 * time.Millisecond},
		{3, 200 * time.Millisecond},
		{4, 400 * time.Millisecond},
		{5, 500 * time.Millisecond},  // would be 800ms uncapped
		{10, 500 * time.Millisecond}, // stays capped for any further growth
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, backoff(tt.attempt))
	}
}

func TestIsRetryableGRPC(t *testing.T) {
	tests := []struct {
		code codes.Code
		want bool
	}{
		{codes.Unavailable, true},
		{codes.DeadlineExceeded, true},
		{codes.PermissionDenied, false},
		{codes.InvalidArgument, false},
		{codes.Internal, false},
		{codes.NotFound, false},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, isRetryableGRPC(status.Error(tt.code, "x")), "code=%s", tt.code)
	}
}

func TestIsRetryableGRPC_NonStatusError(t *testing.T) {
	assert.False(t, isRetryableGRPC(assert.AnError))
}
