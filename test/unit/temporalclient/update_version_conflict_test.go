package temporalclient_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporalclient"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestUpdateWorkflow_AStaleRecordVersionIsAVersionConflict(t *testing.T) {
	stale := temporal.NewNonRetryableApplicationError("defer: lost the race", "VersionConflict", nil)
	tests := []struct {
		name         string
		err          error
		wantConflict bool
	}{
		{name: "the handler's own error", err: stale, wantConflict: true},
		{name: "wrapped by the activity failure", err: fmt.Errorf("activity: %w", stale), wantConflict: true},
		{name: "another application error", err: temporal.NewApplicationError("db down", "Other", nil)},
		{name: "a validation failure", err: temporal.NewNonRetryableApplicationError("bad", "ValidationError", nil)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := temporalclient.New(&fakeSDKClient{updateErr: tt.err})

			err := c.UpdateWorkflow(context.Background(), "wf", uuid.New(), "stage-defer", struct{}{})

			require.Error(t, err)
			assert.Equal(t, tt.wantConflict, errors.Is(err, port.ErrRecordVersionConflict))
			assert.NotErrorIs(t, err, port.ErrTemporalUnavailable)
		})
	}
}
