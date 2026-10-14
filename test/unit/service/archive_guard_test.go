package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

func TestArchiveGuard_CheckActiveInstances(t *testing.T) {
	t.Run("active instances exist", func(t *testing.T) {
		instances := newFakeInstanceRepo()
		instances.countResult = 3
		guard := &service.ArchiveGuard{Instances: instances}

		hasActive, count, err := guard.CheckActiveInstances(context.Background(), uuid.New(), uuid.New())
		require.NoError(t, err)
		assert.True(t, hasActive)
		assert.Equal(t, int32(3), count)
	})

	t.Run("no active instances", func(t *testing.T) {
		instances := newFakeInstanceRepo()
		guard := &service.ArchiveGuard{Instances: instances}

		hasActive, count, err := guard.CheckActiveInstances(context.Background(), uuid.New(), uuid.New())
		require.NoError(t, err)
		assert.False(t, hasActive)
		assert.Zero(t, count)
	})

	t.Run("repo error passes through", func(t *testing.T) {
		instances := newFakeInstanceRepo()
		instances.countErr = errors.New("boom")
		guard := &service.ArchiveGuard{Instances: instances}

		_, _, err := guard.CheckActiveInstances(context.Background(), uuid.New(), uuid.New())
		assert.Error(t, err)
	})
}
