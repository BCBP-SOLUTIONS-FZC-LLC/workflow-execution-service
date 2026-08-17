package port_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestAssigneeIneligibleError(t *testing.T) {
	err := &port.AssigneeIneligibleError{Nodes: []string{"finance/review"}}

	assert.Equal(t, port.ErrAssigneeIneligible.Error(), err.Error())
	assert.ErrorIs(t, err, port.ErrAssigneeIneligible)
	assert.True(t, errors.Is(err, port.ErrAssigneeIneligible))
}
