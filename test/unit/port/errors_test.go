package port_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestAssigneeIneligibleError(t *testing.T) {
	err := &port.AssigneeIneligibleError{Nodes: []string{"finance/review"}}

	assert.Contains(t, err.Error(), "default assignees", "a start is refused for its default assignees; a task action uses the sentinel's own text")
	assert.ErrorIs(t, err, port.ErrAssigneeIneligible)
	assert.True(t, errors.Is(err, port.ErrAssigneeIneligible))
}
