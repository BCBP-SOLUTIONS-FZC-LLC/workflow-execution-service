package postgres

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClampLimit(t *testing.T) {
	assert.Equal(t, defaultPageLimit, clampLimit(0))
	assert.Equal(t, defaultPageLimit, clampLimit(-5))
	assert.Equal(t, 10, clampLimit(10))
	assert.Equal(t, maxPageLimit, clampLimit(1000))
}
