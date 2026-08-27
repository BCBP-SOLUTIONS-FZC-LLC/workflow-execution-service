package http

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEligibilityBackoff(t *testing.T) {
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
		assert.Equal(t, tt.want, eligibilityBackoff(tt.attempt))
	}
}
