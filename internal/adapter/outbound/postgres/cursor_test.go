package postgres

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestEncodeDecodeCursor_RoundTrip(t *testing.T) {
	c := port.Cursor{CreatedAt: time.Date(2026, 7, 20, 9, 14, 2, 0, time.UTC), ID: uuid.New()}
	decoded, err := DecodeCursor(EncodeCursor(c))
	require.NoError(t, err)
	assert.True(t, c.CreatedAt.Equal(decoded.CreatedAt))
	assert.Equal(t, c.ID, decoded.ID)
}

func TestDecodeCursor_Invalid(t *testing.T) {
	cases := map[string]string{
		"not base64":             "not-valid-base64!!!",
		"valid base64, not json": "bm90IGpzb24=",
		"valid json, zero id":    "eyJjcmVhdGVkX2F0IjoiMjAyNi0wNy0yMFQwOTowMDowMFoiLCJpZCI6IjAwMDAwMDAwLTAwMDAtMDAwMC0wMDAwLTAwMDAwMDAwMDAwMCJ9",
	}
	for name, s := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := DecodeCursor(s)
			assert.ErrorIs(t, err, ErrInvalidCursor)
		})
	}
}

func TestClampLimit(t *testing.T) {
	assert.Equal(t, defaultPageLimit, clampLimit(0))
	assert.Equal(t, defaultPageLimit, clampLimit(-5))
	assert.Equal(t, 10, clampLimit(10))
	assert.Equal(t, maxPageLimit, clampLimit(1000))
}
