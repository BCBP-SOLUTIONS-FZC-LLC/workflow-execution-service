package port_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestCursor_RoundTrip(t *testing.T) {
	pos := port.CursorPosition{
		CreatedAt: time.Date(2026, 7, 20, 8, 52, 11, 45_000_000, time.UTC),
		ID:        uuid.MustParse("a3c7d1e2-9f45-4b6a-8c12-3e9b7d4f6a01"),
	}

	decoded, err := port.DecodeCursor(port.EncodeCursor(pos))
	require.NoError(t, err)
	assert.True(t, pos.CreatedAt.Equal(decoded.CreatedAt))
	assert.Equal(t, pos.ID, decoded.ID)
}

func TestCursor_DecodeMatchesLLDWorkedExample(t *testing.T) {
	// LLD §5.9's own worked example next_cursor value — decoding it directly
	// pins this codec to the documented wire format, not just its own
	// round-trip.
	const example = "eyJjcmVhdGVkX2F0IjoiMjAyNi0wNy0yMFQwODo1MjoxMS4wNDVaIiwiaWQiOiJhM2M3ZDFlMi05ZjQ1LTRiNmEtOGMxMi0zZTliN2Q0ZjZhMDEifQ=="

	decoded, err := port.DecodeCursor(example)
	require.NoError(t, err)
	assert.Equal(t, uuid.MustParse("a3c7d1e2-9f45-4b6a-8c12-3e9b7d4f6a01"), decoded.ID)
	assert.Equal(t, 2026, decoded.CreatedAt.Year())
}

func TestCursor_DecodeInvalid(t *testing.T) {
	cases := map[string]string{
		"not base64 at all":       "not-valid-base64!!!",
		"base64 but not JSON":     "bm90LWpzb24=",                                             // "not-json"
		"valid JSON, wrong shape": "eyJmb28iOiJiYXIifQ==",                                     // {"foo":"bar"}
		"missing id":              "eyJjcmVhdGVkX2F0IjoiMjAyNi0wNy0yMFQwODo1MjoxMS4wNDVaIn0=", // {"created_at":"..."}
		"empty string":            "",
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := port.DecodeCursor(raw)
			assert.Error(t, err)
		})
	}
}
