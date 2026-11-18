package port_test

import (
	"encoding/base64"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestDecodeCursor_Base64OfJSONNull_IsRefusedAsMissingCreatedAtNotAsBadJSON(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{name: "json null text", raw: base64.URLEncoding.EncodeToString([]byte("null"))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := port.DecodeCursor(tc.raw)

			require.Error(t, err)
			assert.Contains(t, err.Error(), "missing created_at")
			assert.NotContains(t, err.Error(), "json:", "null is valid JSON; the refusal must come from the field check")
			assert.Equal(t, port.CursorPosition{}, got)
		})
	}
}

func TestDecodeCursor_NilUUIDWithValidCreatedAt_IsRefusedAsMissingID(t *testing.T) {
	const createdAt = "2026-07-20T08:52:11.045Z"
	encode := func(id uuid.UUID) string {
		return base64.URLEncoding.EncodeToString([]byte(`{"created_at":"` + createdAt + `","id":"` + id.String() + `"}`))
	}
	cases := []struct {
		name    string
		raw     string
		wantErr string
	}{
		{name: "nil uuid id", raw: encode(uuid.Nil), wantErr: "missing id"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := port.DecodeCursor(tc.raw)

			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
			assert.Equal(t, port.CursorPosition{}, got)
		})
	}

	t.Run("the same created_at with a real id decodes", func(t *testing.T) {
		id := uuid.MustParse("a3c7d1e2-9f45-4b6a-8c12-3e9b7d4f6a01")

		got, err := port.DecodeCursor(encode(id))

		require.NoError(t, err)
		assert.Equal(t, id, got.ID)
	})
}
