package valkeystream

import (
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToEntries_EmptyInputs_ReturnNoEntries(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		run  func() []Entry
	}{
		{"nil stream list", func() []Entry { return toEntries(nil) }},
		{"empty stream list", func() []Entry { return toEntries([]redis.XStream{}) }},
		{"stream with no messages", func() []Entry {
			return toEntries([]redis.XStream{{Stream: "vsx-stream"}})
		}},
		{"nil message list", func() []Entry { return toEntriesFromMessages(nil) }},
		{"empty message list", func() []Entry { return toEntriesFromMessages([]redis.XMessage{}) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var got []Entry
			require.NotPanics(t, func() { got = tc.run() })
			assert.Empty(t, got)
		})
	}
}

func TestToEntriesFromMessages_EntryWithoutFields_KeepsIDWithEmptyFieldMap(t *testing.T) {
	t.Parallel()

	got := toEntriesFromMessages([]redis.XMessage{
		{ID: "1-0"},
		{ID: "2-0", Values: map[string]any{}},
		{ID: "3-0", Values: map[string]any{"text": "kept", "number": int64(7)}},
	})

	require.Len(t, got, 3)
	assert.Equal(t, "1-0", got[0].ID)
	assert.NotNil(t, got[0].Fields, "an entry with no values still gets a usable field map")
	assert.Empty(t, got[0].Fields)
	assert.Equal(t, "2-0", got[1].ID)
	assert.Empty(t, got[1].Fields)
	assert.Equal(t, "3-0", got[2].ID)
	assert.Equal(t, map[string]string{"text": "kept"}, got[2].Fields, "a non-string value is left out, not stringified")
}

func TestToEntries_SeveralStreams_KeepsMessageOrder(t *testing.T) {
	t.Parallel()

	got := toEntries([]redis.XStream{
		{Stream: "vsx-a", Messages: []redis.XMessage{{ID: "1-0"}, {ID: "2-0"}}},
		{Stream: "vsx-b", Messages: []redis.XMessage{{ID: "3-0"}}},
	})

	ids := make([]string, len(got))
	for i, e := range got {
		ids[i] = e.ID
	}
	assert.Equal(t, []string{"1-0", "2-0", "3-0"}, ids)
}
