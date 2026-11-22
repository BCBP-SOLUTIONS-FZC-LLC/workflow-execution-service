package valkeystream

import (
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToEntriesFromMessages_StringValues_AreCopiedAndIDIsTheMessageID(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		id     string
		values map[string]any
		want   map[string]string
	}{
		{
			name:   "plain strings",
			id:     "1700000000000-0",
			values: map[string]any{"task_id": "abc", "seq": "7"},
			want:   map[string]string{"task_id": "abc", "seq": "7"},
		},
		{
			name:   "empty string value is kept",
			id:     "1700000000001-3",
			values: map[string]any{"node_key": "", "task_id": "abc"},
			want:   map[string]string{"node_key": "", "task_id": "abc"},
		},
		{
			name:   "unicode and json text is kept byte for byte",
			id:     "1700000000002-0",
			values: map[string]any{"reason": "zażółć gęślą jaźń \U0001F680", "resolved_inputs": `{"a":"b"}`},
			want:   map[string]string{"reason": "zażółć gęślą jaźń \U0001F680", "resolved_inputs": `{"a":"b"}`},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := toEntriesFromMessages([]redis.XMessage{{ID: tc.id, Values: tc.values}})

			require.Len(t, got, 1)
			assert.Equal(t, tc.id, got[0].ID)
			assert.Equal(t, tc.want, got[0].Fields)
			assert.Zero(t, got[0].Deliveries, "the decode step does not know the delivery count")
		})
	}
}

// The non-string branch is defensive: the client returns string values for stream entries, so this
// only proves a stray value is skipped and does not stop the entry from being decoded.
func TestToEntriesFromMessages_NonStringValues_AreDroppedAndTheRestKept(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		values map[string]any
		want   map[string]string
	}{
		{
			name:   "int and nil next to a string",
			values: map[string]any{"task_id": "abc", "n": 42, "x": nil},
			want:   map[string]string{"task_id": "abc"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := toEntriesFromMessages([]redis.XMessage{{ID: "5-0", Values: tc.values}})

			require.Len(t, got, 1)
			assert.Equal(t, "5-0", got[0].ID)
			assert.NotNil(t, got[0].Fields)
			assert.Equal(t, tc.want, got[0].Fields)
		})
	}
}
