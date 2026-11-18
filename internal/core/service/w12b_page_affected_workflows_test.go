package service

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// w12bOrderedID is an id whose byte order is its first byte, so a test can name ids between and beyond the listed ones.
func w12bOrderedID(first byte) uuid.UUID {
	var id uuid.UUID
	id[0] = first
	return id
}

func w12bAffected() []affectedWorkflow {
	base := time.Date(2026, 11, 18, 9, 0, 0, 0, time.UTC)
	out := make([]affectedWorkflow, 0, 5)
	for i, first := range []byte{0x10, 0x20, 0x30, 0x40, 0x50} {
		out = append(out, affectedWorkflow{id: w12bOrderedID(first), createdAt: base.Add(time.Duration(i) * time.Hour)})
	}
	return out
}

func TestPageAffectedWorkflows_LimitCursorAndBoundaries(t *testing.T) {
	affected := w12bAffected()
	ids := func(from, to int) []uuid.UUID {
		out := make([]uuid.UUID, 0, to-from)
		for _, w := range affected[from:to] {
			out = append(out, w.id)
		}
		return out
	}
	at := func(i int) *port.CursorPosition {
		return &port.CursorPosition{CreatedAt: affected[i].createdAt, ID: affected[i].id}
	}

	cases := []struct {
		name       string
		list       []affectedWorkflow
		page       port.Page
		wantIDs    []uuid.UUID
		wantCursor int // index of the item the next cursor names; -1 for no cursor
	}{
		{"limit 0 returns every item and no cursor", affected, port.Page{Limit: 0}, ids(0, 5), -1},
		{"a negative limit returns every item and no cursor", affected, port.Page{Limit: -1}, ids(0, 5), -1},
		{"a limit above the remainder returns every item and no cursor", affected, port.Page{Limit: 9}, ids(0, 5), -1},
		{"a limit equal to the remainder returns every item and no cursor", affected, port.Page{Limit: 5}, ids(0, 5), -1},
		{"a limit one below the remainder gives a cursor at the last item returned", affected, port.Page{Limit: 4}, ids(0, 4), 3},
		{"a limit of one gives a cursor at the first item", affected, port.Page{Limit: 1}, ids(0, 1), 0},
		{"a cursor at an item starts after it", affected, port.Page{Cursor: at(0), Limit: 2}, ids(1, 3), 2},
		{"a limit equal to the remainder after a cursor ends the walk", affected, port.Page{Cursor: at(1), Limit: 3}, ids(2, 5), -1},
		{"a cursor naming no listed id starts at the first larger id", affected, port.Page{Cursor: &port.CursorPosition{CreatedAt: affected[0].createdAt, ID: w12bOrderedID(0x25)}, Limit: 2}, ids(2, 4), 3},
		{"a cursor before the first id starts at the first item", affected, port.Page{Cursor: &port.CursorPosition{CreatedAt: affected[0].createdAt, ID: w12bOrderedID(0x01)}}, ids(0, 5), -1},
		{"a cursor at the last item returns nothing and no cursor", affected, port.Page{Cursor: at(4), Limit: 2}, ids(0, 0), -1},
		{"a cursor past the last item returns nothing and no cursor", affected, port.Page{Cursor: &port.CursorPosition{CreatedAt: affected[4].createdAt, ID: w12bOrderedID(0x60)}, Limit: 2}, ids(0, 0), -1},
		{"an empty list returns nothing and no cursor", nil, port.Page{Limit: 3}, ids(0, 0), -1},
		{"an empty list with a cursor returns nothing and no cursor", nil, port.Page{Cursor: at(0), Limit: 3}, ids(0, 0), -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, next := pageAffectedWorkflows(tc.list, tc.page)

			assert.Equal(t, tc.wantIDs, got)
			if tc.wantCursor < 0 {
				assert.Empty(t, next)
				return
			}
			require.NotEmpty(t, next)
			position, err := port.DecodeCursor(next)
			require.NoError(t, err, "the cursor handed to the caller decodes")
			assert.Equal(t, affected[tc.wantCursor].id, position.ID)
			assert.True(t, affected[tc.wantCursor].createdAt.Equal(position.CreatedAt))
		})
	}

	t.Run("the cursor of a page returns exactly the rest when it is fed back", func(t *testing.T) {
		first, next := pageAffectedWorkflows(affected, port.Page{Limit: 4})
		require.Equal(t, ids(0, 4), first)
		position, err := port.DecodeCursor(next)
		require.NoError(t, err)

		rest, after := pageAffectedWorkflows(affected, port.Page{Cursor: &position, Limit: 4})

		assert.Equal(t, ids(4, 5), rest, "the fifth item, and only it")
		assert.Empty(t, after)
	})
}
