package postgres

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// ErrInvalidCursor lets a future HTTP handler map a tampered/unparseable
// cursor to 400 BAD_REQUEST (LLD §5.9) instead of silently restarting the page.
var ErrInvalidCursor = errors.New("invalid pagination cursor")

type cursorPayload struct {
	CreatedAt time.Time `json:"created_at"`
	ID        uuid.UUID `json:"id"`
}

func EncodeCursor(c port.Cursor) string {
	payload := cursorPayload{CreatedAt: c.CreatedAt, ID: c.ID}
	b, _ := json.Marshal(payload) // time.Time/uuid.UUID always marshal
	return base64.StdEncoding.EncodeToString(b)
}

func DecodeCursor(s string) (port.Cursor, error) {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return port.Cursor{}, ErrInvalidCursor
	}
	var payload cursorPayload
	if err := json.Unmarshal(b, &payload); err != nil {
		return port.Cursor{}, ErrInvalidCursor
	}
	if payload.ID == uuid.Nil {
		return port.Cursor{}, ErrInvalidCursor
	}
	return port.Cursor{CreatedAt: payload.CreatedAt, ID: payload.ID}, nil
}

// Mirrors LLD §5.9's HTTP-level pagination contract in case this is called
// before that contract is enforced upstream.
const (
	defaultPageLimit = 25
	maxPageLimit     = 100
)

func clampLimit(limit int) int {
	switch {
	case limit <= 0:
		return defaultPageLimit
	case limit > maxPageLimit:
		return maxPageLimit
	default:
		return limit
	}
}

// paginate expects rows fetched with LIMIT limit+1, trimming the peeked row
// back off — avoids a "phantom next page" a plain len(rows)==limit check
// would produce on an exact-limit final page.
func paginate[T any](rows []T, limit int, key func(T) (time.Time, uuid.UUID)) ([]T, *port.Cursor) {
	if len(rows) <= limit {
		return rows, nil
	}
	rows = rows[:limit]
	createdAt, id := key(rows[limit-1])
	return rows, &port.Cursor{CreatedAt: createdAt, ID: id}
}
