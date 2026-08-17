package postgres

import (
	"time"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

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
