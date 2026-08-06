package port

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// CursorPosition is the decoded form of the opaque keyset `cursor` every list
// endpoint accepts (LLD §5.9): the previous page's last (created_at, id)
// pair, in the same (created_at DESC, id DESC) order every keyset index in
// this schema is built around.
type CursorPosition struct {
	CreatedAt time.Time
	ID        uuid.UUID
}

type cursorWire struct {
	CreatedAt time.Time `json:"created_at"`
	ID        uuid.UUID `json:"id"`
}

// EncodeCursor produces the opaque, base64-encoded JSON `next_cursor` wire
// value a real list query hands back after its last row.
func EncodeCursor(p CursorPosition) string {
	b, _ := json.Marshal(cursorWire(p)) // fields are always JSON-safe; Marshal never errors
	return base64.URLEncoding.EncodeToString(b)
}

// DecodeCursor reverses EncodeCursor. Per LLD §5.9's explicit edge-case
// decision, an unparseable or tampered cursor must be rejected outright
// (400 BAD_REQUEST at the call site) rather than silently treated as "no
// cursor" — that would mask a client-side bug as a quietly-restarted page.
func DecodeCursor(raw string) (CursorPosition, error) {
	b, err := base64.URLEncoding.DecodeString(raw)
	if err != nil {
		return CursorPosition{}, fmt.Errorf("decode cursor: %w", err)
	}
	var w cursorWire
	if err := json.Unmarshal(b, &w); err != nil {
		return CursorPosition{}, fmt.Errorf("decode cursor: %w", err)
	}
	if w.CreatedAt.IsZero() {
		return CursorPosition{}, fmt.Errorf("decode cursor: missing created_at")
	}
	if w.ID == uuid.Nil {
		return CursorPosition{}, fmt.Errorf("decode cursor: missing id")
	}
	return CursorPosition(w), nil
}
