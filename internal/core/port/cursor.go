package port

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type CursorPosition struct {
	CreatedAt time.Time
	ID        uuid.UUID
}

type cursorWire struct {
	CreatedAt time.Time `json:"created_at"`
	ID        uuid.UUID `json:"id"`
}

func EncodeCursor(p CursorPosition) string {
	b, _ := json.Marshal(cursorWire(p))
	return base64.URLEncoding.EncodeToString(b)
}

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
