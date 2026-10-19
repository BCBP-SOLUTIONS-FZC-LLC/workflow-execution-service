package postgres

import (
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
)

func TestFromPgtypeText(t *testing.T) {
	assert.Equal(t, "", fromPgtypeText(pgtype.Text{}))
	assert.Equal(t, "hello", fromPgtypeText(pgtype.Text{String: "hello", Valid: true}))
}

func TestFromPgtypeUUID(t *testing.T) {
	assert.Nil(t, fromPgtypeUUID(pgtype.UUID{}))

	id := uuid.New()
	got := fromPgtypeUUID(pgtype.UUID{Bytes: id, Valid: true})
	if assert.NotNil(t, got) {
		assert.Equal(t, id, *got)
	}
}

func TestTextPointerRoundTrip(t *testing.T) {
	assert.Nil(t, fromPgtypeTextPtr(toPgtypeTextPtr(nil)))
	v := "abc"
	assert.True(t, toPgtypeTextPtr(&v).Valid)
	assert.Equal(t, &v, fromPgtypeTextPtr(toPgtypeTextPtr(&v)))
}
