package postgres

import (
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
)

func TestMapErr(t *testing.T) {
	assert.NoError(t, mapErr(nil))
	assert.ErrorIs(t, mapErr(pgx.ErrNoRows), domain.ErrNotFound)

	assert.ErrorIs(t, mapErr(&pgconn.PgError{Code: "23505", ConstraintName: constraintInstanceBusinessKey}),
		domain.ErrDuplicateBusinessKey)
	assert.ErrorIs(t, mapErr(&pgconn.PgError{Code: "23505", ConstraintName: constraintTaskAssignmentActive}),
		domain.ErrDuplicateActiveAssignment)

	unrelated := &pgconn.PgError{Code: "23505", ConstraintName: "some_other_constraint"}
	assert.Same(t, error(unrelated), mapErr(unrelated))

	other := errors.New("boom")
	assert.Same(t, other, mapErr(other))
}

func TestNotFoundOrVersionConflict(t *testing.T) {
	assert.ErrorIs(t, notFoundOrVersionConflict(pgx.ErrNoRows), domain.ErrNotFound)
	assert.ErrorIs(t, notFoundOrVersionConflict(nil), domain.ErrRecordVersionConflict)

	probeErr := errors.New("connection lost")
	assert.Same(t, probeErr, notFoundOrVersionConflict(probeErr))
}
