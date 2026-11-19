package postgres

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres/db"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
)

func TestMapErr_PrimaryKeyUniqueViolation_ReturnsErrAlreadyExists(t *testing.T) {
	cases := []struct {
		name       string
		constraint string
	}{
		{name: "instance primary key", constraint: constraintInstancePK},
		{name: "task primary key", constraint: constraintTaskPK},
		{name: "assignment primary key", constraint: constraintTaskAssignmentPK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := mapErr(&pgconn.PgError{Code: "23505", ConstraintName: tc.constraint})

			require.ErrorIs(t, err, domain.ErrAlreadyExists)
			assert.NotErrorIs(t, err, domain.ErrDuplicateBusinessKey)
			assert.NotErrorIs(t, err, domain.ErrDuplicateActiveAssignment)
		})
	}
}

func TestMapErr_NonUniqueViolationOnAKnownConstraint_PassesThroughUnchanged(t *testing.T) {
	cases := []struct {
		name string
		code string
	}{
		{name: "foreign key violation", code: "23503"},
		{name: "not-null violation", code: "23502"},
		{name: "check violation", code: "23514"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := &pgconn.PgError{Code: tc.code, ConstraintName: constraintTaskPK}

			got := mapErr(in)

			assert.Same(t, error(in), got)
			assert.NotErrorIs(t, got, domain.ErrAlreadyExists)
		})
	}
}

func TestToNullableText_EmptyStringIsNull(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want pgtype.Text
	}{
		{name: "empty string writes NULL", in: "", want: pgtype.Text{}},
		{name: "text writes a valid value", in: "run-1", want: pgtype.Text{String: "run-1", Valid: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, toNullableText(tc.in))
		})
	}
}

func TestPgtypeTimestamptz_NilIsNullAndValueRoundTrips(t *testing.T) {
	at := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	cases := []struct {
		name      string
		in        *time.Time
		wantValid bool
	}{
		{name: "nil pointer writes NULL", in: nil, wantValid: false},
		{name: "time writes a valid value", in: &at, wantValid: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			written := toPgtypeTimestamptz(tc.in)
			require.Equal(t, tc.wantValid, written.Valid)

			read := fromPgtypeTimestamptz(written)

			if !tc.wantValid {
				assert.Nil(t, read)
				return
			}
			require.NotNil(t, read)
			assert.True(t, at.Equal(*read))
		})
	}
	t.Run("a NULL column reads as nil", func(t *testing.T) {
		assert.Nil(t, fromPgtypeTimestamptz(pgtype.Timestamptz{}))
	})
	t.Run("the read value is a copy", func(t *testing.T) {
		src := pgtype.Timestamptz{Time: at, Valid: true}
		got := fromPgtypeTimestamptz(src)
		require.NotNil(t, got)
		*got = got.Add(time.Hour)
		assert.True(t, at.Equal(src.Time), "changing the result must not change the column value")
	})
}

func TestPgtypeUUID_NilIsNullAndValueRoundTrips(t *testing.T) {
	id := uuid.New()
	cases := []struct {
		name      string
		in        *uuid.UUID
		wantValid bool
	}{
		{name: "nil pointer writes NULL", in: nil, wantValid: false},
		{name: "uuid writes a valid value", in: &id, wantValid: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			written := toPgtypeUUID(tc.in)
			require.Equal(t, tc.wantValid, written.Valid)

			read := fromPgtypeUUID(written)

			if !tc.wantValid {
				assert.Nil(t, read)
				return
			}
			assert.Equal(t, [16]byte(id), written.Bytes)
			require.NotNil(t, read)
			assert.Equal(t, id, *read)
		})
	}
}

func TestInstanceFromDB_NullColumns_BecomeEmptyOrNil(t *testing.T) {
	id := uuid.New()
	started := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	cases := []struct {
		name          string
		row           db.WorkflowInstance
		wantRunID     string
		wantStartedAt bool
		wantCompleted bool
	}{
		{
			name:          "all nullable columns NULL",
			row:           db.WorkflowInstance{ID: id},
			wantRunID:     "",
			wantStartedAt: false,
			wantCompleted: false,
		},
		{
			name: "all nullable columns set",
			row: db.WorkflowInstance{
				ID:            id,
				TemporalRunID: pgtype.Text{String: "run-9", Valid: true},
				StartedAt:     pgtype.Timestamptz{Time: started, Valid: true},
				CompletedAt:   pgtype.Timestamptz{Time: started.Add(time.Hour), Valid: true},
			},
			wantRunID:     "run-9",
			wantStartedAt: true,
			wantCompleted: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := instanceFromDB(tc.row)

			assert.Equal(t, id, got.ID)
			assert.Equal(t, tc.wantRunID, got.TemporalRunID)
			assert.Equal(t, tc.wantStartedAt, got.StartedAt != nil)
			assert.Equal(t, tc.wantCompleted, got.CompletedAt != nil)
			if tc.wantStartedAt {
				assert.True(t, started.Equal(*got.StartedAt))
				assert.True(t, started.Add(time.Hour).Equal(*got.CompletedAt))
			}
		})
	}
}

func TestTaskFromDB_NullColumns_BecomeNil(t *testing.T) {
	id, deferredFrom := uuid.New(), uuid.New()
	at := time.Date(2026, 5, 6, 7, 8, 9, 0, time.UTC)
	connector := "email"

	t.Run("all nullable columns NULL", func(t *testing.T) {
		got := taskFromDB(db.WorkflowTask{ID: id, AssigneeMode: "single", VisitCount: 2, RecordVersion: 4})

		assert.Equal(t, id, got.ID)
		assert.Equal(t, "single", got.AssigneeMode)
		assert.EqualValues(t, 2, got.VisitCount)
		assert.EqualValues(t, 4, got.RecordVersion)
		assert.Nil(t, got.ConnectorType)
		assert.Nil(t, got.DeferredFromTaskID)
		assert.Nil(t, got.DueAt)
		assert.Nil(t, got.FollowUpAt)
		assert.Nil(t, got.CompletedAt)
	})

	t.Run("all nullable columns set", func(t *testing.T) {
		got := taskFromDB(db.WorkflowTask{
			ID:                 id,
			ConnectorType:      pgtype.Text{String: connector, Valid: true},
			DeferredFromTaskID: pgtype.UUID{Bytes: deferredFrom, Valid: true},
			DueAt:              pgtype.Timestamptz{Time: at, Valid: true},
			FollowUpAt:         pgtype.Timestamptz{Time: at.Add(time.Hour), Valid: true},
			CompletedAt:        pgtype.Timestamptz{Time: at.Add(2 * time.Hour), Valid: true},
		})

		require.NotNil(t, got.ConnectorType)
		assert.Equal(t, connector, *got.ConnectorType)
		require.NotNil(t, got.DeferredFromTaskID)
		assert.Equal(t, deferredFrom, *got.DeferredFromTaskID)
		require.NotNil(t, got.DueAt)
		assert.True(t, at.Equal(*got.DueAt))
		require.NotNil(t, got.FollowUpAt)
		assert.True(t, at.Add(time.Hour).Equal(*got.FollowUpAt))
		require.NotNil(t, got.CompletedAt)
		assert.True(t, at.Add(2*time.Hour).Equal(*got.CompletedAt))
	})
}

func TestTaskAssignmentFromDB_NullColumns_BecomeEmptyOrNil(t *testing.T) {
	id, assigner := uuid.New(), uuid.New()
	at := time.Date(2026, 7, 8, 9, 10, 11, 0, time.UTC)

	t.Run("all nullable columns NULL", func(t *testing.T) {
		got := taskAssignmentFromDB(db.WorkflowTaskAssignment{ID: id, IsLead: true, IsActive: true})

		assert.Equal(t, id, got.ID)
		assert.True(t, got.IsLead)
		assert.True(t, got.IsActive)
		assert.Nil(t, got.AssignedBy)
		assert.Equal(t, "", got.Reason)
		assert.Nil(t, got.AssignedAt)
		assert.Nil(t, got.ClaimedAt)
		assert.Nil(t, got.CompletedAt)
		assert.Nil(t, got.VacatedAt)
	})

	t.Run("all nullable columns set", func(t *testing.T) {
		got := taskAssignmentFromDB(db.WorkflowTaskAssignment{
			ID:          id,
			AssignedBy:  pgtype.UUID{Bytes: assigner, Valid: true},
			Reason:      pgtype.Text{String: "delegation", Valid: true},
			AssignedAt:  pgtype.Timestamptz{Time: at, Valid: true},
			ClaimedAt:   pgtype.Timestamptz{Time: at.Add(time.Minute), Valid: true},
			CompletedAt: pgtype.Timestamptz{Time: at.Add(2 * time.Minute), Valid: true},
			VacatedAt:   pgtype.Timestamptz{Time: at.Add(3 * time.Minute), Valid: true},
		})

		require.NotNil(t, got.AssignedBy)
		assert.Equal(t, assigner, *got.AssignedBy)
		assert.Equal(t, "delegation", got.Reason)
		require.NotNil(t, got.AssignedAt)
		assert.True(t, at.Equal(*got.AssignedAt))
		require.NotNil(t, got.ClaimedAt)
		assert.True(t, at.Add(time.Minute).Equal(*got.ClaimedAt))
		require.NotNil(t, got.CompletedAt)
		assert.True(t, at.Add(2*time.Minute).Equal(*got.CompletedAt))
		require.NotNil(t, got.VacatedAt)
		assert.True(t, at.Add(3*time.Minute).Equal(*got.VacatedAt))
	})
}
