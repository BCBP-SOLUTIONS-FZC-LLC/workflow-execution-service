package domain

import "errors"

var (
	// Also covers a row filtered out by RLS (LLD §5.9): not-found and
	// wrong-tenant are indistinguishable at this layer.
	ErrNotFound = errors.New("not found")

	ErrDuplicateBusinessKey      = errors.New("business key already active for this tenant")
	ErrRecordVersionConflict     = errors.New("record version conflict")
	ErrDuplicateActiveAssignment = errors.New("an active assignment already exists for this task and user")
	// ErrAlreadyExists marks a primary-key conflict on a row given a
	// deterministic, replay-derived ID (CreateTask/DeferTask's regression
	// task) — the intended idempotent-retry signal under Temporal's
	// at-least-once activity execution, not a real conflict.
	ErrAlreadyExists = errors.New("already exists")
)
