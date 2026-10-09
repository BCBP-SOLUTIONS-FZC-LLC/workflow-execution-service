package domain

import "errors"

var (
	ErrNotFound = errors.New("not found")

	ErrDuplicateBusinessKey      = errors.New("business key already active for this subject")
	ErrRecordVersionConflict     = errors.New("record version conflict")
	ErrDuplicateActiveAssignment = errors.New("an active assignment already exists for this task and user")
	ErrAlreadyExists             = errors.New("already exists")
)
