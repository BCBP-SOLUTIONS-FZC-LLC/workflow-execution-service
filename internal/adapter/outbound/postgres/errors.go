package postgres

import (
	"errors"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"
	"github.com/jackc/pgx/v5"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
)

const (
	constraintInstanceBusinessKey  = "uq_workflow_instance_business_key"
	constraintTaskAssignmentActive = "uq_workflow_task_assignment_active"
	constraintTaskPK               = "workflow_task_pkey"
	constraintTaskAssignmentPK     = "workflow_task_assignment_pkey"
	constraintSLAEventTaskUnique   = "idx_outbox_events_sla_task_unique"
)

func notFoundOrVersionConflict(probeErr error) error {
	if errors.Is(probeErr, pgx.ErrNoRows) {
		return domain.ErrNotFound
	}
	if probeErr != nil {
		return probeErr
	}
	return domain.ErrRecordVersionConflict
}

func mapErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrNotFound
	}
	if pgcommon.IsUniqueViolation(err) {
		switch pgcommon.ConstraintName(err) {
		case constraintInstanceBusinessKey:
			return domain.ErrDuplicateBusinessKey
		case constraintTaskAssignmentActive:
			return domain.ErrDuplicateActiveAssignment
		case constraintTaskPK, constraintTaskAssignmentPK:
			return domain.ErrAlreadyExists
		case constraintSLAEventTaskUnique:
			return domain.ErrAlreadyExists
		}
	}
	return err
}
