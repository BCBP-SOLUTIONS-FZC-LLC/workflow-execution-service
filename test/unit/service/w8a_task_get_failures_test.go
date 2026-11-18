package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var errW8aTaskLookup = errors.New("w8a: task lookup failed")

// w8aFailingTaskLookup fails every task read and records its tenant and task id.
type w8aFailingTaskLookup struct {
	*csdTaskRepo
	reads []csdIDCall
}

func (r *w8aFailingTaskLookup) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Task, error) {
	r.reads = append(r.reads, csdIDCall{TenantID: tenantID, ID: id})
	return nil, errW8aTaskLookup
}

func TestTaskGet_TaskLookupFailsUnexpectedly_ReturnsTheUnmappedErrorAndReadsNoAssignments(t *testing.T) {
	scopes := map[string]func(h *csdTaskHarness) port.ReadScope{
		"administrator": func(*csdTaskHarness) port.ReadScope { return port.ReadScope{IsAdmin: true} },
		"assignee":      func(h *csdTaskHarness) port.ReadScope { return port.ReadScope{CallerUserID: h.callerID} },
	}
	for name, scope := range scopes {
		t.Run(name, func(t *testing.T) {
			h := csdNewTaskHarness(t, "all", csdReviewStage())
			failing := &w8aFailingTaskLookup{csdTaskRepo: h.tasks}
			h.svc.Tasks = failing

			got, assignments, err := h.svc.Get(context.Background(), h.tenantID, h.task.ID, scope(h))

			require.ErrorIs(t, err, errW8aTaskLookup)
			for _, mapped := range []error{port.ErrTaskNotFound, port.ErrNotAuthorizedForRead, port.ErrRecordVersionConflict} {
				assert.NotErrorIs(t, err, mapped, "an unexpected failure is not reported as a refusal the caller can act on")
			}
			assert.Nil(t, got)
			assert.Nil(t, assignments)
			assert.Equal(t, []csdIDCall{{TenantID: h.tenantID, ID: h.task.ID}}, failing.reads, "one read, for the caller's tenant and the task asked for")
			assert.Empty(t, h.assignments.lists, "no assignment query follows a failed task read")
		})
	}
}
