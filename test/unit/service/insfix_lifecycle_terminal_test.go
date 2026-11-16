package service_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

func TestInstanceLifecycle_StaleRequestOnFinishedInstance_AnswersAlreadyTerminalNotVersionConflict(t *testing.T) {
	calls := map[string]func(svc *service.InstanceService, tenantID, instanceID uuid.UUID, version int64) error{
		"pause": func(s *service.InstanceService, tn, id uuid.UUID, v int64) error {
			return s.Pause(context.Background(), tn, id, uuid.New(), "late", v)
		},
		"cancel": func(s *service.InstanceService, tn, id uuid.UUID, v int64) error {
			return s.Cancel(context.Background(), tn, id, uuid.New(), "late", v)
		},
		"terminate (sibling)": func(s *service.InstanceService, tn, id uuid.UUID, _ int64) error {
			return s.Terminate(context.Background(), tn, id, uuid.New(), "late")
		},
	}
	for name, call := range calls {
		for _, status := range []domain.InstanceStatus{
			domain.InstanceStatusCompleted, domain.InstanceStatusTerminated, domain.InstanceStatusFailed,
		} {
			t.Run(name+"/"+string(status), func(t *testing.T) {
				tenantID := uuid.New()
				inst := newCscInstance(tenantID, status, 6)
				h := newCscHarness(inst)

				err := call(h.svc, tenantID, inst.ID, 5)

				require.ErrorIs(t, err, port.ErrInstanceAlreadyTerminal)
				assert.NotErrorIs(t, err, port.ErrRecordVersionConflict)
				assert.Empty(t, h.temporal.signals)
			})
		}
	}
}

func TestInstanceLifecycle_StaleRequestOnLiveInstance_StillAnswersVersionConflict(t *testing.T) {
	tenantID := uuid.New()
	inst := newCscInstance(tenantID, domain.InstanceStatusRunning, 6)
	h := newCscHarness(inst)

	require.ErrorIs(t, h.svc.Pause(context.Background(), tenantID, inst.ID, uuid.New(), "x", 5), port.ErrRecordVersionConflict)
	require.ErrorIs(t, h.svc.Cancel(context.Background(), tenantID, inst.ID, uuid.New(), "x", 5), port.ErrRecordVersionConflict)
	assert.Empty(t, h.temporal.signals)
}
