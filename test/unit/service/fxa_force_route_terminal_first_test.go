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

func TestInstanceForceRoute_StaleRequestOnFinishedInstance_AnswersAlreadyTerminalNotVersionConflict(t *testing.T) {
	calls := map[string]func(s *service.InstanceService, tenantID, instanceID uuid.UUID, version int64) error{
		"force forward": func(s *service.InstanceService, tn, id uuid.UUID, v int64) error {
			return s.ForceForward(context.Background(), tn, id, uuid.New(), "finance/approve", v)
		},
		"force back": func(s *service.InstanceService, tn, id uuid.UUID, v int64) error {
			return s.ForceBack(context.Background(), tn, id, uuid.New(), "finance/review", v)
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
