package temporal_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	outboundtemporal "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestPauseAndResumeInstance_AdminIdThatIsNotAUUID_IsANonRetryableValidationErrorBeforeAnyWrite(t *testing.T) {
	t.Parallel()

	activities := []struct {
		name string
		run  func(d *outboundtemporal.Deps, admin string) error
	}{
		{name: "pause", run: func(d *outboundtemporal.Deps, admin string) error {
			return d.PauseInstance(context.Background(), port.PauseInstanceInput{
				TenantID: uuid.NewString(), InstanceID: uuid.NewString(), AdminUserID: admin, Initiator: domain.InitiatorAdmin, RecordVersion: 3,
			})
		}},
		{name: "resume", run: func(d *outboundtemporal.Deps, admin string) error {
			return d.ResumeInstance(context.Background(), port.ResumeInstanceInput{
				TenantID: uuid.NewString(), InstanceID: uuid.NewString(), AdminUserID: admin, Initiator: domain.InitiatorAdmin, RecordVersion: 3,
			})
		}},
	}
	for _, a := range activities {
		t.Run(a.name, func(t *testing.T) {
			t.Parallel()
			rig := w25bNewRig()

			err := a.run(w25bNewDeps(t, rig), w29aNotAUUID)

			w29aRequireRefusedID(t, rig, err, "admin_user_id")
		})
	}
}
