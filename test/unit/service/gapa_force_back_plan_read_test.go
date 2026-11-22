package service_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type gapaPlanRead struct{ tenantID, versionID uuid.UUID }

// gapaDefinitions records which tenant and version each compiled-plan read asked for.
type gapaDefinitions struct {
	resp  *port.CompiledWorkflow
	err   error
	reads []gapaPlanRead
}

func (d *gapaDefinitions) GetCompiledWorkflow(_ context.Context, tenantID, versionID uuid.UUID) (*port.CompiledWorkflow, error) {
	d.reads = append(d.reads, gapaPlanRead{tenantID, versionID})
	return d.resp, d.err
}

func TestForceBack_DegradedWithAKeyWhenThePlanCannotBeRead_FailsBeforeTheSignal(t *testing.T) {
	cases := []struct {
		name string
		err  error
		plan string
		want error
	}{
		{
			name: "the Definition Service is unavailable (the route answers 503 UPSTREAM_UNAVAILABLE)",
			err:  fmt.Errorf("get compiled workflow: %w", port.ErrDefinitionUnavailable),
			want: port.ErrDefinitionUnavailable,
		},
		{
			name: "the stored plan is not JSON (the route answers 409 VERSION_INVALID)",
			plan: "{not json",
			want: port.ErrVersionInvalid,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, instances, _, _, _, temporal, _, _ := newInstanceServiceHarness()
			tenantID, versionID := uuid.New(), uuid.New()
			inst := &domain.Instance{
				ID: uuid.New(), TenantID: tenantID, WorkflowVersionID: versionID,
				Status: domain.InstanceStatusDegraded, RecordVersion: 3, TemporalWorkflowID: "tenant:biz",
			}
			instances.byID[inst.ID] = inst
			definitions := &gapaDefinitions{err: tc.err}
			if tc.err == nil {
				definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, tc.plan)
			}
			svc.Definitions = definitions

			err := svc.ForceBack(context.Background(), tenantID, inst.ID, uuid.New(), "finance/check", 3)

			require.ErrorIs(t, err, tc.want)
			assert.Empty(t, temporal.signals, "no signal is sent when the target cannot be checked")
			assert.Equal(t, []gapaPlanRead{{tenantID, versionID}}, definitions.reads, "the plan is read once, for the instance's tenant and version")
		})
	}
}
