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

var errW16aPlanDown = errors.New("w16a: definitions unreachable")

type w16aPlanRead struct{ tenantID, versionID uuid.UUID }

type w16aFailingDefinitions struct {
	fakeDefinitionClient
	reads []w16aPlanRead
}

func (d *w16aFailingDefinitions) GetCompiledWorkflow(ctx context.Context, tenantID, versionID uuid.UUID) (*port.CompiledWorkflow, error) {
	d.reads = append(d.reads, w16aPlanRead{tenantID, versionID})
	return d.fakeDefinitionClient.GetCompiledWorkflow(ctx, tenantID, versionID)
}

func TestTaskService_Logger_AConfiguredLoggerHearsTheWarningAndNoLoggerStillWorks(t *testing.T) {
	cases := []struct {
		name      string
		withLog   bool
		wantWarns int
	}{
		{"a configured logger receives the plan warning with its cause", true, 1},
		{"without a logger the same failure is returned and nothing panics", false, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, instances, tasks, assignments, _, temporal := newTaskServiceHarness()
			definitions := &w16aFailingDefinitions{fakeDefinitionClient: fakeDefinitionClient{err: errW16aPlanDown}}
			svc.Definitions = definitions
			svc.Eligibility = &fakeEligibilityChecker{}
			log := &fakeLogger{}
			if tc.withLog {
				svc.Log = log
			}
			tenantID, instanceID, versionID, newUserID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
			instances.byID[instanceID] = &domain.Instance{ID: instanceID, TenantID: tenantID, WorkflowVersionID: versionID, TemporalWorkflowID: "tenant:biz", Status: domain.InstanceStatusRunning}
			task := newReadyTask(tenantID, instanceID)
			tasks.byID[task.ID] = task
			assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: uuid.New(), IsActive: true}

			require.NotPanics(t, func() {
				_, err := svc.Reassign(context.Background(), tenantID, task.ID, uuid.New(), newUserID, task.RecordVersion)
				require.ErrorIs(t, err, errW16aPlanDown)
			})

			assert.Equal(t, []w16aPlanRead{{tenantID, versionID}}, definitions.reads, "the plan is read once for the instance's own tenant and version")
			assert.Empty(t, temporal.signals)
			require.Len(t, log.warnFields, tc.wantWarns)
			if tc.withLog {
				assert.Equal(t, []string{"compiled plan unreadable"}, log.warnCalls)
				assert.Equal(t, map[string]any{"version_id": versionID, "error": errW16aPlanDown.Error()}, log.warnFields[0])
			}
		})
	}
}
