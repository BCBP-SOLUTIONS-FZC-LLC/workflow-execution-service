package temporal_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	outboundtemporal "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// w25bDefinitions answers every plan read with one canned answer and records
// the tenant and version it was asked for.
type w25bDefinitions struct {
	answer *port.CompiledWorkflow
	err    error
	asked  []w25bAsk
}

type w25bAsk struct {
	tenant  uuid.UUID
	version uuid.UUID
}

func (d *w25bDefinitions) GetCompiledWorkflow(_ context.Context, tenantID, versionID uuid.UUID) (*port.CompiledWorkflow, error) {
	d.asked = append(d.asked, w25bAsk{tenant: tenantID, version: versionID})
	return d.answer, d.err
}

const w25bPlanJSON = `{"main_plan":"main","schema_version":1}`

func w25bPlanDeps(d *w25bDefinitions) *outboundtemporal.Deps {
	return &outboundtemporal.Deps{Definitions: d}
}

func TestGetCompiledPlan_IdsThatAreNotUUIDs_AreRefusedWithoutCallingDefinition(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		in          port.GetCompiledPlanInput
		messagePart string
	}{
		{name: "tenant id", in: port.GetCompiledPlanInput{TenantID: "bad", VersionID: uuid.NewString()}, messagePart: "parse tenant_id"},
		{name: "version id", in: port.GetCompiledPlanInput{TenantID: uuid.NewString(), VersionID: "bad"}, messagePart: "parse version_id"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			defs := &w25bDefinitions{answer: &port.CompiledWorkflow{Status: "PUBLISHED", IsValid: true, CompiledPlanJSON: w25bPlanJSON}}

			_, err := w25bPlanDeps(defs).GetCompiledPlan(context.Background(), tc.in)

			_ = w25bRequireNonRetryable(t, err, "DefinitionServiceClientError", tc.messagePart)
			assert.Empty(t, defs.asked, "an unreadable id never reaches the Definition Service")
		})
	}
}

func TestGetCompiledPlan_AReadablePublishedVersion_AsksDefinitionForExactlyThatTenantAndVersion(t *testing.T) {
	t.Parallel()
	tenant, version := uuid.New(), uuid.New()
	defs := &w25bDefinitions{answer: &port.CompiledWorkflow{Status: "PUBLISHED", IsValid: true, CompiledPlanJSON: w25bPlanJSON}}
	in := port.GetCompiledPlanInput{TenantID: tenant.String(), VersionID: version.String()}

	first, err := w25bPlanDeps(defs).GetCompiledPlan(context.Background(), in)
	require.NoError(t, err)
	second, err := w25bPlanDeps(defs).GetCompiledPlan(context.Background(), in)
	require.NoError(t, err)

	assert.Equal(t, "main", first.Collaboration.MainPlan)
	assert.Equal(t, first, second, "a retry reads the same plan")
	assert.Equal(t, []w25bAsk{{tenant: tenant, version: version}, {tenant: tenant, version: version}}, defs.asked)
}

func TestGetCompiledPlan_VersionThatCannotRun_IsNonRetryableAndNamesWhy(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		status  string
		isValid bool
		wantErr error
		other   error
	}{
		{name: "draft", status: "DRAFT", isValid: true, wantErr: port.ErrVersionNotPublished, other: port.ErrVersionInvalid},
		{name: "archived", status: "ARCHIVED", isValid: true, wantErr: port.ErrVersionNotPublished, other: port.ErrVersionInvalid},
		{name: "published but not valid", status: "PUBLISHED", isValid: false, wantErr: port.ErrVersionInvalid, other: port.ErrVersionNotPublished},
		{name: "draft and not valid", status: "DRAFT", isValid: false, wantErr: port.ErrVersionNotPublished, other: port.ErrVersionInvalid},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			version := uuid.New()
			defs := &w25bDefinitions{answer: &port.CompiledWorkflow{Status: tc.status, IsValid: tc.isValid, CompiledPlanJSON: w25bPlanJSON}}

			_, err := w25bPlanDeps(defs).GetCompiledPlan(context.Background(), port.GetCompiledPlanInput{TenantID: uuid.NewString(), VersionID: version.String()})

			_ = w25bRequireNonRetryable(t, err, "DefinitionServiceClientError", version.String())
			require.ErrorIs(t, err, tc.wantErr)
			assert.False(t, errors.Is(err, tc.other), "the error names one reason only")
			assert.Len(t, defs.asked, 1)
		})
	}
}

func TestNonRetryableErrors_CarryTheirTypeTheCauseMessageAndTheCause(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		wantTyp string
		run     func(t *testing.T) error
	}{
		{name: "validation", wantTyp: "ValidationError", run: func(t *testing.T) error {
			rig := w25bNewRig()
			_, err := w25bNewDeps(t, rig).CreateTask(context.Background(), port.CreateTaskInput{
				TenantID: "bad", InstanceID: uuid.NewString(), CompiledNode: []byte(`{}`),
			})
			return err
		}},
		{name: "not found", wantTyp: "NotFoundError", run: func(t *testing.T) error {
			rig := w25bNewRig()
			return w25bNewDeps(t, rig).UpdateInstanceNodes(context.Background(), port.UpdateInstanceNodesInput{
				TenantID: uuid.NewString(), InstanceID: uuid.NewString(), NodeKeys: []domain.NodeKey{"sales/review"},
			})
		}},
		{name: "definition client", wantTyp: "DefinitionServiceClientError", run: func(t *testing.T) error {
			_, err := w25bPlanDeps(&w25bDefinitions{}).GetCompiledPlan(context.Background(), port.GetCompiledPlanInput{TenantID: uuid.NewString(), VersionID: "bad"})
			return err
		}},
		{name: "version conflict", wantTyp: "VersionConflict", run: func(t *testing.T) error {
			s := w25bNewScene(t, true)
			s.rig.tasks[s.task.ID].RecordVersion++
			return s.reassignToNewUser()
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.run(t)

			appErr := w25bRequireNonRetryable(t, err, tc.wantTyp, "")
			cause := appErr.Unwrap()
			require.NotNil(t, cause, "the cause is attached to the application error")
			assert.Equal(t, cause.Error(), appErr.Message(), "the message is the cause's message")
		})
	}
}
