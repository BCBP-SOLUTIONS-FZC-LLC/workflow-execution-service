package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

func TestTemplateCachePrewarmer_Prewarm(t *testing.T) {
	t.Run("refreshes both the compiled-plan and workflow-key caches", func(t *testing.T) {
		definitions := &fakeDefinitionClient{}
		cache := newFakeCacheStore()
		tenantID, workflowID, versionID := uuid.New(), uuid.New(), uuid.New()
		definitions.resp = &port.CompiledWorkflow{WorkflowID: workflowID, VersionID: versionID, Status: "PUBLISHED", IsValid: true, CompiledPlanJSON: `{"main_plan":"main"}`}

		svc := &service.TemplateCachePrewarmer{Definitions: definitions, Cache: cache}
		err := svc.Prewarm(context.Background(), port.TemplatePublishedInput{TenantID: tenantID, WorkflowID: workflowID, WorkflowKey: "onboarding", VersionID: versionID})
		require.NoError(t, err)
		assert.Len(t, cache.setCalls, 2)
		assert.NotEmpty(t, cache.values["compiled_plan:"+tenantID.String()+":"+versionID.String()])
		assert.Equal(t, versionID.String(), cache.values["workflow_key_version:"+tenantID.String()+":onboarding"])
	})

	t.Run("a GetCompiledWorkflow failure returns an error and writes nothing", func(t *testing.T) {
		definitions := &fakeDefinitionClient{err: errors.New("upstream down")}
		cache := newFakeCacheStore()
		svc := &service.TemplateCachePrewarmer{Definitions: definitions, Cache: cache}

		err := svc.Prewarm(context.Background(), port.TemplatePublishedInput{TenantID: uuid.New(), VersionID: uuid.New()})
		require.Error(t, err)
		assert.Empty(t, cache.setCalls)
	})

	t.Run("a cache Set failure surfaces the error", func(t *testing.T) {
		definitions := &fakeDefinitionClient{resp: &port.CompiledWorkflow{Status: "PUBLISHED", IsValid: true}}
		cache := newFakeCacheStore()
		cache.setErr = errors.New("valkey unavailable")
		svc := &service.TemplateCachePrewarmer{Definitions: definitions, Cache: cache}

		err := svc.Prewarm(context.Background(), port.TemplatePublishedInput{TenantID: uuid.New(), VersionID: uuid.New()})
		assert.Error(t, err)
	})
}
