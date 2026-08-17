package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var _ port.TemplateCachePrewarmer = (*TemplateCachePrewarmer)(nil)

// compiledPlanCacheTTL/templateVersionCacheTTL are provisional numbers
// (matching this LLD's own treatment of MAX_CLIENT_CONN/
// MAX_TENANT_QUEUES_PER_WORKER elsewhere) — a cache miss always falls
// through to a direct GetCompiledWorkflow call, so an over- or
// under-generous TTL only affects hit rate, never correctness.
const (
	compiledPlanCacheTTL    = time.Hour
	templateVersionCacheTTL = time.Hour
)

func compiledPlanCacheKey(tenantID, versionID uuid.UUID) string {
	return "compiled_plan:" + tenantID.String() + ":" + versionID.String()
}

func templateVersionCacheKey(tenantID uuid.UUID, workflowKey string) string {
	return "workflow_key_version:" + tenantID.String() + ":" + workflowKey
}

// TemplateCachePrewarmer implements port.TemplateCachePrewarmer (LLD §6.2
// item 5): refreshes the compiled-plan cache InstanceService.Start's own
// cache-aside read consults, plus the workflow_key -> active version_id map.
// Fail-open by handler contract — Prewarm still returns its own error
// rather than swallowing it, so the caller can log cause, but a failure
// here never blocks the 200 the handler already commits to; the plan loads
// lazily on Start's next direct GetCompiledWorkflow call regardless.
type TemplateCachePrewarmer struct {
	Definitions port.DefinitionServiceClient
	Cache       port.CacheStore
}

func (s *TemplateCachePrewarmer) Prewarm(ctx context.Context, in port.TemplatePublishedInput) error {
	compiled, err := s.Definitions.GetCompiledWorkflow(ctx, in.TenantID, in.VersionID)
	if err != nil {
		return err
	}

	raw, err := json.Marshal(compiled)
	if err != nil {
		return fmt.Errorf("marshal compiled workflow for cache: %w", err)
	}
	if err := s.Cache.Set(ctx, compiledPlanCacheKey(in.TenantID, in.VersionID), string(raw), compiledPlanCacheTTL); err != nil {
		return err
	}
	return s.Cache.Set(ctx, templateVersionCacheKey(in.TenantID, in.WorkflowKey), in.VersionID.String(), templateVersionCacheTTL)
}
