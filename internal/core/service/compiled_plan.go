package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

const scopeTender = "tender"

func scopeMatches(ctx context.Context, log port.Logger, plans *compiledPlanCache, tenantID uuid.UUID, c delegationCandidate, scope string, scopeID *string) bool {
	switch scope {
	case "all":
		return true
	case "department":
		if scopeID == nil {
			return false
		}
		plan, err := plans.mainPlan(ctx, tenantID, c.instance.WorkflowVersionID)
		if err != nil {
			log.Warn("scope match: skipping candidate with unreadable compiled plan", map[string]any{"task_id": c.task.ID})
			return false
		}
		taskDeptID, _ := deptAndSuffix(c.task.NodeKey)
		for _, dept := range plan.Departments {
			if dept.ID != taskDeptID {
				continue
			}
			return sameUUID(dept.IAMDepartmentID, *scopeID) && deptUUID(&dept) == c.task.DepartmentID
		}
		return false
	case scopeTender:
		return scopeID != nil && sameUUID(c.instance.SubjectID, *scopeID)
	default:
		observability.IncCounterVec(observability.DelegationScopeUnhandledTotal, observability.UnhandledScopeLabel)
		log.Warn("scope match: delegation scope is unknown to this service, matching nothing", map[string]any{
			"scope": scope, "task_id": c.task.ID,
		})
		return false
	}
}

// sameUUID compares two ids by value; text that is not a UUID, or is the nil UUID, on either
// side matches nothing.
func sameUUID(a, b string) bool {
	left, err := uuid.Parse(a)
	if err != nil || left == uuid.Nil {
		return false
	}
	right, err := uuid.Parse(b)
	return err == nil && left == right
}

type compiledPlanCache struct {
	definitions port.DefinitionServiceClient
	store       port.CacheStore
	log         port.Logger
	byVersion   map[uuid.UUID]*dsl.CompiledPlan
}

func newCompiledPlanCache(definitions port.DefinitionServiceClient, log port.Logger) *compiledPlanCache {
	return &compiledPlanCache{definitions: definitions, log: log, byVersion: make(map[uuid.UUID]*dsl.CompiledPlan)}
}

func (c *compiledPlanCache) backedBy(store port.CacheStore) *compiledPlanCache {
	c.store = store
	return c
}

func cachedCompiledWorkflow(
	ctx context.Context, store port.CacheStore, definitions port.DefinitionServiceClient, log port.Logger, tenantID, versionID uuid.UUID,
) (*port.CompiledWorkflow, error) {
	if store != nil {
		raw, err := store.Get(ctx, compiledPlanCacheKey(tenantID, versionID))
		if err != nil {
			log.Warn("compiled-plan cache read failed, falling back to Definitions", map[string]any{"error": err.Error()})
		} else if raw != "" {
			var compiled port.CompiledWorkflow
			if err := json.Unmarshal([]byte(raw), &compiled); err == nil && holdsMainPlan(&compiled) {
				return &compiled, nil
			}
			log.Warn("compiled-plan cache entry is not a compiled plan, falling back to Definitions", nil)
		}
	}
	return liveCompiledWorkflow(ctx, store, definitions, log, tenantID, versionID)
}

// holdsMainPlan tells a usable cache entry from one that is valid JSON but no compiled plan
// (null, {}): such an entry is a miss, so the next read replaces it instead of failing until it expires.
func holdsMainPlan(compiled *port.CompiledWorkflow) bool {
	var collab dsl.CompiledCollaboration
	if err := json.Unmarshal([]byte(compiled.CompiledPlanJSON), &collab); err != nil {
		return false
	}
	return findPlan(&collab, collab.MainPlan) != nil
}

func liveCompiledWorkflow(
	ctx context.Context, store port.CacheStore, definitions port.DefinitionServiceClient, log port.Logger, tenantID, versionID uuid.UUID,
) (*port.CompiledWorkflow, error) {
	compiled, err := definitions.GetCompiledWorkflow(ctx, tenantID, versionID)
	if err != nil {
		return nil, err
	}
	if store != nil {
		if raw, err := json.Marshal(compiled); err != nil {
			log.Warn("compiled-plan marshal for cache write-through failed", map[string]any{"error": err.Error()})
		} else if err := store.Set(ctx, compiledPlanCacheKey(tenantID, versionID), string(raw), compiledPlanCacheTTL); err != nil {
			log.Warn("compiled-plan cache write-through failed", map[string]any{"error": err.Error()})
		}
	}
	return compiled, nil
}

func (c *compiledPlanCache) mainPlan(ctx context.Context, tenantID, versionID uuid.UUID) (*dsl.CompiledPlan, error) {
	plan, err := c.readMainPlan(ctx, tenantID, versionID)
	if err != nil {
		c.log.Warn("compiled plan unreadable", map[string]any{"version_id": versionID, "error": err.Error()})
	}
	return plan, err
}

func (c *compiledPlanCache) readMainPlan(ctx context.Context, tenantID, versionID uuid.UUID) (*dsl.CompiledPlan, error) {
	if plan, ok := c.byVersion[versionID]; ok {
		observability.IncCounter(observability.WFCacheHitsTotal)
		return plan, nil
	}
	observability.IncCounter(observability.WFCacheMissesTotal)
	compiled, err := cachedCompiledWorkflow(ctx, c.store, c.definitions, c.log, tenantID, versionID)
	if err != nil {
		return nil, err
	}
	plan, err := mainPlanOfJSON(compiled.CompiledPlanJSON)
	if err != nil {
		return nil, err
	}
	c.byVersion[versionID] = plan
	return plan, nil
}

func requiredLevelForTask(plan *dsl.CompiledPlan, task *domain.Task) (string, bool) {
	for _, dept := range plan.Departments {
		if deptUUID(&dept) != task.DepartmentID {
			continue
		}
		for i := range dept.Stages {
			stage := &dept.Stages[i]
			if dsl.NodeKey(dept.ID, stage) == task.NodeKey {
				return stage.Role, true
			}
		}
	}
	return "", false
}

// decodeMainPlan reads the plan an instance start or a force-forward works from. A stored plan
// that does not decode, or names no main plan, is a broken version (ErrVersionInvalid) for the
// caller; the decoder's own text is logged, not returned.
func decodeMainPlan(log port.Logger, planJSON string) (*dsl.CompiledPlan, error) {
	plan, err := mainPlanOfJSON(planJSON)
	var noMain *noMainPlanError
	switch {
	case err == nil:
		return plan, nil
	case errors.Is(err, port.ErrVersionInvalid):
		return nil, err
	case errors.As(err, &noMain):
		return nil, fmt.Errorf("%w: %s", port.ErrVersionInvalid, err.Error())
	}
	log.Warn("stored compiled plan does not decode", map[string]any{"error": err.Error()})
	return nil, fmt.Errorf("%w: compiled plan is not valid JSON", port.ErrVersionInvalid)
}

type noMainPlanError struct{ name string }

func (e *noMainPlanError) Error() string {
	return fmt.Sprintf("main plan %q not found in compiled collaboration", e.name)
}

// mainPlanOfJSON decodes a stored plan and picks its main plan, with the raw decoder errors:
// the one place both readers of a stored plan share.
func mainPlanOfJSON(planJSON string) (*dsl.CompiledPlan, error) {
	collab, err := decodeCollaboration(planJSON)
	if err != nil {
		return nil, err
	}
	plan := findPlan(collab, collab.MainPlan)
	if plan == nil {
		return nil, &noMainPlanError{name: collab.MainPlan}
	}
	return plan, nil
}

func decodeCollaboration(planJSON string) (*dsl.CompiledCollaboration, error) {
	var collab dsl.CompiledCollaboration
	if err := json.Unmarshal([]byte(planJSON), &collab); err != nil {
		return nil, fmt.Errorf("unmarshal compiled plan: %w", err)
	}
	if collab.SchemaVersion != dsl.CurrentSchemaVersion {
		return nil, fmt.Errorf("%w: compiled plan has schema version %d, this service reads %d", port.ErrVersionInvalid, collab.SchemaVersion, dsl.CurrentSchemaVersion)
	}
	expanded, err := dsl.ExpandCalls(&collab, domain.MaxPlanStages)
	if err != nil {
		return nil, fmt.Errorf("%w: expand compiled plan: %w", port.ErrVersionInvalid, err)
	}
	return expanded, nil
}
