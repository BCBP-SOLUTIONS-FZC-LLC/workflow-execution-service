package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
)

// completionClient calls execution_service's own
// POST /api/v1/internal/connector-tasks/:id/{complete,fail} endpoints — the one
// place this binary reaches back into the workflow, since it never touches
// the Temporal SDK directly (LLD workflow_connectors.md §6.1 Decision #2).
type completionClient struct {
	addr   string
	token  string
	actors map[uuid.UUID]uuid.UUID
	http   *http.Client
}

func newCompletionClient(addr, token string, actors map[uuid.UUID]uuid.UUID, timeout time.Duration) *completionClient {
	return &completionClient{
		addr: strings.TrimRight(addr, "/"), token: token, actors: actors,
		http: &http.Client{Timeout: timeout},
	}
}

// One worker serves every tenant, so the actor is resolved per callback rather
// than held once. An unconfigured tenant fails the callback instead of
// borrowing another tenant's principal, which would attribute its work to an
// identity that does not exist in its own realm.
func (c *completionClient) actorFor(tenantID uuid.UUID) (uuid.UUID, error) {
	actor, ok := c.actors[tenantID]
	if !ok {
		return uuid.Nil, fmt.Errorf("no automation principal configured for tenant %s", tenantID)
	}
	return actor, nil
}

type completeRequest struct {
	TenantID    uuid.UUID      `json:"tenant_id"`
	ActorUserID uuid.UUID      `json:"actor_user_id"`
	Output      map[string]any `json:"output"`
}

type failRequest struct {
	TenantID    uuid.UUID `json:"tenant_id"`
	ActorUserID uuid.UUID `json:"actor_user_id"`
	ErrorClass  string    `json:"error_class"`
}

func (c *completionClient) Complete(ctx context.Context, tenantID, taskID uuid.UUID, output map[string]any) error {
	actor, err := c.actorFor(tenantID)
	if err != nil {
		return err
	}
	body, err := json.Marshal(completeRequest{TenantID: tenantID, ActorUserID: actor, Output: output})
	if err != nil {
		return fmt.Errorf("encode complete request: %w", err)
	}
	return c.post(ctx, fmt.Sprintf("/api/v1/internal/connector-tasks/%s/complete", taskID), body)
}

func (c *completionClient) Fail(ctx context.Context, tenantID, taskID uuid.UUID, errorClass string) error {
	actor, err := c.actorFor(tenantID)
	if err != nil {
		return err
	}
	body, err := json.Marshal(failRequest{TenantID: tenantID, ActorUserID: actor, ErrorClass: errorClass})
	if err != nil {
		return fmt.Errorf("encode fail request: %w", err)
	}
	return c.post(ctx, fmt.Sprintf("/api/v1/internal/connector-tasks/%s/fail", taskID), body)
}

// parseActorUserIDs reads "<tenant-uuid>=<subject-uuid>,..." — the stopgap
// standing in for the identity subsystem's own per-tenant lookup.
func parseActorUserIDs(raw string) (map[uuid.UUID]uuid.UUID, error) {
	actors := make(map[uuid.UUID]uuid.UUID)
	for _, pair := range strings.Split(raw, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		tenant, subject, ok := strings.Cut(pair, "=")
		if !ok {
			return nil, fmt.Errorf("entry %q is not <tenant-uuid>=<subject-uuid>", pair)
		}
		tenantID, err := uuid.Parse(strings.TrimSpace(tenant))
		if err != nil {
			return nil, fmt.Errorf("parse tenant in %q: %w", pair, err)
		}
		subjectID, err := uuid.Parse(strings.TrimSpace(subject))
		if err != nil {
			return nil, fmt.Errorf("parse subject in %q: %w", pair, err)
		}
		actors[tenantID] = subjectID
	}
	if len(actors) == 0 {
		return nil, fmt.Errorf("no entries")
	}
	return actors, nil
}

func (c *completionClient) post(ctx context.Context, path string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.addr+path, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-internal-token", c.token)

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("call %s: %w", path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("call %s: status %d", path, resp.StatusCode)
	}
	return nil
}
