// Package eventbus holds outbound-event wire plumbing beyond the plain Glue
// codec: ValidatingCodec adds a runtime JSON-Schema safety net in front of it.
package eventbus

import (
	"context"
	"encoding/json"
	"fmt"

	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/eventschema"
)

// ValidatingCodec wraps an inner port.GlueCodec and validates each payload
// against its JSON Schema before delegating to it. A payload that doesn't
// conform to its schema, or an event type with no registered schema, is
// rejected before it ever reaches the inner codec - preventing malformed or
// unrecognized events from corrupting the wf.workflow.events stream and all
// downstream consumers. Ported from iam-user-profile's
// internal/adapter/outbound/eventbus.ValidatingCodec, adapted to
// port.GlueCodec's 2-value Encode signature.
type ValidatingCodec struct {
	inner   port.GlueCodec
	schemas map[string]*jsonschema.Schema
}

type schemaEntry struct {
	name string
	src  []byte
}

// defaultSchemaEntries are the 18 event schemas embedded at build time
// (internal/eventschema). Every wire type in domain's Event* consts must
// have an entry here - Encode fails closed for any type that doesn't.
var defaultSchemaEntries = []schemaEntry{
	{domain.EventWorkflowInstanceStarted, eventschema.WorkflowInstanceStarted},
	{domain.EventWorkflowInstancePaused, eventschema.WorkflowInstancePaused},
	{domain.EventWorkflowInstanceResumed, eventschema.WorkflowInstanceResumed},
	{domain.EventWorkflowInstanceCancelled, eventschema.WorkflowInstanceCancelled},
	{domain.EventWorkflowInstanceTerminated, eventschema.WorkflowInstanceTerminated},
	{domain.EventWorkflowInstanceDegraded, eventschema.WorkflowInstanceDegraded},
	{domain.EventWorkflowInstanceFailed, eventschema.WorkflowInstanceFailed},
	{domain.EventWorkflowInstanceFinished, eventschema.WorkflowInstanceFinished},
	{domain.EventWorkflowInstanceForceRouted, eventschema.WorkflowInstanceForceRouted},
	{domain.EventWorkflowTaskCreated, eventschema.WorkflowTaskCreated},
	{domain.EventWorkflowTaskClaimed, eventschema.WorkflowTaskClaimed},
	{domain.EventWorkflowTaskCompleted, eventschema.WorkflowTaskCompleted},
	{domain.EventWorkflowTaskDeferred, eventschema.WorkflowTaskDeferred},
	{domain.EventWorkflowTaskReassigned, eventschema.WorkflowTaskReassigned},
	{domain.EventWorkflowTaskSuperseded, eventschema.WorkflowTaskSuperseded},
	{domain.EventWorkflowTaskFailed, eventschema.WorkflowTaskFailed},
	{domain.EventWorkflowTaskSLAWarning, eventschema.WorkflowTaskSLAWarning},
	{domain.EventWorkflowTaskSLABreached, eventschema.WorkflowTaskSLABreached},
}

// NewValidatingCodec compiles the 18 embedded event schemas and returns a
// codec that validates payloads against them before delegating to inner.
func NewValidatingCodec(inner port.GlueCodec) (*ValidatingCodec, error) {
	return newValidatingCodecFromEntries(inner, defaultSchemaEntries)
}

func newValidatingCodecFromEntries(inner port.GlueCodec, entries []schemaEntry) (*ValidatingCodec, error) {
	c := jsonschema.NewCompiler()
	compiled := make(map[string]*jsonschema.Schema, len(entries))
	for _, e := range entries {
		var v any
		if err := json.Unmarshal(e.src, &v); err != nil {
			return nil, fmt.Errorf("load event schema %q: %w", e.name, err)
		}
		url := "execution-event-schema:" + e.name
		if err := c.AddResource(url, v); err != nil {
			return nil, fmt.Errorf("load event schema %q: %w", e.name, err)
		}
		sch, err := c.Compile(url)
		if err != nil {
			return nil, fmt.Errorf("compile event schema %q: %w", e.name, err)
		}
		compiled[e.name] = sch
	}
	return &ValidatingCodec{inner: inner, schemas: compiled}, nil
}

func (v *ValidatingCodec) Encode(ctx context.Context, schemaName string, payload []byte) ([]byte, error) {
	sch, ok := v.schemas[schemaName]
	if !ok {
		return nil, fmt.Errorf("no schema registered for event type %q - add it to defaultSchemaEntries", schemaName)
	}
	var instance any
	if err := json.Unmarshal(payload, &instance); err != nil {
		return nil, fmt.Errorf("schema validation: unmarshal %q payload: %w", schemaName, err)
	}
	if err := sch.Validate(instance); err != nil {
		return nil, fmt.Errorf("event payload violates schema %q: %w", schemaName, err)
	}
	return v.inner.Encode(ctx, schemaName, payload)
}
