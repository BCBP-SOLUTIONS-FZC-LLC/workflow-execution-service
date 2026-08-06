// Package eventbus holds outbound-event wire plumbing: SchemaValidator is a
// runtime JSON-Schema safety net applied before an event payload is
// embedded in an outbound envelope.
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

// SchemaValidator validates each outbound event payload against its JSON
// Schema. A payload that doesn't conform to its schema, or an event type
// with no registered schema, is rejected before it ever reaches
// buildEnvelope - preventing malformed or unrecognized events from
// corrupting the wf.workflow.events stream and all downstream consumers.
// Ported from iam-user-profile's internal/adapter/outbound/eventbus.ValidatingCodec,
// adapted to port.EventValidator's validate-only contract now that
// wire-format encoding is handled separately by a platform-events
// events.Codec at SNS-publish time.
type SchemaValidator struct {
	schemas map[string]*jsonschema.Schema
}

var _ port.EventValidator = (*SchemaValidator)(nil)

type schemaEntry struct {
	name string
	src  []byte
}

// defaultSchemaEntries are the 18 event schemas embedded at build time
// (internal/eventschema). Every wire type in domain's Event* consts must
// have an entry here - Validate fails closed for any type that doesn't.
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

// NewSchemaValidator compiles the 18 embedded event schemas and returns a
// validator that checks payloads against them.
func NewSchemaValidator() (*SchemaValidator, error) {
	return newSchemaValidatorFromEntries(defaultSchemaEntries)
}

func newSchemaValidatorFromEntries(entries []schemaEntry) (*SchemaValidator, error) {
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
	return &SchemaValidator{schemas: compiled}, nil
}

func (v *SchemaValidator) Validate(_ context.Context, eventType string, payload json.RawMessage) error {
	sch, ok := v.schemas[eventType]
	if !ok {
		return fmt.Errorf("no schema registered for event type %q - add it to defaultSchemaEntries", eventType)
	}
	var instance any
	if err := json.Unmarshal(payload, &instance); err != nil {
		return fmt.Errorf("schema validation: unmarshal %q payload: %w", eventType, err)
	}
	if err := sch.Validate(instance); err != nil {
		return fmt.Errorf("event payload violates schema %q: %w", eventType, err)
	}
	return nil
}
