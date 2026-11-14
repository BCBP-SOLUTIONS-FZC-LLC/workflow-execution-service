package main

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/connectors"
)

type ingfixLogger struct{ warns []map[string]any }

func (*ingfixLogger) Debug(string, map[string]any) {}
func (*ingfixLogger) Info(string, map[string]any)  {}
func (l *ingfixLogger) Warn(_ string, fields map[string]any) {
	l.warns = append(l.warns, fields)
}
func (*ingfixLogger) Error(string, map[string]any) {}

func TestObserveFailures_ProviderLabelIsBoundedByTheRegistry(t *testing.T) {
	obstest.Init(t)
	const metric = "workflow_execution_service_connector_task_failures_total"
	decision := connectors.RetryDecision{Retry: true, Class: connectors.ClassTransient, Reason: "http 503", Rule: "transient"}

	for _, tc := range []struct {
		name          string
		connectorType string
		provider      any
		wantLabel     string
	}{
		{"a known provider of the type", "storage", "aws-s3", "aws-s3"},
		{"a known provider of another type", "storage", "sendgrid", "other"},
		{"free text a template author chose", "storage", "tenant-made-up-7731", "other"},
		{"absent provider", "storage", nil, "none"},
		{"empty provider", "storage", "", "none"},
		{"a type with no provider field", "rest-call", "aws-s3", "other"},
		{"a type the registry does not know", "unknown-type", "aws-s3", "other"},
		{"a provider that is not text", "storage", 42, "none"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			labels := map[string]string{"connector_type": tc.connectorType, "provider": tc.wantLabel, "class": "transient", "retried": "true"}
			before := obstest.Sum(t, metric, labels)
			input := map[string]any{}
			if tc.provider != nil {
				input["provider"] = tc.provider
			}
			job := dispatchJob{tenantID: uuid.New(), taskID: uuid.New(), connectorType: tc.connectorType}

			observeFailures(&ingfixLogger{}, job, input)(1, decision)

			assert.Equal(t, before+1, obstest.Sum(t, metric, labels))
		})
	}
}

func TestObserveFailures_LogKeepsTheRawProviderText(t *testing.T) {
	obstest.Init(t)
	log := &ingfixLogger{}
	job := dispatchJob{tenantID: uuid.New(), taskID: uuid.New(), connectorType: "storage"}

	observeFailures(log, job, map[string]any{"provider": "tenant-made-up-7731"})(1, connectors.RetryDecision{Class: connectors.ClassPermanent})

	require.Len(t, log.warns, 1)
	assert.Equal(t, "tenant-made-up-7731", log.warns[0]["provider"])
}
