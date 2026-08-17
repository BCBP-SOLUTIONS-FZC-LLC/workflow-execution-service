package valkeystream_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkeystream"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

type fakePublisher struct {
	streamKey string
	fields    map[string]string
	err       error
}

func (f *fakePublisher) Publish(_ context.Context, streamKey string, fields map[string]string) (string, error) {
	f.streamKey = streamKey
	f.fields = fields
	if f.err != nil {
		return "", f.err
	}
	return "1-0", nil
}

func TestEventPublisher_PublishTaskCreated_FlattensFields(t *testing.T) {
	t.Parallel()

	fake := &fakePublisher{}
	pub := valkeystream.NewEventPublisher(fake, "connector-tasks")

	event := port.ConnectorTaskCreatedEvent{
		EventID:        uuid.New(),
		TenantID:       uuid.New(),
		InstanceID:     uuid.New(),
		TaskID:         uuid.New(),
		NodeKey:        "dept-1/nodeA",
		DepartmentID:   uuid.New(),
		ConnectorType:  "storage",
		ResolvedInputs: map[string]any{"bucket": "b1"},
		OutputMapping:  []dsl.IOVar{{Source: "contentRef", Target: "docRef"}},
	}

	require.NoError(t, pub.PublishTaskCreated(context.Background(), event))

	assert.Equal(t, "connector-tasks", fake.streamKey)
	assert.Equal(t, event.TaskID.String(), fake.fields["task_id"])
	assert.Equal(t, "storage", fake.fields["connector_type"])

	var resolvedInputs map[string]any
	require.NoError(t, json.Unmarshal([]byte(fake.fields["resolved_inputs"]), &resolvedInputs))
	assert.Equal(t, "b1", resolvedInputs["bucket"])

	var outputMapping []dsl.IOVar
	require.NoError(t, json.Unmarshal([]byte(fake.fields["output_mapping"]), &outputMapping))
	require.Len(t, outputMapping, 1)
	assert.Equal(t, "contentRef", outputMapping[0].Source)
}

func TestEventPublisher_PublishTaskCreated_PropagatesError(t *testing.T) {
	t.Parallel()

	fake := &fakePublisher{err: errors.New("boom")}
	pub := valkeystream.NewEventPublisher(fake, "connector-tasks")

	err := pub.PublishTaskCreated(context.Background(), port.ConnectorTaskCreatedEvent{EventID: uuid.New()})
	require.Error(t, err)
}
