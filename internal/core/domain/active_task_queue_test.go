package domain

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// The name is the Commercial catalog's feature flag; a rename here silently turns every tenant's grant off.
func TestFeatureDedicatedWorkflowQueue_IsTheCatalogName(t *testing.T) {
	assert.Equal(t, "dedicated_workflow_queue", FeatureDedicatedWorkflowQueue)
}
