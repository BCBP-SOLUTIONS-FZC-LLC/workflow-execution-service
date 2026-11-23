package eventbus

import (
	"testing"

	"github.com/google/uuid"
)

func TestSchemaValidator_TaskCreatedAcceptsNullForItsNullableOptionalFields(t *testing.T) {
	core := w10aCore()
	payload := map[string]any{
		"workflow_instance_id": core.WorkflowInstanceID, "subject_id": core.SubjectID, "business_key": core.BusinessKey,
		"workflow_version_id": core.WorkflowVersionID, "task_id": uuid.New(), "node_key": "ops/Step",
		"department_id": uuid.New(), "assignee_user_ids": []uuid.UUID{}, "stage_type": "prep",
		"due_at": nil, "follow_up_at": nil, "connector_type": nil, "resolved_inputs": nil, "output_mapping": nil,
	}

	if err := w10aValidate(t, "WorkflowTaskCreated", payload); err != nil {
		t.Fatalf("null optional fields were refused: %v", err)
	}
}

func TestSchemaValidator_TaskCreatedRefusesNullForARequiredField(t *testing.T) {
	core := w10aCore()
	payload := map[string]any{
		"workflow_instance_id": core.WorkflowInstanceID, "subject_id": core.SubjectID, "business_key": core.BusinessKey,
		"workflow_version_id": core.WorkflowVersionID, "task_id": uuid.New(), "node_key": nil,
		"department_id": uuid.New(), "assignee_user_ids": []uuid.UUID{}, "stage_type": "prep",
	}

	if err := w10aValidate(t, "WorkflowTaskCreated", payload); err == nil {
		t.Fatal("a null node_key was accepted")
	}
}
