// Package eventschema embeds the 18 outbound event JSON Schemas (LLD §6.8)
// consumed by internal/adapter/outbound/eventbus.SchemaValidator and
// extracted by the Makefile's schema-* targets for platform-schemagov.
package eventschema

import _ "embed"

//go:embed workflow_instance_started.json
var WorkflowInstanceStarted []byte

//go:embed workflow_instance_paused.json
var WorkflowInstancePaused []byte

//go:embed workflow_instance_resumed.json
var WorkflowInstanceResumed []byte

//go:embed workflow_instance_cancelled.json
var WorkflowInstanceCancelled []byte

//go:embed workflow_instance_terminated.json
var WorkflowInstanceTerminated []byte

//go:embed workflow_instance_degraded.json
var WorkflowInstanceDegraded []byte

//go:embed workflow_instance_failed.json
var WorkflowInstanceFailed []byte

//go:embed workflow_instance_finished.json
var WorkflowInstanceFinished []byte

//go:embed workflow_instance_force_routed.json
var WorkflowInstanceForceRouted []byte

//go:embed workflow_task_created.json
var WorkflowTaskCreated []byte

//go:embed workflow_task_claimed.json
var WorkflowTaskClaimed []byte

//go:embed workflow_task_completed.json
var WorkflowTaskCompleted []byte

//go:embed workflow_task_deferred.json
var WorkflowTaskDeferred []byte

//go:embed workflow_task_reassigned.json
var WorkflowTaskReassigned []byte

//go:embed workflow_task_superseded.json
var WorkflowTaskSuperseded []byte

//go:embed workflow_task_failed.json
var WorkflowTaskFailed []byte

//go:embed workflow_task_sla_warning.json
var WorkflowTaskSLAWarning []byte

//go:embed workflow_task_sla_breached.json
var WorkflowTaskSLABreached []byte
