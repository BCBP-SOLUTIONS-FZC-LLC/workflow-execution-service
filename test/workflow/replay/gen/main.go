// Command gen bootstraps a replay fixture: runs Execute for real against a
// local Temporal dev server, then the temporal CLI exports its history.
// Never run by CI — see test/workflow/replay/README.md for the full command.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

func main() {
	c, err := client.Dial(client.Options{HostPort: "localhost:17233"})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	taskQueue := "replay-fixture-gen-tq"
	w := worker.New(c, taskQueue, worker.Options{})
	w.RegisterWorkflow(wfengine.Execute)

	collab := dsl.CompiledCollaboration{
		MainPlan:      "main",
		SchemaVersion: dsl.CurrentSchemaVersion,
		Plans: []*dsl.CompiledPlan{{
			Name: "main",
			Departments: []dsl.DepartmentDef{
				{ID: "sales", Stages: []dsl.StageDef{{Type: "approve", Activity: "approve_order", Role: "sales_rep"}}},
			},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"sales"}}}},
		}},
	}

	w.RegisterActivityWithOptions(
		func(_ context.Context, _ port.GetCompiledPlanInput) (port.GetCompiledPlanOutput, error) {
			return port.GetCompiledPlanOutput{Collaboration: collab}, nil
		},
		activity.RegisterOptions{Name: port.ActivityGetCompiledPlan},
	)
	w.RegisterActivityWithOptions(
		func(_ context.Context, in port.CreateTaskInput) (port.CreateTaskOutput, error) {
			return port.CreateTaskOutput{TaskID: string(in.NodeKey)}, nil
		},
		activity.RegisterOptions{Name: port.ActivityCreateTask},
	)
	w.RegisterActivityWithOptions(
		func(_ context.Context, _ port.UpdateInstanceNodesInput) error { return nil },
		activity.RegisterOptions{Name: port.ActivityUpdateInstanceNodes},
	)
	w.RegisterActivityWithOptions(
		func(_ context.Context, _ port.CompleteAssignmentInput) (port.CompleteAssignmentOutput, error) {
			return port.CompleteAssignmentOutput{AllDone: true}, nil
		},
		activity.RegisterOptions{Name: port.ActivityCompleteAssignment},
	)
	w.RegisterActivityWithOptions(
		func(_ context.Context, _ port.UpdateInstanceStatusInput) error { return nil },
		activity.RegisterOptions{Name: port.ActivityUpdateInstanceStatus},
	)
	w.RegisterActivityWithOptions(
		func(_ context.Context, _ port.RecordForceRouteInput) error { return nil },
		activity.RegisterOptions{Name: port.ActivityRecordForceRoute},
	)
	w.RegisterActivityWithOptions(
		func(_ context.Context, _ port.RecordSLAWarningInput) error { return nil },
		activity.RegisterOptions{Name: port.ActivityRecordSLAWarning},
	)
	w.RegisterActivityWithOptions(
		func(_ context.Context, _ port.RecordSLABreachInput) error { return nil },
		activity.RegisterOptions{Name: port.ActivityRecordSLABreach},
	)
	w.RegisterActivityWithOptions(
		func(_ context.Context, _ port.DeferTaskInput) (port.DeferTaskOutput, error) {
			return port.DeferTaskOutput{}, nil
		},
		activity.RegisterOptions{Name: port.ActivityDeferTask},
	)

	if err := w.Start(); err != nil {
		log.Fatal(err)
	}
	defer w.Stop()

	wfID := "initial-interpreter-fixture"
	run, err := c.ExecuteWorkflow(context.Background(), client.StartWorkflowOptions{
		ID: wfID, TaskQueue: taskQueue,
	}, wfengine.Execute, wfengine.ExecuteInput{
		TenantID: "tenant-1", InstanceID: "instance-1", VersionID: "version-1",
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("started run", run.GetRunID())

	time.Sleep(1500 * time.Millisecond)
	if err := c.SignalWorkflow(context.Background(), wfID, "", "stage-transition:instance-1", struct {
		DeptID        string
		ToStage       string
		NodeID        string
		UserID        string
		ResultJSON    string
		RecordVersion int64
	}{DeptID: "sales", ToStage: "approve", ResultJSON: `{"decision":"approved"}`, RecordVersion: 1}); err != nil {
		log.Fatal(err)
	}

	var out wfengine.ExecuteOutput
	if err := run.Get(context.Background(), &out); err != nil {
		log.Fatal(err)
	}
	fmt.Println("workflow completed with status:", out.Status)
}
