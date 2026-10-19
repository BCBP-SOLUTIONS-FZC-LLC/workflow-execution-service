package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

type stageTransition struct {
	DeptID        string
	ToStage       string
	NodeID        string
	UserID        string
	ResultJSON    string
	RecordVersion int64
	VisitCount    int64
}

type adminSignal struct {
	AdminUserID   string
	Reason        string
	Initiator     string
	TargetDeptID  string
	TargetNodeKey string
	RecordVersion int64
	FromDefer     bool
}

func main() {
	scenario := "initial-interpreter"
	if len(os.Args) > 1 {
		scenario = os.Args[1]
	}

	c, err := client.Dial(client.Options{HostPort: "localhost:17233"})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	switch scenario {
	case "initial-interpreter":
		runInitialInterpreter(c)
	case "degraded-pause":
		runDegradedPause(c, "degraded-pause-fixture")
	case "degraded-pause-held":
		runDegradedPause(c, "degraded-pause-held-fixture")
	default:
		log.Fatalf("unknown scenario %q", scenario)
	}
}

func startWorker(c client.Client, taskQueue string, collab dsl.CompiledCollaboration, createTask func(port.CreateTaskInput) (port.CreateTaskOutput, error)) worker.Worker {
	w := worker.New(c, taskQueue, worker.Options{})
	w.RegisterWorkflow(wfengine.Execute)

	w.RegisterActivityWithOptions(
		func(_ context.Context, _ port.GetCompiledPlanInput) (port.GetCompiledPlanOutput, error) {
			return port.GetCompiledPlanOutput{Collaboration: collab}, nil
		},
		activity.RegisterOptions{Name: port.ActivityGetCompiledPlan},
	)
	w.RegisterActivityWithOptions(
		func(_ context.Context, in port.CreateTaskInput) (port.CreateTaskOutput, error) {
			return createTask(in)
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
		func(_ context.Context, _ port.DeferTaskInput) error { return nil },
		activity.RegisterOptions{Name: port.ActivityDeferTask},
	)
	w.RegisterActivityWithOptions(
		func(_ context.Context, _ port.UpdateTaskStatusInput) error { return nil },
		activity.RegisterOptions{Name: port.ActivityUpdateTaskStatus},
	)
	w.RegisterActivityWithOptions(
		func(_ context.Context, _ port.PauseInstanceInput) error { return nil },
		activity.RegisterOptions{Name: port.ActivityPauseInstance},
	)
	w.RegisterActivityWithOptions(
		func(_ context.Context, _ port.ResumeInstanceInput) error { return nil },
		activity.RegisterOptions{Name: port.ActivityResumeInstance},
	)

	if err := w.Start(); err != nil {
		log.Fatal(err)
	}
	return w
}

func runInitialInterpreter(c client.Client) {
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

	taskQueue := "replay-fixture-gen-tq"
	w := startWorker(c, taskQueue, collab, func(in port.CreateTaskInput) (port.CreateTaskOutput, error) {
		return port.CreateTaskOutput{TaskID: string(in.NodeKey)}, nil
	})
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
	send(c, wfID, "stage-transition:instance-1", stageTransition{DeptID: "sales", ToStage: "approve", ResultJSON: `{"decision":"approved"}`, RecordVersion: 1})

	var out wfengine.ExecuteOutput
	if err := run.Get(context.Background(), &out); err != nil {
		log.Fatal(err)
	}
	fmt.Println("workflow completed with status:", out.Status)
}

func runDegradedPause(c client.Client, wfID string) {
	collab := dsl.CompiledCollaboration{
		MainPlan:      "main",
		SchemaVersion: dsl.CurrentSchemaVersion,
		Plans: []*dsl.CompiledPlan{{
			Name: "main",
			Departments: []dsl.DepartmentDef{
				{ID: "warehouse", IAMDepartmentID: "11111111-1111-1111-1111-111111111111", Stages: []dsl.StageDef{{Type: "approve", NodeID: "pack", Role: "reviewer"}}},
				{ID: "billing", IAMDepartmentID: "22222222-2222-2222-2222-222222222222", Stages: []dsl.StageDef{{Type: "approve", NodeID: "charge", Role: "reviewer"}}},
			},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Parallel: []dsl.ParallelBranch{
				{DeptID: "warehouse", Steps: []dsl.ExecutionStep{{Sequential: []string{"warehouse"}}}},
				{DeptID: "billing", Steps: []dsl.ExecutionStep{{Sequential: []string{"billing"}}}},
			}}}},
		}},
	}

	taskQueue := "replay-fixture-gen-degraded-tq"
	w := startWorker(c, taskQueue, collab, func(in port.CreateTaskInput) (port.CreateTaskOutput, error) {
		if in.NodeKey == "billing/charge" {
			return port.CreateTaskOutput{}, temporal.NewApplicationError("simulated downstream failure", "ValidationError")
		}
		return port.CreateTaskOutput{TaskID: string(in.NodeKey)}, nil
	})
	defer w.Stop()

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
	send(c, wfID, "stage-transition:instance-1", stageTransition{DeptID: "warehouse", NodeID: "pack", ResultJSON: `{"decision":"approved"}`, RecordVersion: 1, VisitCount: 1})

	time.Sleep(1500 * time.Millisecond)
	send(c, wfID, "instance-pause:instance-1", adminSignal{Initiator: "tenant_state", RecordVersion: 1})

	time.Sleep(1500 * time.Millisecond)
	send(c, wfID, "instance-force-forward:instance-1", adminSignal{
		AdminUserID: "33333333-3333-3333-3333-333333333333", Initiator: "admin",
		TargetDeptID: "billing", TargetNodeKey: "billing/charge", RecordVersion: 1,
	})

	var out wfengine.ExecuteOutput
	if err := run.Get(context.Background(), &out); err != nil {
		log.Fatal(err)
	}
	fmt.Println("workflow completed with status:", out.Status)
}

func send(c client.Client, wfID, channel string, payload any) {
	if err := c.SignalWorkflow(context.Background(), wfID, "", channel, payload); err != nil {
		log.Fatal(err)
	}
}
