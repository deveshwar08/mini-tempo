package core_test

import (
	"context"
	"errors"
	"testing"

	"mini-tempo/pkg/core"
	"mini-tempo/pkg/storage"
)

func TestDeterministicReplay(t *testing.T) {
	instanceID := "test_wf_1"
	store := storage.NewMemoryEventStore()
	_ = store.CreateInstance(context.Background(), instanceID, "TestWorkflow")

	executionCount := 0

	// Define an activity that tracks how many times it was executed
	myActivity := func() (interface{}, error) {
		executionCount++
		return "result", nil
	}

	// Define a simple workflow
	runWorkflow := func(wfCtx *core.WorkflowContext, crashAfterFirst bool) error {
		var res string
		err := wfCtx.ExecuteActivity(context.Background(), "myActivity", myActivity, &res)
		if err != nil {
			return err
		}
		if res != "result" {
			return errors.New("unexpected result")
		}

		if crashAfterFirst {
			return errors.New("simulated crash")
		}

		err = wfCtx.ExecuteActivity(context.Background(), "myActivity2", myActivity, nil)
		return err
	}

	// RUN 1: Execute first activity, then crash
	ctx1 := core.NewWorkflowContext(instanceID, store, nil)
	err := runWorkflow(ctx1, true)
	if err == nil || err.Error() != "simulated crash" {
		t.Fatalf("expected simulated crash, got: %v", err)
	}

	if executionCount != 1 {
		t.Fatalf("expected 1 execution, got %d", executionCount)
	}

	// RUN 2: Rehydrate and complete
	events, _ := store.GetEvents(context.Background(), instanceID)
	ctx2 := core.NewWorkflowContext(instanceID, store, events)

	err = runWorkflow(ctx2, false)
	if err != nil {
		t.Fatalf("unexpected error during replay: %v", err)
	}

	// Execution count should be 2 (first activity was skipped due to replay, second activity executed)
	if executionCount != 2 {
		t.Fatalf("expected execution count to be 2 due to replay skipping Act1, got %d", executionCount)
	}
}
