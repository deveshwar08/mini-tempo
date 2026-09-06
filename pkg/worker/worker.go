package worker

import (
	"context"
	"fmt"
	"log"

	"mini-tempo/pkg/core"
	"mini-tempo/pkg/storage"
)

// WorkflowFunc defines the signature for a durable workflow
type WorkflowFunc func(ctx *core.WorkflowContext, simulateCrash bool) error

type Worker struct {
	store    storage.EventStore
	registry map[string]WorkflowFunc
}

func NewWorker(store storage.EventStore) *Worker {
	return &Worker{
		store:    store,
		registry: make(map[string]WorkflowFunc),
	}
}

// RegisterWorkflow maps a workflow name to its function so the worker knows what to run
func (w *Worker) RegisterWorkflow(name string, fn WorkflowFunc) {
	w.registry[name] = fn
}

// ResumeWorkflow orchestrates fetching the workflow state and executing it (Rehydration)
func (w *Worker) ResumeWorkflow(ctx context.Context, instanceID string, workflowName string, simulateCrash bool) error {
	fn, exists := w.registry[workflowName]
	if !exists {
		return fmt.Errorf("workflow '%s' not registered", workflowName)
	}

	// 1. Rehydration: The worker is responsible for fetching the history
	events, err := w.store.GetEvents(ctx, instanceID)
	if err != nil {
		return fmt.Errorf("failed to fetch events for instance %s: %w", instanceID, err)
	}

	// 2. Instantiate Context: Give the history to the context
	wfCtx := core.NewWorkflowContext(instanceID, w.store, events)

	// 3. Execution: Let the deterministic logic fast-forward using the hydrated context
	log.Printf("▶️ Starting/Resuming workflow for instance: %s", instanceID)
	if err := fn(wfCtx, simulateCrash); err != nil {
		return err
	}

	// 4. Mark as completed once the function returns without error
	if err := w.store.UpdateInstanceStatus(ctx, instanceID, "COMPLETED"); err != nil {
		return fmt.Errorf("failed to update status for instance %s: %w", instanceID, err)
	}

	log.Printf("✅ Workflow for instance %s has been resumed and completed", instanceID)
	return nil
}
