package main

import (
	"context"
	"errors"
	"log"
	"time"

	"mini-tempo/pkg/core"
	"mini-tempo/pkg/storage"
	"mini-tempo/pkg/worker"
)

// Activity 1: Simulates external payment gateway
func ChargeCard() (interface{}, error) {
	time.Sleep(200 * time.Millisecond)
	return "tx_ch_908123", nil
}

// Activity 2: Simulates database provisioning
func ProvisionAccount() (interface{}, error) {
	time.Sleep(200 * time.Millisecond)
	return "acc_usr_44321", nil
}

// Durable Workflow Function
func UserOnboardingWorkflow(ctx *core.WorkflowContext, simulateCrash bool) error {
	var txID string
	if err := ctx.ExecuteActivity(context.Background(), "ChargeCard", ChargeCard, &txID); err != nil {
		return err
	}
	log.Printf("  [STATE] Transaction recorded: %s", txID)

	if simulateCrash {
		log.Println("💥 [CRASH] Fatal host interruption before Step 2!")
		return errors.New("host crashed unexpectedly")
	}

	var accID string
	if err := ctx.ExecuteActivity(context.Background(), "ProvisionAccount", ProvisionAccount, &accID); err != nil {
		return err
	}
	log.Printf("  [STATE] Account provisioned: %s", accID)

	log.Printf("✅ [COMPLETE] Workflow finished successfully.")
	return nil
}

func main() {
	// Initialize the persistent store (in-memory for this example)
	store := storage.NewMemoryEventStore()
	instanceID := "wf_onboarding_123"
	workflowName := "UserOnboarding"
	_ = store.CreateInstance(context.Background(), instanceID, workflowName)

	// Create and register the worker
	w := worker.NewWorker(store)
	w.RegisterWorkflow(workflowName, UserOnboardingWorkflow)

	log.Println("=== RUN 1: UNHANDLED INTERRUPTION ===")
	// Start workflow (simulating crash)
	_ = w.ResumeWorkflow(context.Background(), instanceID, workflowName, true)

	log.Println("=== RUN 2: RESTART & REPLAY FROM LOG ===")
	// In a real system, the process restarts here. The worker picks up the task
	// and automatically rehydrates history from the store.
	if err := w.ResumeWorkflow(context.Background(), instanceID, workflowName, false); err != nil {
		log.Fatalf("Recovery failed: %v", err)
	}
}
