package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"mini-tempo/pkg/core"
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
func UserOnboardingWorkflow(ctx *WorkflowContext, simulateCrash bool) error {
func UserOnboardingWorkflow(ctx *core.WorkflowContext, simulateCrash bool) error {
	var txID string
	if err := ctx.ExecuteActivity("ChargeCard", ChargeCard, &txID); err != nil {
		return err
	}
	log.Printf("  [STATE] Transaction recorded: %s", txID)

	if simulateCrash {
		log.Println("💥 [CRASH] Fatal host interruption before Step 2!")
		return errors.New("host crashed unexpectedly")
	}

	var accID string
	if err := ctx.ExecuteActivity("ProvisionAccount", ProvisionAccount, &accID); err != nil {
		return err
	}
	log.Printf("  [STATE] Account provisioned: %s", accID)

	log.Printf("✅ [COMPLETE] Workflow finished successfully.")
	return nil
}

func main() {
	var persistentStore []Event
	var persistentStore []core.Event

	log.Println("=== RUN 1: UNHANDLED INTERRUPTION ===")
	ctx1 := &WorkflowContext{History: persistentStore}
	ctx1 := &core.WorkflowContext{History: persistentStore}
	_ = UserOnboardingWorkflow(ctx1, true)

	// Persist the append-only event log
	persistentStore = ctx1.History

	log.Println("=== RUN 2: RESTART & REPLAY FROM LOG ===")
	ctx2 := &WorkflowContext{History: persistentStore}
	ctx2 := &core.WorkflowContext{History: persistentStore}
	if err := UserOnboardingWorkflow(ctx2, false); err != nil {
		log.Fatalf("Recovery failed: %v", err)
	}
}