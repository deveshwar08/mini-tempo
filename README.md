# ⏳ mini-tempo

A lightweight, crash-resilient workflow orchestration engine written in Go, implementing **Event Sourcing** and **Deterministic Replay**.

`mini-tempo` transforms standard, procedural Go code into durable workflows. If a workflow execution crashes, halts, or is interrupted mid-flight, it replays from line 1, retrieves cached outputs for completed side effects (Activities), and resumes execution without re-executing completed operations.

---

## Table of Contents

- [The Mental Model](#the-mental-model)
- [Architecture](#architecture)
- [Project Layout](#project-layout)
- [Working Prototype (Self-Contained)](#working-prototype-self-contained)
- [Core Abstractions](#core-abstractions)
- [Implementation Roadmap](#implementation-roadmap)
- [Contributing & Verification](#contributing--verification)
- [License](#license)

---

## The Mental Model

In standard software, process termination during a multi-step routine leaves the system in an indeterminate state:

```go
func Onboarding(email string) {
    txID := ChargeCard(email, 100)      // Step 1: Stripe API
    accID := ProvisionAccount(email)    // Step 2: Database Write
    SendEmail(email, accID)             // Step 3: Mailer API
}
```

If the host machine reboots or panics during **Step 2**:
- Rerunning the routine re-charges the customer.
- Failing to rerun the routine leaves an unprovisioned account with a billed card.

### How `mini-tempo` Solves This

1. **Deterministic Workflow Functions:** Orchestration logic is written as standard procedural Go code. It must be strictly deterministic (no unseeded randomness, raw clock calls, or unmonitored goroutines).
2. **Activity Encapsulation:** External mutations and network calls are wrapped as Activities.
3. **Deterministic Replay Loop:** Upon process restart, the workflow executes from line 1. For every activity previously completed, `mini-tempo` fetches the cached result from the append-only history log, assigns it to the target variable, and skips execution.

---

## Architecture

```text
+-------------------------------------------------------------+
|                      Workflow Function                      |
|           (Imperative Go logic: loops, branches, calls)     |
+------------------------------+------------------------------+
                               | ctx.ExecuteActivity()
                               v
+-------------------------------------------------------------+
|                      WorkflowContext                        |
|   +-----------------------------------------------------+   |
|   |  Replay Check: Event present in History?            |   |
|   |  - YES -> Unmarshal cached payload & return         |   |
|   |  - NO  -> Run activity, record event, advance cursor|   |
|   +-----------------------------------------------------+   |
+------------------------------+------------------------------+
                               |
                               v
+-------------------------------------------------------------+
|               Append-Only Event Store (WAL)                 |
|   +-----------------------------------------------------+   |
|   | [Seq: 1 | Activity: ChargeCard   | Payload: {...} ] |   |
|   | [Seq: 2 | Activity: ProvisionAcc | Payload: {...} ] |   |
|   +-----------------------------------------------------+   |
+-------------------------------------------------------------+
```

---

## Project Layout

```text
mini-tempo/
├── cmd/
│   └── server/
│       └── main.go             # Server initialization & HTTP trigger endpoints
├── pkg/
│   ├── core/
│   │   ├── context.go          # WorkflowContext definition & replay engine
│   │   └── event.go            # Event model, sequence IDs, and serialization
│   ├── storage/
│   │   ├── store.go            # EventStore interface
│   │   ├── memory.go           # In-memory history store (Phase 1)
│   │   └── sqlite.go           # SQLite append-only engine (Phase 2)
│   └── worker/
│       └── worker.go           # Worker pool and task poller (Phase 3)
├── examples/
│   └── onboarding/
│       └── main.go             # End-to-end runnable sample
├── go.mod
└── README.md
```

---

## Working Prototype (Self-Contained)

You can run this self-contained demonstration directly. It runs a two-step workflow, triggers an intentional process panic mid-stream, and subsequently restarts using the recorded event history to complete without duplicate side effects:

```go
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"
)

// Event represents an immutable activity completion record
type Event struct {
	ActivityName string `json:"activity_name"`
	Payload      []byte `json:"payload"`
	Completed    bool   `json:"completed"`
}

// WorkflowContext coordinates deterministic replay
type WorkflowContext struct {
	History []Event
	cursor  int
}

// ExecuteActivity runs an activity function or retrieves cached state from history
func (ctx *WorkflowContext) ExecuteActivity(name string, fn func() (interface{}, error), target interface{}) error {
	// Replay Check: Determine if this activity was logged in a previous run
	if ctx.cursor < len(ctx.History) {
		event := ctx.History[ctx.cursor]
		if event.ActivityName != name || !event.Completed {
			return fmt.Errorf("non-deterministic replay mismatch: expected %s, got %s", name, event.ActivityName)
		}
		log.Printf("⏩ [REPLAY] Skipping '%s' -> using cached result", name)
		if target != nil && len(event.Payload) > 0 {
			if err := json.Unmarshal(event.Payload, target); err != nil {
				return err
			}
		}
		ctx.cursor++
		return nil
	}

	// Live Execution: Execute side effect and append to history log
	log.Printf("⚙️ [EXECUTE] Running '%s'...", name)
	res, err := fn()
	if err != nil {
		return err
	}

	data, err := json.Marshal(res)
	if err != nil {
		return err
	}

	ctx.History = append(ctx.History, Event{
		ActivityName: name,
		Payload:      data,
		Completed:    true,
	})
	ctx.cursor++

	if target != nil && len(data) > 0 {
		return json.Unmarshal(data, target)
	}
	return nil
}

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

	log.Println("=== RUN 1: UNHANDLED INTERRUPTION ===")
	ctx1 := &WorkflowContext{History: persistentStore}
	_ = UserOnboardingWorkflow(ctx1, true)

	// Persist the append-only event log
	persistentStore = ctx1.History

	log.Println("=== RUN 2: RESTART & REPLAY FROM LOG ===")
	ctx2 := &WorkflowContext{History: persistentStore}
	if err := UserOnboardingWorkflow(ctx2, false); err != nil {
		log.Fatalf("Recovery failed: %v", err)
	}
}
```

---

## Core Abstractions

- **`WorkflowContext`**: Encapsulates workflow execution state, holding the sequence history and monotonic cursor.
- **`Event`**: Immutable state record tracking activity names, execution completion flags, and serialized payload outputs.
- **`ExecuteActivity`**: Interceptor function that mediates between history replay and live system I/O.

---

## Implementation Roadmap

- [x] **Phase 1: Deterministic Replay Core**
  - [x] Monotonic cursor tracking over completed events.
  - [x] In-memory event replay and JSON payload unmarshaling.
  - [x] Validation of non-deterministic branching errors.
- [x] **Phase 2: Persistent Storage (WAL)**
  - [x] Implement append-only schema in SQLite/PostgreSQL (`workflow_instances`, `workflow_events`).
  - [x] Monotonic event sequence IDs and atomic write operations.
  - [x] Boot-time workflow rehydration from storage.
- [ ] **Phase 3: Queue & Worker Decoupling**
  - [ ] Split orchestrator into `WorkflowWorker` and `ActivityWorker`.
  - [ ] Activity execution timeouts, heartbeats, and exponential retry policies.
- [ ] **Phase 4: Durable Timers & Signals**
  - [ ] Durable timers (`ctx.Sleep(duration)`) that free worker threads.
  - [ ] External signal channels for human-in-the-loop validation steps.

---

## Contributing & Verification

1. **Determinism Verification:** Run workflows under unit test loops to ensure that swapping activity order triggers replay error detection.
2. **Crash Loop Simulation:** Execute random termination (`SIGKILL`) scripts during multi-step activity loops to verify that zero duplicate activities are executed.

---

## License

MIT License. Designed for systems engineering and distributed orchestration studies.
