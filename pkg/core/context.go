package core

import (
	"encoding/json"
	"fmt"
	"log"
)

// WorkflowContext represents the context of a workflow execution.
type WorkflowContext struct {
	WorkflowID   string
	RunID        string
	Namespace    string
	TaskQueue    string
	WorkflowType string
	History      []Event
	cursor       int
}


// ExecuteActivity executes an activity function and records its result in the workflow history.
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


