package core

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"mini-tempo/pkg/storage"
)

// WorkflowContext coordinates deterministic replay
type WorkflowContext struct {
	InstanceID string
	store      storage.EventStore
	history    []storage.Event
	cursor     int
}

// NewWorkflowContext initializes a context with a pre-loaded history of events.
// The Worker is responsible for fetching this history from the database.
func NewWorkflowContext(instanceID string, store storage.EventStore, history []storage.Event) *WorkflowContext {
	if len(history) > 0 {
		log.Printf("♻️ [REHYDRATE] Context initialized with %d events for instance %s", len(history), instanceID)
	}

	return &WorkflowContext{
		InstanceID: instanceID,
		store:      store,
		history:    history,
		cursor:     0,
	}
}

// ExecuteActivity runs an activity function or retrieves cached state from history
func (c *WorkflowContext) ExecuteActivity(ctx context.Context, name string, fn func() (interface{}, error), target interface{}) error {
	// Replay Check: Determine if this activity was logged in a previous run
	if c.cursor < len(c.history) {
		event := c.history[c.cursor]
		if event.ActivityName != name || !event.Completed {
			return fmt.Errorf("non-deterministic replay mismatch: expected %s, got %s", name, event.ActivityName)
		}
		log.Printf("⏩ [REPLAY] Skipping '%s' -> using cached result", name)
		if target != nil && len(event.Payload) > 0 {
			if err := json.Unmarshal(event.Payload, target); err != nil {
				return err
			}
		}
		c.cursor++
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

	event := storage.Event{
		SequenceNum:  c.cursor,
		ActivityName: name,
		Payload:      data,
		Completed:    true,
	}

	if err := c.store.AppendEvent(ctx, c.InstanceID, event); err != nil {
		return fmt.Errorf("failed to persist event: %w", err)
	}

	c.history = append(c.history, event)
	c.cursor++

	if target != nil && len(data) > 0 {
		return json.Unmarshal(data, target)
	}
	return nil
}
