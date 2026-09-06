package storage_test

import (
	"context"
	"errors"
	"os"
	"testing"

	"mini-tempo/pkg/storage"
)

func TestEventStores(t *testing.T) {
	// Setup memory store
	memStore := storage.NewMemoryEventStore()

	// Setup sqlite store
	tmpFile, err := os.CreateTemp("", "mini-tempo-test-*.db")
	if err != nil {
		t.Fatalf("failed to create temp db: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	sqliteStore, err := storage.NewSqliteEventStore(tmpFile.Name())
	if err != nil {
		t.Fatalf("failed to create sqlite store: %v", err)
	}

	stores := map[string]storage.EventStore{
		"Memory": memStore,
		"SQLite": sqliteStore,
	}

	for name, store := range stores {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			instanceID := "test_instance_" + name

			// 1. Create Instance
			err := store.CreateInstance(ctx, instanceID, "TestWorkflow")
			if err != nil {
				t.Fatalf("expected nil error, got: %v", err)
			}

			// 2. Create Duplicate Instance (Should fail)
			err = store.CreateInstance(ctx, instanceID, "TestWorkflow")
			if err == nil || !errors.Is(err, storage.ErrInstanceExists) {
				t.Errorf("expected ErrInstanceExists, got: %v", err)
			}

			// 3. Check Initial Status
			status, err := store.GetInstanceStatus(ctx, instanceID)
			if err != nil {
				t.Fatalf("expected nil error, got: %v", err)
			}
			if status != "RUNNING" {
				t.Errorf("expected RUNNING, got: %s", status)
			}

			// 4. Update Status
			err = store.UpdateInstanceStatus(ctx, instanceID, "COMPLETED")
			if err != nil {
				t.Fatalf("expected nil error, got: %v", err)
			}
			status, _ = store.GetInstanceStatus(ctx, instanceID)
			if status != "COMPLETED" {
				t.Errorf("expected COMPLETED, got: %s", status)
			}

			// 5. Append Events
			event1 := storage.Event{
				SequenceNum:  0,
				ActivityName: "Act1",
				Payload:      []byte(`{"foo":"bar"}`),
				Completed:    true,
			}
			event2 := storage.Event{
				SequenceNum:  1,
				ActivityName: "Act2",
				Payload:      []byte(`{"baz":123}`),
				Completed:    true,
			}

			err = store.AppendEvent(ctx, instanceID, event1)
			if err != nil {
				t.Fatalf("failed to append event1: %v", err)
			}
			err = store.AppendEvent(ctx, instanceID, event2)
			if err != nil {
				t.Fatalf("failed to append event2: %v", err)
			}

			// 6. Get Events
			events, err := store.GetEvents(ctx, instanceID)
			if err != nil {
				t.Fatalf("failed to get events: %v", err)
			}
			if len(events) != 2 {
				t.Fatalf("expected 2 events, got %d", len(events))
			}
			if events[0].ActivityName != "Act1" || events[1].ActivityName != "Act2" {
				t.Errorf("events out of order or corrupted")
			}
		})
	}
}
