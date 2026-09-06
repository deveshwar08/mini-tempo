// Trigger worker on application startup
package main

import (
	"log"

	"mini-tempo/pkg/storage"
	"mini-tempo/pkg/worker"
)

func main() {
	// Initialize the event store (SQLite in this case)
	store, err := storage.NewSqliteEventStore("events.db")
	if err != nil {
		log.Fatalf("Failed to initialize event store: %v", err)
	}
	defer store.Close()

	// Create a new worker
	w := worker.NewWorker(store)

	// In a complete implementation (Phase 3), the worker would listen to a queue
	// or expose an HTTP API here to accept and poll for workflow instances.
	log.Println("Server initialized with worker:", w)
	log.Println("Ready to accept workflows. (Polling and HTTP API coming in Phase 3)")
}
