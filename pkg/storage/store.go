package storage

import (
	"context"
	"errors"
)

var (
	ErrInstanceNotFound = errors.New("instance not found")
	ErrInstanceExists   = errors.New("instance already exists")
)

// Event represents an immutable activity completion record
type Event struct {
	SequenceNum  int    `json:"sequence_num"`
	ActivityName string `json:"activity_name"`
	Payload      []byte `json:"payload"`
	Completed    bool   `json:"completed"`
}

// EventStore abstracts the persistence layer for workflow runs
type EventStore interface {
	CreateInstance(ctx context.Context, instanceID string, name string) error
	AppendEvent(ctx context.Context, instanceID string, event Event) error
	GetEvents(ctx context.Context, instanceID string) ([]Event, error)
	GetInstanceStatus(ctx context.Context, instanceID string) (string, error)
	UpdateInstanceStatus(ctx context.Context, instanceID string, status string) error
}
