package storage

import (
	"context"
	"fmt"
)

type MemoryEventStore struct {
	instances map[string]string
	events    map[string][]Event
}

func NewMemoryEventStore() *MemoryEventStore {
	return &MemoryEventStore{
		instances: make(map[string]string),
		events:    make(map[string][]Event),
	}
}

func (s *MemoryEventStore) CreateInstance(ctx context.Context, instanceID string, name string) error {
	if _, exists := s.instances[instanceID]; exists {
		return fmt.Errorf("%w: %s", ErrInstanceExists, instanceID)
	}
	s.instances[instanceID] = "RUNNING"
	return nil
}

func (s *MemoryEventStore) AppendEvent(ctx context.Context, instanceID string, event Event) error {
	if _, exists := s.instances[instanceID]; !exists {
		return fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}
	s.events[instanceID] = append(s.events[instanceID], event)
	return nil
}

func (s *MemoryEventStore) GetEvents(ctx context.Context, instanceID string) ([]Event, error) {
	return s.events[instanceID], nil
}

func (s *MemoryEventStore) GetInstanceStatus(ctx context.Context, instanceID string) (string, error) {
	status, exists := s.instances[instanceID]
	if !exists {
		return "", fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}
	return status, nil
}

func (s *MemoryEventStore) UpdateInstanceStatus(ctx context.Context, instanceID string, status string) error {
	if _, exists := s.instances[instanceID]; !exists {
		return fmt.Errorf("%w: %s", ErrInstanceNotFound, instanceID)
	}
	s.instances[instanceID] = status
	return nil
}
