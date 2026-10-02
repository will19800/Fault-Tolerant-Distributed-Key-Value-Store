// Package storage implements the deterministic key-value state machine.
package storage

import (
	"errors"
	"slices"
	"sync"
)

var (
	ErrEmptyKey    = errors.New("key must not be empty")
	ErrKeyNotFound = errors.New("key not found")
)

// MemoryStore is a concurrent in-memory key-value store.
type MemoryStore struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{data: make(map[string][]byte)}
}

// Put creates key or replaces its current value.
func (s *MemoryStore) Put(key string, value []byte) error {
	if key == "" {
		return ErrEmptyKey
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = slices.Clone(value)
	return nil
}

// Get returns a copy of the current value for key.
func (s *MemoryStore) Get(key string) ([]byte, error) {
	if key == "" {
		return nil, ErrEmptyKey
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.data[key]
	if !ok {
		return nil, ErrKeyNotFound
	}
	return slices.Clone(value), nil
}

// Delete removes key. Deleting a missing key succeeds without changing state.
func (s *MemoryStore) Delete(key string) error {
	if key == "" {
		return ErrEmptyKey
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.data, key)
	return nil
}
