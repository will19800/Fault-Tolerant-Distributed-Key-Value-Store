package database

import (
	"errors"
	"fmt"
	"sync"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/storage"
	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/wal"
)

var ErrClosed = errors.New("database is closed")

// Recovery summarizes the records restored during Open.
type Recovery struct {
	Records        int
	TruncatedBytes int64
}

// Store is a durable single-node key-value database.
type Store struct {
	lifecycle sync.RWMutex
	writes    sync.Mutex
	state     *storage.MemoryStore
	log       *wal.Log
	closed    bool
}

// Open creates or recovers a database from walPath.
func Open(walPath string, policy wal.SyncPolicy) (*Store, Recovery, error) {
	log, err := wal.Open(walPath, policy)
	if err != nil {
		return nil, Recovery{}, err
	}

	store := &Store{
		state: storage.NewMemoryStore(),
		log:   log,
	}
	result, err := log.Replay(store.apply)
	if err != nil {
		return nil, Recovery{}, errors.Join(err, log.Close())
	}
	return store, Recovery{
		Records:        result.Records,
		TruncatedBytes: result.TruncatedBytes,
	}, nil
}

// Put durably records a value before applying it to the in-memory state.
func (s *Store) Put(key string, value []byte) error {
	s.lifecycle.RLock()
	defer s.lifecycle.RUnlock()
	if s.closed {
		return ErrClosed
	}

	s.writes.Lock()
	defer s.writes.Unlock()
	record := wal.Record{Operation: wal.OperationPut, Key: key, Value: value}
	if err := s.log.Append(record); err != nil {
		return fmt.Errorf("record put: %w", err)
	}
	if err := s.apply(record); err != nil {
		return fmt.Errorf("apply durable put: %w", err)
	}
	return nil
}

// Get returns the current value for key.
func (s *Store) Get(key string) ([]byte, error) {
	s.lifecycle.RLock()
	defer s.lifecycle.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	return s.state.Get(key)
}

// Delete durably records a deletion before applying it in memory.
func (s *Store) Delete(key string) error {
	s.lifecycle.RLock()
	defer s.lifecycle.RUnlock()
	if s.closed {
		return ErrClosed
	}

	s.writes.Lock()
	defer s.writes.Unlock()
	record := wal.Record{Operation: wal.OperationDelete, Key: key}
	if err := s.log.Append(record); err != nil {
		return fmt.Errorf("record delete: %w", err)
	}
	if err := s.apply(record); err != nil {
		return fmt.Errorf("apply durable delete: %w", err)
	}
	return nil
}

// Close flushes the WAL and prevents further operations. Repeated calls
// succeed.
func (s *Store) Close() error {
	s.lifecycle.Lock()
	defer s.lifecycle.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.log.Close()
}

func (s *Store) apply(record wal.Record) error {
	return applyRecord(s.state, record)
}
