package database

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/storage"
	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/wal"
)

// Proposer is the Raft behavior required by ReplicatedStore.
type Proposer interface {
	Propose(context.Context, []byte) (raft.LogIndex, error)
}

// ReplicatedStateMachine owns the key-value state derived from committed Raft
// commands.
type ReplicatedStateMachine struct {
	state *storage.MemoryStore
}

// NewReplicatedStateMachine returns an empty key-value state machine.
func NewReplicatedStateMachine() *ReplicatedStateMachine {
	return &ReplicatedStateMachine{state: storage.NewMemoryStore()}
}

// Apply decodes and applies one committed mutation.
func (s *ReplicatedStateMachine) Apply(command []byte) error {
	reader := bytes.NewReader(command)
	record, err := wal.Decode(reader)
	if err != nil {
		return fmt.Errorf("decode replicated command: %w", err)
	}
	if _, err := wal.Decode(reader); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("replicated command contains multiple records")
		}
		return fmt.Errorf("decode trailing replicated command data: %w", err)
	}
	return applyRecord(s.state, record)
}

// Get returns a value from the applied state.
func (s *ReplicatedStateMachine) Get(key string) ([]byte, error) {
	return s.state.Get(key)
}

// ReplicatedStore translates API mutations into Raft commands and serves
// reads from the applied state machine.
type ReplicatedStore struct {
	proposer Proposer
	state    *ReplicatedStateMachine
}

// NewReplicatedStore joins a Raft proposer with its applied state machine.
func NewReplicatedStore(proposer Proposer, state *ReplicatedStateMachine) (*ReplicatedStore, error) {
	if proposer == nil {
		return nil, errors.New("proposer must not be nil")
	}
	if state == nil {
		return nil, errors.New("replicated state machine must not be nil")
	}
	return &ReplicatedStore{proposer: proposer, state: state}, nil
}

// Put waits until Raft commits and applies the encoded mutation.
func (s *ReplicatedStore) Put(key string, value []byte) error {
	return s.propose(wal.Record{Operation: wal.OperationPut, Key: key, Value: value})
}

// Get reads the value currently visible in the applied state machine.
func (s *ReplicatedStore) Get(key string) ([]byte, error) {
	return s.state.Get(key)
}

// Delete waits until Raft commits and applies the encoded mutation.
func (s *ReplicatedStore) Delete(key string) error {
	return s.propose(wal.Record{Operation: wal.OperationDelete, Key: key})
}

func (s *ReplicatedStore) propose(record wal.Record) error {
	command, err := wal.Encode(record)
	if err != nil {
		return fmt.Errorf("encode replicated command: %w", err)
	}
	if _, err := s.proposer.Propose(context.Background(), command); err != nil {
		return fmt.Errorf("propose replicated command: %w", err)
	}
	return nil
}

func applyRecord(state *storage.MemoryStore, record wal.Record) error {
	switch record.Operation {
	case wal.OperationPut:
		return state.Put(record.Key, record.Value)
	case wal.OperationDelete:
		return state.Delete(record.Key)
	default:
		return fmt.Errorf("unsupported replicated operation %d", record.Operation)
	}
}
