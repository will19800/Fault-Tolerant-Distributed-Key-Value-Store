package database

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/storage"
	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/wal"
)

const stateSnapshotVersion uint8 = 1

// Consensus is the Raft behavior required by ReplicatedStore.
type Consensus interface {
	Propose(context.Context, []byte) (raft.LogIndex, error)
	ReadIndex(context.Context) (raft.LogIndex, error)
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

// Snapshot deterministically encodes the complete applied key-value state.
func (s *ReplicatedStateMachine) Snapshot() ([]byte, error) {
	state := s.state.Snapshot()
	keys := make([]string, 0, len(state))
	for key := range state {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var output bytes.Buffer
	output.WriteByte(stateSnapshotVersion)
	if err := binary.Write(&output, binary.BigEndian, uint32(len(keys))); err != nil {
		return nil, err
	}
	for _, key := range keys {
		value := state[key]
		if len(key) > int(^uint32(0)) || len(value) > int(^uint32(0)) {
			return nil, errors.New("snapshot key or value exceeds 4 GiB")
		}
		if err := binary.Write(&output, binary.BigEndian, uint32(len(key))); err != nil {
			return nil, err
		}
		if err := binary.Write(&output, binary.BigEndian, uint32(len(value))); err != nil {
			return nil, err
		}
		output.WriteString(key)
		output.Write(value)
	}
	return output.Bytes(), nil
}

// ReplicatedStore translates API mutations into Raft commands and serves
// reads from the applied state machine.
type ReplicatedStore struct {
	consensus Consensus
	state     *ReplicatedStateMachine
}

// NewReplicatedStore joins Raft consensus with its applied state machine.
func NewReplicatedStore(consensus Consensus, state *ReplicatedStateMachine) (*ReplicatedStore, error) {
	if consensus == nil {
		return nil, errors.New("consensus must not be nil")
	}
	if state == nil {
		return nil, errors.New("replicated state machine must not be nil")
	}
	return &ReplicatedStore{consensus: consensus, state: state}, nil
}

// Put waits until Raft commits and applies the encoded mutation.
func (s *ReplicatedStore) Put(key string, value []byte) error {
	return s.PutContext(context.Background(), key, value)
}

// PutContext waits for commitment and application or for ctx to end.
func (s *ReplicatedStore) PutContext(ctx context.Context, key string, value []byte) error {
	return s.propose(ctx, wal.Record{Operation: wal.OperationPut, Key: key, Value: value})
}

// Get reads the value currently visible in the applied state machine.
func (s *ReplicatedStore) Get(key string) ([]byte, error) {
	return s.GetContext(context.Background(), key)
}

// GetContext confirms leadership with a quorum before reading applied state.
func (s *ReplicatedStore) GetContext(ctx context.Context, key string) ([]byte, error) {
	if _, err := s.consensus.ReadIndex(ctx); err != nil {
		return nil, fmt.Errorf("confirm linearizable read: %w", err)
	}
	return s.state.Get(key)
}

// Delete waits until Raft commits and applies the encoded mutation.
func (s *ReplicatedStore) Delete(key string) error {
	return s.DeleteContext(context.Background(), key)
}

// DeleteContext waits for commitment and application or for ctx to end.
func (s *ReplicatedStore) DeleteContext(ctx context.Context, key string) error {
	return s.propose(ctx, wal.Record{Operation: wal.OperationDelete, Key: key})
}

func (s *ReplicatedStore) propose(ctx context.Context, record wal.Record) error {
	command, err := wal.Encode(record)
	if err != nil {
		return fmt.Errorf("encode replicated command: %w", err)
	}
	if _, err := s.consensus.Propose(ctx, command); err != nil {
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
