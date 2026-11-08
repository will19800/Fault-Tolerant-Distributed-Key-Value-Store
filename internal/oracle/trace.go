package oracle

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
)

// StateEntry is one deterministically ordered key-value pair in a trace.
type StateEntry struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

// ReplicaTrace captures the evidence from one replica.
type ReplicaTrace struct {
	Node        raft.NodeID   `json:"node"`
	BaseIndex   raft.LogIndex `json:"base_index"`
	BaseTerm    raft.Term     `json:"base_term"`
	CommitIndex raft.LogIndex `json:"commit_index"`
	Entries     []raft.Entry  `json:"entries"`
	State       []StateEntry  `json:"state"`
}

// Trace is the deterministic artifact emitted for a failed scenario run.
type Trace struct {
	Scenario   string         `json:"scenario"`
	Seed       int64          `json:"seed"`
	Failure    string         `json:"failure,omitempty"`
	Operations []Operation    `json:"operations"`
	Replicas   []ReplicaTrace `json:"replicas"`
	Events     []Event        `json:"events"`
}

// Trace returns a deep copy of the current execution evidence.
func (o *Oracle) Trace(cause error) Trace {
	o.mu.Lock()
	defer o.mu.Unlock()
	trace := Trace{Scenario: o.scenario, Seed: o.seed, Operations: slices.Clone(o.operations), Events: slices.Clone(o.events)}
	if cause != nil {
		trace.Failure = cause.Error()
	}
	for index := range trace.Operations {
		trace.Operations[index].Value = slices.Clone(trace.Operations[index].Value)
		trace.Operations[index].Result = slices.Clone(trace.Operations[index].Result)
	}
	for _, node := range sortedNodes(o.replicas) {
		observation := o.replicas[node]
		replica := ReplicaTrace{
			Node: node, BaseIndex: observation.BaseIndex, BaseTerm: observation.BaseTerm,
			CommitIndex: observation.CommitIndex, Entries: cloneObservation(observation).Entries,
		}
		keys := make([]string, 0, len(observation.State))
		for key := range observation.State {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			replica.State = append(replica.State, StateEntry{Key: key, Value: slices.Clone(observation.State[key])})
		}
		trace.Replicas = append(trace.Replicas, replica)
	}
	return trace
}

// MarshalTrace encodes stable, human-readable JSON suitable for replay and
// comparison between runs.
func (o *Oracle) MarshalTrace(cause error) ([]byte, error) {
	data, err := json.MarshalIndent(o.Trace(cause), "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode correctness trace: %w", err)
	}
	return append(data, '\n'), nil
}

// WriteTrace emits the current trace using an atomic rename.
func (o *Oracle) WriteTrace(path string, cause error) error {
	if path == "" {
		return fmt.Errorf("trace path must not be empty")
	}
	data, err := o.MarshalTrace(cause)
	if err != nil {
		return err
	}
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, ".oracle-trace-*")
	if err != nil {
		return fmt.Errorf("create correctness trace: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if _, err := temporary.Write(data); err != nil {
		temporary.Close()
		return fmt.Errorf("write correctness trace: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return fmt.Errorf("sync correctness trace: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close correctness trace: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("publish correctness trace: %w", err)
	}
	return nil
}
