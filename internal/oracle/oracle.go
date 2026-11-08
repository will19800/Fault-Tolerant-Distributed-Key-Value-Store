// Package oracle independently records distributed executions and checks
// safety and convergence properties across replicas.
package oracle

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
)

// OperationID identifies one client operation in a scenario trace.
type OperationID uint64

// OperationKind identifies the requested client behavior.
type OperationKind string

const (
	Put    OperationKind = "put"
	Get    OperationKind = "get"
	Delete OperationKind = "delete"
)

// OperationOutcome records whether a request is unresolved, acknowledged, or
// timed out. A timeout remains ambiguous because the command may later commit.
type OperationOutcome string

const (
	Pending      OperationOutcome = "pending"
	Acknowledged OperationOutcome = "acknowledged"
	TimedOut     OperationOutcome = "timed_out"
)

// Operation is a client request and its observed outcome.
type Operation struct {
	ID      OperationID      `json:"id"`
	Node    raft.NodeID      `json:"node"`
	Kind    OperationKind    `json:"kind"`
	Key     string           `json:"key"`
	Value   []byte           `json:"value,omitempty"`
	Outcome OperationOutcome `json:"outcome"`
	Result  []byte           `json:"result,omitempty"`
}

// ReplicaObservation is one consistent view of a replica. Entries contains
// the retained log after BaseIndex and may include an uncommitted suffix.
type ReplicaObservation struct {
	Node        raft.NodeID
	BaseIndex   raft.LogIndex
	BaseTerm    raft.Term
	CommitIndex raft.LogIndex
	Entries     []raft.Entry
	State       map[string][]byte
}

// Event is a deterministic, logical point in an execution trace.
type Event struct {
	Sequence    uint64        `json:"sequence"`
	Type        string        `json:"type"`
	OperationID OperationID   `json:"operation_id,omitempty"`
	Node        raft.NodeID   `json:"node,omitempty"`
	Index       raft.LogIndex `json:"index,omitempty"`
}

// Oracle records an execution without participating in the system under test.
type Oracle struct {
	mu           sync.Mutex
	scenario     string
	seed         int64
	nextID       OperationID
	nextSequence uint64
	operations   []Operation
	replicas     map[raft.NodeID]ReplicaObservation
	events       []Event
}

// New creates an empty oracle for one reproducible scenario run.
func New(scenario string, seed int64) (*Oracle, error) {
	if scenario == "" {
		return nil, errors.New("scenario name must not be empty")
	}
	return &Oracle{scenario: scenario, seed: seed, replicas: make(map[raft.NodeID]ReplicaObservation)}, nil
}

// Issue records a request before it is sent to a node.
func (o *Oracle) Issue(node raft.NodeID, kind OperationKind, key string, value []byte) (OperationID, error) {
	if err := node.Validate(); err != nil {
		return 0, fmt.Errorf("operation node: %w", err)
	}
	if key == "" {
		return 0, errors.New("operation key must not be empty")
	}
	switch kind {
	case Put:
	case Get, Delete:
		if len(value) != 0 {
			return 0, fmt.Errorf("%s operation must not contain a value", kind)
		}
	default:
		return 0, fmt.Errorf("unknown operation kind %q", kind)
	}

	o.mu.Lock()
	defer o.mu.Unlock()
	o.nextID++
	operation := Operation{ID: o.nextID, Node: node, Kind: kind, Key: key, Value: slices.Clone(value), Outcome: Pending}
	o.operations = append(o.operations, operation)
	o.appendEventLocked(Event{Type: "issued", OperationID: operation.ID, Node: node})
	return operation.ID, nil
}

// Acknowledge records a successful response. Result is used by reads and may
// be nil for writes and deletes.
func (o *Oracle) Acknowledge(id OperationID, result []byte) error {
	return o.finish(id, Acknowledged, result)
}

// Timeout records that the client stopped waiting without deciding whether
// the operation committed.
func (o *Oracle) Timeout(id OperationID) error {
	return o.finish(id, TimedOut, nil)
}

func (o *Oracle) finish(id OperationID, outcome OperationOutcome, result []byte) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if id == 0 || id > OperationID(len(o.operations)) {
		return fmt.Errorf("unknown operation %d", id)
	}
	index := int(id - 1)
	operation := &o.operations[index]
	if operation.Outcome != Pending {
		return fmt.Errorf("operation %d already has outcome %q", id, operation.Outcome)
	}
	if outcome == TimedOut && len(result) != 0 {
		return errors.New("timed-out operation must not contain a result")
	}
	operation.Outcome = outcome
	operation.Result = slices.Clone(result)
	o.appendEventLocked(Event{Type: string(outcome), OperationID: id, Node: operation.Node})
	return nil
}

// ObserveReplica replaces the latest consistent view for a node.
func (o *Oracle) ObserveReplica(observation ReplicaObservation) error {
	if err := validateObservation(observation); err != nil {
		return err
	}
	cloned := cloneObservation(observation)
	o.mu.Lock()
	o.replicas[observation.Node] = cloned
	o.appendEventLocked(Event{Type: "replica_observed", Node: observation.Node, Index: observation.CommitIndex})
	o.mu.Unlock()
	return nil
}

func validateObservation(observation ReplicaObservation) error {
	if err := observation.Node.Validate(); err != nil {
		return fmt.Errorf("replica node: %w", err)
	}
	if (observation.BaseIndex == 0) != (observation.BaseTerm == 0) {
		return errors.New("replica base index and term must either both be zero or both be positive")
	}
	previousIndex, previousTerm := observation.BaseIndex, observation.BaseTerm
	for position, entry := range observation.Entries {
		if err := entry.Validate(); err != nil {
			return fmt.Errorf("replica %q entry %d: %w", observation.Node, position, err)
		}
		if entry.Index != previousIndex+1 {
			return fmt.Errorf("replica %q entry %d has index %d after %d", observation.Node, position, entry.Index, previousIndex)
		}
		if entry.Term < previousTerm {
			return fmt.Errorf("replica %q entry %d term %d is below preceding term %d", observation.Node, position, entry.Term, previousTerm)
		}
		previousIndex, previousTerm = entry.Index, entry.Term
	}
	if observation.CommitIndex < observation.BaseIndex || observation.CommitIndex > previousIndex {
		return fmt.Errorf("replica %q commit index %d is outside retained range %d..%d", observation.Node, observation.CommitIndex, observation.BaseIndex, previousIndex)
	}
	return nil
}

func cloneObservation(observation ReplicaObservation) ReplicaObservation {
	observation.Entries = slices.Clone(observation.Entries)
	for index := range observation.Entries {
		observation.Entries[index].Command = slices.Clone(observation.Entries[index].Command)
	}
	state := make(map[string][]byte, len(observation.State))
	for key, value := range observation.State {
		state[key] = slices.Clone(value)
	}
	observation.State = state
	return observation
}

func (o *Oracle) appendEventLocked(event Event) {
	o.nextSequence++
	event.Sequence = o.nextSequence
	o.events = append(o.events, event)
}

func entriesEqual(left, right raft.Entry) bool {
	return left.Index == right.Index && left.Term == right.Term && bytes.Equal(left.Command, right.Command)
}

func sortedNodes(replicas map[raft.NodeID]ReplicaObservation) []raft.NodeID {
	nodes := make([]raft.NodeID, 0, len(replicas))
	for node := range replicas {
		nodes = append(nodes, node)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i] < nodes[j] })
	return nodes
}
