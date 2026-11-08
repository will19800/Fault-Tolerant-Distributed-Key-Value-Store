package oracle

import (
	"bytes"
	"errors"
	"fmt"
	"sort"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
)

// ViolationKind identifies the failed distributed correctness property.
type ViolationKind string

const (
	CommittedPrefixViolation ViolationKind = "committed_prefix"
	ConvergenceViolation     ViolationKind = "convergence"
)

// Violation reports evidence that a correctness property did not hold.
type Violation struct {
	Kind ViolationKind
	Text string
}

func (v *Violation) Error() string { return fmt.Sprintf("%s violation: %s", v.Kind, v.Text) }

// Check verifies committed-prefix agreement followed by applied-state
// convergence across the latest observations.
func (o *Oracle) Check() error {
	if err := o.CheckCommittedPrefix(); err != nil {
		return err
	}
	return o.CheckConvergence()
}

// CheckCommittedPrefix ensures replicas never report different committed
// entries at the same index. A compacted boundary can verify the term but no
// longer exposes the command at that index.
func (o *Oracle) CheckCommittedPrefix() error {
	replicas := o.replicaSnapshot()
	if len(replicas) == 0 {
		return errors.New("committed-prefix check requires at least one replica observation")
	}
	nodes := sortedNodes(replicas)
	for leftIndex, leftNode := range nodes {
		for _, rightNode := range nodes[leftIndex+1:] {
			left, right := replicas[leftNode], replicas[rightNode]
			start := max(left.BaseIndex, right.BaseIndex)
			if start == 0 {
				start = 1
			}
			end := min(left.CommitIndex, right.CommitIndex)
			for index := start; index <= end; index++ {
				leftEntry, leftCommand, leftOK := committedEntryAt(left, index)
				rightEntry, rightCommand, rightOK := committedEntryAt(right, index)
				if !leftOK || !rightOK {
					continue
				}
				if leftEntry.Term != rightEntry.Term || (leftCommand && rightCommand && !entriesEqual(leftEntry, rightEntry)) {
					return &Violation{Kind: CommittedPrefixViolation, Text: fmt.Sprintf("nodes %q and %q disagree at index %d", leftNode, rightNode, index)}
				}
				if index == end {
					break
				}
			}
		}
	}
	return nil
}

func committedEntryAt(observation ReplicaObservation, index raft.LogIndex) (raft.Entry, bool, bool) {
	if index > observation.CommitIndex || index < observation.BaseIndex {
		return raft.Entry{}, false, false
	}
	if index == observation.BaseIndex {
		return raft.Entry{Index: index, Term: observation.BaseTerm}, false, true
	}
	position := index - observation.BaseIndex - 1
	if position >= raft.LogIndex(len(observation.Entries)) {
		return raft.Entry{}, false, false
	}
	return observation.Entries[position], true, true
}

// CheckConvergence ensures all observed replicas expose the same applied
// key-value state. Call it after the workload stops and faults heal.
func (o *Oracle) CheckConvergence() error {
	replicas := o.replicaSnapshot()
	if len(replicas) == 0 {
		return errors.New("convergence check requires at least one replica observation")
	}
	nodes := sortedNodes(replicas)
	reference := replicas[nodes[0]].State
	for _, node := range nodes[1:] {
		if key, equal := firstStateDifference(reference, replicas[node].State); !equal {
			return &Violation{Kind: ConvergenceViolation, Text: fmt.Sprintf("nodes %q and %q differ at key %q", nodes[0], node, key)}
		}
	}
	return nil
}

func firstStateDifference(left, right map[string][]byte) (string, bool) {
	keys := make([]string, 0, len(left)+len(right))
	seen := make(map[string]struct{}, len(left)+len(right))
	for key := range left {
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	for key := range right {
		if _, exists := seen[key]; !exists {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		leftValue, leftExists := left[key]
		rightValue, rightExists := right[key]
		if leftExists != rightExists || !bytes.Equal(leftValue, rightValue) {
			return key, false
		}
	}
	return "", true
}

func (o *Oracle) replicaSnapshot() map[raft.NodeID]ReplicaObservation {
	o.mu.Lock()
	defer o.mu.Unlock()
	replicas := make(map[raft.NodeID]ReplicaObservation, len(o.replicas))
	for node, observation := range o.replicas {
		replicas[node] = cloneObservation(observation)
	}
	return replicas
}
