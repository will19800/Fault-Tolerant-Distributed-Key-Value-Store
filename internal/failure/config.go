// Package failure provides deterministic fault injection around Raft's
// transport and process lifecycle boundaries.
package failure

import (
	"errors"
	"fmt"
	"time"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
)

// MessageType selects the Raft RPC affected by a network rule.
type MessageType string

const (
	AnyMessage      MessageType = "any"
	RequestVote     MessageType = "request_vote"
	AppendEntries   MessageType = "append_entries"
	InstallSnapshot MessageType = "install_snapshot"
)

// NetworkRule affects messages on one directed link. Duration zero means the
// rule remains active after StartAfter.
type NetworkRule struct {
	From       raft.NodeID
	To         raft.NodeID
	Message    MessageType
	Drop       bool
	LossRate   float64
	Delay      time.Duration
	Jitter     time.Duration
	StartAfter time.Duration
	Duration   time.Duration
}

// ProcessActionKind identifies a scheduled lifecycle transition.
type ProcessActionKind string

const (
	Crash        ProcessActionKind = "crash"
	GracefulStop ProcessActionKind = "graceful_stop"
	Restart      ProcessActionKind = "restart"
)

// ProcessAction invokes one lifecycle hook at an offset from schedule start.
type ProcessAction struct {
	At   time.Duration
	Node raft.NodeID
	Kind ProcessActionKind
}

// Scenario is a reproducible collection of network and process faults.
type Scenario struct {
	Seed    int64
	Network []NetworkRule
	Actions []ProcessAction
}

// Validate rejects ambiguous or impossible fault configurations.
func (s Scenario) Validate() error {
	for index, rule := range s.Network {
		if err := rule.validate(); err != nil {
			return fmt.Errorf("network rule %d: %w", index, err)
		}
	}
	for index, action := range s.Actions {
		if err := action.validate(); err != nil {
			return fmt.Errorf("process action %d: %w", index, err)
		}
	}
	return nil
}

func (r NetworkRule) validate() error {
	if err := r.From.Validate(); err != nil {
		return fmt.Errorf("source node: %w", err)
	}
	if err := r.To.Validate(); err != nil {
		return fmt.Errorf("target node: %w", err)
	}
	if r.From == r.To {
		return errors.New("source and target nodes must differ")
	}
	switch r.Message {
	case AnyMessage, RequestVote, AppendEntries, InstallSnapshot:
	default:
		return fmt.Errorf("unknown message type %q", r.Message)
	}
	if r.LossRate < 0 || r.LossRate > 1 {
		return fmt.Errorf("loss rate %.3f must be between 0 and 1", r.LossRate)
	}
	if r.Delay < 0 || r.Jitter < 0 || r.StartAfter < 0 || r.Duration < 0 {
		return errors.New("delay, jitter, start offset, and duration must not be negative")
	}
	if !r.Drop && r.LossRate == 0 && r.Delay == 0 && r.Jitter == 0 {
		return errors.New("rule does not inject a fault")
	}
	return nil
}

func (a ProcessAction) validate() error {
	if a.At < 0 {
		return errors.New("action offset must not be negative")
	}
	if err := a.Node.Validate(); err != nil {
		return fmt.Errorf("node: %w", err)
	}
	switch a.Kind {
	case Crash, GracefulStop, Restart:
		return nil
	default:
		return fmt.Errorf("unknown process action %q", a.Kind)
	}
}

// PartitionRules returns bidirectional drop rules for every pair of nodes in
// different groups. A one-way partition is represented directly by a rule.
func PartitionRules(groups [][]raft.NodeID, startAfter, duration time.Duration) ([]NetworkRule, error) {
	if startAfter < 0 || duration < 0 {
		return nil, errors.New("partition start offset and duration must not be negative")
	}
	if len(groups) < 2 {
		return nil, errors.New("partition requires at least two groups")
	}
	seen := make(map[raft.NodeID]struct{})
	for groupIndex, group := range groups {
		if len(group) == 0 {
			return nil, fmt.Errorf("partition group %d is empty", groupIndex)
		}
		for _, node := range group {
			if err := node.Validate(); err != nil {
				return nil, fmt.Errorf("partition group %d: %w", groupIndex, err)
			}
			if _, exists := seen[node]; exists {
				return nil, fmt.Errorf("node %q appears in multiple partition groups", node)
			}
			seen[node] = struct{}{}
		}
	}
	var rules []NetworkRule
	for left := 0; left < len(groups); left++ {
		for right := left + 1; right < len(groups); right++ {
			for _, from := range groups[left] {
				for _, to := range groups[right] {
					rules = append(rules,
						NetworkRule{From: from, To: to, Message: AnyMessage, Drop: true, StartAfter: startAfter, Duration: duration},
						NetworkRule{From: to, To: from, Message: AnyMessage, Drop: true, StartAfter: startAfter, Duration: duration},
					)
				}
			}
		}
	}
	return rules, nil
}
