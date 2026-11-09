package raft

import "context"

// EventKind identifies an observable Raft state transition.
type EventKind string

const (
	EventElectionStarted     EventKind = "election_started"
	EventBecameLeader        EventKind = "became_leader"
	EventBecameFollower      EventKind = "became_follower"
	EventReplicationProgress EventKind = "replication_progress"
	EventCommitAdvanced      EventKind = "commit_advanced"
	EventApplied             EventKind = "applied"
	EventSnapshotCreated     EventKind = "snapshot_created"
	EventSnapshotInstalled   EventKind = "snapshot_installed"
)

// Event is a structured consensus event. Observer callbacks run on the Raft
// event loop and must return quickly.
type Event struct {
	Kind     EventKind
	Node     NodeID
	Peer     NodeID
	Role     Role
	Term     Term
	LeaderID NodeID
	Index    LogIndex
}

// Observer receives structured consensus events.
type Observer func(Event)

// PeerProgress reports a leader's known replicated index and lag for one peer.
type PeerProgress struct {
	Node       NodeID   `json:"node"`
	MatchIndex LogIndex `json:"match_index"`
	Lag        LogIndex `json:"lag"`
}

// Metrics is a consistent, read-only observability snapshot.
type Metrics struct {
	NodeID          NodeID         `json:"node_id"`
	Role            string         `json:"role"`
	Term            Term           `json:"term"`
	LeaderID        NodeID         `json:"leader_id,omitempty"`
	CommitIndex     LogIndex       `json:"commit_index"`
	LastApplied     LogIndex       `json:"last_applied"`
	LastLogIndex    LogIndex       `json:"last_log_index"`
	SnapshotIndex   LogIndex       `json:"snapshot_index"`
	ApplyLag        LogIndex       `json:"apply_lag"`
	PeerReplication []PeerProgress `json:"peer_replication,omitempty"`
}

// Metrics returns a consistent snapshot from the event loop.
func (n *Node) Metrics(ctx context.Context) (Metrics, error) {
	reply := make(chan Metrics, 1)
	if err := n.submit(ctx, metricsEvent{reply: reply}); err != nil {
		return Metrics{}, err
	}
	select {
	case metrics := <-reply:
		return metrics, nil
	case <-ctx.Done():
		return Metrics{}, ctx.Err()
	case <-n.done:
		return Metrics{}, n.stoppedError()
	}
}

type metricsEvent struct{ reply chan<- Metrics }
