package raft

import "fmt"

// Role identifies a node's current place in the Raft election protocol.
type Role uint8

const (
	// Follower receives leader traffic and may grant one vote per term.
	Follower Role = iota
	// Candidate is attempting to win an election for its current term.
	Candidate
	// Leader coordinates log replication for its current term.
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return fmt.Sprintf("Role(%d)", r)
	}
}

// Status is an immutable snapshot of a node's event-loop-owned state.
type Status struct {
	ID       NodeID
	Role     Role
	Term     Term
	LeaderID NodeID
}
