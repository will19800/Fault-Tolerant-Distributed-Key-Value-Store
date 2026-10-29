package raft

import "fmt"

// RequestVoteRequest asks a peer to vote for a candidate in an election term.
type RequestVoteRequest struct {
	Term         Term     `json:"term"`
	CandidateID  NodeID   `json:"candidate_id"`
	LastLogIndex LogIndex `json:"last_log_index"`
	LastLogTerm  Term     `json:"last_log_term"`
}

// Validate checks the structural invariants of a vote request.
func (r RequestVoteRequest) Validate() error {
	if r.Term == 0 {
		return fmt.Errorf("request vote term must be positive")
	}
	if err := r.CandidateID.Validate(); err != nil {
		return fmt.Errorf("candidate ID: %w", err)
	}
	return validateLogPosition(r.LastLogIndex, r.LastLogTerm, "last log")
}

// RequestVoteResponse reports a peer's current term and voting decision.
type RequestVoteResponse struct {
	Term        Term `json:"term"`
	VoteGranted bool `json:"vote_granted"`
}

// Validate checks the structural invariants of a vote response.
func (r RequestVoteResponse) Validate() error {
	if r.Term == 0 {
		return fmt.Errorf("request vote response term must be positive")
	}
	return nil
}

// AppendEntriesRequest replicates entries and also serves as an empty
// heartbeat when Entries has length zero.
type AppendEntriesRequest struct {
	Term         Term     `json:"term"`
	LeaderID     NodeID   `json:"leader_id"`
	PrevLogIndex LogIndex `json:"prev_log_index"`
	PrevLogTerm  Term     `json:"prev_log_term"`
	Entries      []Entry  `json:"entries"`
	LeaderCommit LogIndex `json:"leader_commit"`
}

// Validate checks structural invariants without deciding whether a follower's
// local log accepts the request.
func (r AppendEntriesRequest) Validate() error {
	if r.Term == 0 {
		return fmt.Errorf("append entries term must be positive")
	}
	if err := r.LeaderID.Validate(); err != nil {
		return fmt.Errorf("leader ID: %w", err)
	}
	if err := validateLogPosition(r.PrevLogIndex, r.PrevLogTerm, "previous log"); err != nil {
		return err
	}

	expectedIndex := r.PrevLogIndex + 1
	previousTerm := r.PrevLogTerm
	for position, entry := range r.Entries {
		if err := entry.Validate(); err != nil {
			return fmt.Errorf("entry %d: %w", position, err)
		}
		if entry.Index != expectedIndex {
			return fmt.Errorf("entry %d index is %d, want contiguous index %d", position, entry.Index, expectedIndex)
		}
		if entry.Term < previousTerm {
			return fmt.Errorf("entry %d term %d is below preceding term %d", position, entry.Term, previousTerm)
		}
		if entry.Term > r.Term {
			return fmt.Errorf("entry %d term %d exceeds leader term %d", position, entry.Term, r.Term)
		}
		if len(entry.Command) > MaxCommandBytes {
			return fmt.Errorf("entry %d command exceeds %d bytes", position, MaxCommandBytes)
		}
		expectedIndex++
		previousTerm = entry.Term
	}
	return nil
}

// AppendEntriesResponse reports replication success and includes conflict
// hints that a future leader implementation can use to skip failed indexes.
type AppendEntriesResponse struct {
	Term          Term     `json:"term"`
	Success       bool     `json:"success"`
	MatchIndex    LogIndex `json:"match_index"`
	ConflictIndex LogIndex `json:"conflict_index"`
	ConflictTerm  Term     `json:"conflict_term"`
}

// Validate checks the structural invariants of an append response.
func (r AppendEntriesResponse) Validate() error {
	if r.Term == 0 {
		return fmt.Errorf("append entries response term must be positive")
	}
	if r.Success {
		if r.ConflictIndex != 0 || r.ConflictTerm != 0 {
			return fmt.Errorf("successful append response must not contain conflict hints")
		}
		return nil
	}
	if r.MatchIndex != 0 {
		return fmt.Errorf("failed append response must not contain a match index")
	}
	if r.ConflictIndex == 0 {
		return fmt.Errorf("failed append response must contain a conflict index")
	}
	return nil
}

func validateLogPosition(index LogIndex, term Term, name string) error {
	if index == 0 && term != 0 {
		return fmt.Errorf("%s term must be zero when index is zero", name)
	}
	if index != 0 && term == 0 {
		return fmt.Errorf("%s term must be positive when index is positive", name)
	}
	return nil
}
