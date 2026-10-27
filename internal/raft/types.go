package raft

import "fmt"

const MaxNodeIDBytes = 64

// NodeID uniquely identifies a member of a fixed Raft cluster.
type NodeID string

// Validate checks whether an ID is safe for configuration, logs, and URLs.
func (id NodeID) Validate() error {
	if len(id) == 0 || len(id) > MaxNodeIDBytes {
		return fmt.Errorf("node ID length must be between 1 and %d bytes", MaxNodeIDBytes)
	}
	for _, character := range id {
		if (character >= 'a' && character <= 'z') ||
			(character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') ||
			character == '-' || character == '_' || character == '.' {
			continue
		}
		return fmt.Errorf("node ID %q contains unsupported character %q", id, character)
	}
	return nil
}

// Term is a monotonically increasing Raft election term.
type Term uint64

// LogIndex identifies a position in the replicated log.
type LogIndex uint64

// Entry is one command in the replicated log. Command bytes are opaque to
// Raft and are encoded as base64 by encoding/json.
type Entry struct {
	Index   LogIndex `json:"index"`
	Term    Term     `json:"term"`
	Command []byte   `json:"command"`
}

// Validate checks the invariants of a real, non-sentinel log entry.
func (e Entry) Validate() error {
	if e.Index == 0 {
		return fmt.Errorf("entry index must be positive")
	}
	if e.Term == 0 {
		return fmt.Errorf("entry term must be positive")
	}
	return nil
}
