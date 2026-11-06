package raft

// StateMachine applies opaque committed commands in Raft log order. Apply
// must return only after the command is visible to subsequent state reads.
type StateMachine interface {
	Apply(command []byte) error
}

// SnapshotStateMachine can serialize its complete applied state. Snapshot is
// called from the Raft event loop, so it observes exactly LastApplied.
type SnapshotStateMachine interface {
	StateMachine
	Snapshot() ([]byte, error)
	Restore([]byte) error
}

// Snapshot is an immutable state-machine image and the Raft position it
// includes.
type Snapshot struct {
	LastIncludedIndex LogIndex
	LastIncludedTerm  Term
	State             []byte
}

// SnapshotStore atomically publishes durable snapshots.
type SnapshotStore interface {
	Load() (Snapshot, error)
	Save(Snapshot) error
}

// LogCompactor durably removes entries covered by a snapshot while retaining
// the supplied suffix.
type LogCompactor interface {
	Compact(LogIndex, Term, []Entry) error
	InstallSnapshot(LogIndex, Term, []Entry) error
}
