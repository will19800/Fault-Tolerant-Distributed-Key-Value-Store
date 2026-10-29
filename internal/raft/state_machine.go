package raft

// StateMachine applies opaque committed commands in Raft log order. Apply
// must return only after the command is visible to subsequent state reads.
type StateMachine interface {
	Apply(command []byte) error
}
