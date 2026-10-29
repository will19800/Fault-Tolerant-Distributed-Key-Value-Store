package database_test

import (
	"context"
	"errors"
	"testing"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/database"
	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/storage"
)

func TestReplicatedStoreEncodesMutationsAndReadsAppliedState(t *testing.T) {
	state := database.NewReplicatedStateMachine()
	proposer := &applyingProposer{state: state}
	store, err := database.NewReplicatedStore(proposer, state)
	if err != nil {
		t.Fatalf("NewReplicatedStore: %v", err)
	}

	if err := store.Put("theme", []byte("light")); err != nil {
		t.Fatalf("Put light: %v", err)
	}
	if err := store.Put("theme", []byte("dark")); err != nil {
		t.Fatalf("Put dark: %v", err)
	}
	value, err := store.Get("theme")
	if err != nil || string(value) != "dark" {
		t.Fatalf("Get theme = %q, %v", value, err)
	}
	if err := store.Delete("theme"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := store.Get("theme"); !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("Get deleted key error = %v", err)
	}
	if proposer.proposals != 3 {
		t.Fatalf("proposal count = %d, want 3", proposer.proposals)
	}
}

func TestReplicatedStateMachineRejectsMalformedOrMultipleCommands(t *testing.T) {
	state := database.NewReplicatedStateMachine()
	if err := state.Apply([]byte("not a command")); err == nil {
		t.Fatal("Apply malformed command returned nil")
	}

	proposer := &capturingProposer{}
	store, err := database.NewReplicatedStore(proposer, state)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Put("one", []byte("1")); err != nil {
		t.Fatal(err)
	}
	first := append([]byte(nil), proposer.command...)
	if err := store.Put("two", []byte("2")); err != nil {
		t.Fatal(err)
	}
	if err := state.Apply(append(first, proposer.command...)); err == nil {
		t.Fatal("Apply multiple commands returned nil")
	}
	if _, err := state.Get("one"); !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("malformed application changed state: %v", err)
	}
}

func TestNewReplicatedStoreValidatesDependencies(t *testing.T) {
	state := database.NewReplicatedStateMachine()
	if _, err := database.NewReplicatedStore(nil, state); err == nil {
		t.Fatal("nil proposer returned nil error")
	}
	if _, err := database.NewReplicatedStore(&capturingProposer{}, nil); err == nil {
		t.Fatal("nil state machine returned nil error")
	}
}

type applyingProposer struct {
	state     *database.ReplicatedStateMachine
	proposals int
}

func (p *applyingProposer) Propose(_ context.Context, command []byte) (raft.LogIndex, error) {
	p.proposals++
	if err := p.state.Apply(command); err != nil {
		return 0, err
	}
	return raft.LogIndex(p.proposals), nil
}

type capturingProposer struct {
	command []byte
}

func (p *capturingProposer) Propose(_ context.Context, command []byte) (raft.LogIndex, error) {
	p.command = append([]byte(nil), command...)
	return 1, nil
}
