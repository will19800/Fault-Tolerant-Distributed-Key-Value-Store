package raft_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
)

func TestFileHardStateStoreRoundTripAndReplace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hard-state")
	store, err := raft.NewFileHardStateStore(path)
	if err != nil {
		t.Fatalf("NewFileHardStateStore: %v", err)
	}
	if state, err := store.Load(); err != nil || state != (raft.HardState{}) {
		t.Fatalf("initial Load = %+v, %v", state, err)
	}

	first := raft.HardState{CurrentTerm: 4, VotedFor: "node-2"}
	if err := store.Save(first); err != nil {
		t.Fatalf("Save first: %v", err)
	}
	second := raft.HardState{CurrentTerm: 5}
	if err := store.Save(second); err != nil {
		t.Fatalf("Save second: %v", err)
	}

	reopened, err := raft.NewFileHardStateStore(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if got, err := reopened.Load(); err != nil || got != second {
		t.Fatalf("reopened Load = %+v, %v; want %+v", got, err, second)
	}
}

func TestNodeRecoversTermFromFileAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hard-state")
	store, err := raft.NewFileHardStateStore(path)
	if err != nil {
		t.Fatal(err)
	}
	firstTimer := newManualTimer()
	first, err := raft.NewNode(raft.NodeConfig{
		ID: "node-1", Members: testMembers, Transport: unavailableTransport{}, ElectionTimeout: time.Second,
		Clock: manualClock{timer: firstTimer}, Persistence: store, LogStore: newMemoryLogStore(nil),
	})
	if err != nil {
		t.Fatalf("first NewNode: %v", err)
	}
	firstTimer.Fire()
	if status, err := first.Status(context.Background()); err != nil || status.Term != 1 {
		t.Fatalf("first Status = %+v, %v", status, err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}

	reopened, err := raft.NewFileHardStateStore(path)
	if err != nil {
		t.Fatal(err)
	}
	secondTimer := newManualTimer()
	second, err := raft.NewNode(raft.NodeConfig{
		ID: "node-1", Members: testMembers, Transport: unavailableTransport{}, ElectionTimeout: time.Second,
		Clock: manualClock{timer: secondTimer}, Persistence: reopened, LogStore: newMemoryLogStore(nil),
	})
	if err != nil {
		t.Fatalf("second NewNode: %v", err)
	}
	defer second.Close()
	if status, err := second.Status(context.Background()); err != nil || status.Term != 1 || status.Role != raft.Follower {
		t.Fatalf("recovered Status = %+v, %v", status, err)
	}
}

func TestFileHardStateStoreRejectsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hard-state")
	store, err := raft.NewFileHardStateStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(raft.HardState{CurrentTerm: 3, VotedFor: "node-1"}); err != nil {
		t.Fatal(err)
	}
	frame, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	frame[len(frame)-1] ^= 0xff
	if err := os.WriteFile(path, frame, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(); !errors.Is(err, raft.ErrHardStateCorrupt) {
		t.Fatalf("Load corruption error = %v, want ErrHardStateCorrupt", err)
	}
}

func TestHardStateValidation(t *testing.T) {
	invalid := []raft.HardState{
		{VotedFor: "node-1"},
		{CurrentTerm: 1, VotedFor: "node 1"},
	}
	for _, state := range invalid {
		if err := state.Validate(); err == nil {
			t.Fatalf("HardState(%+v).Validate returned nil error", state)
		}
	}
	if _, err := raft.NewFileHardStateStore(""); err == nil {
		t.Fatal("NewFileHardStateStore accepted an empty path")
	}
}
