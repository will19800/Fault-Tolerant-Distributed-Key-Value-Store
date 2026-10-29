package raft_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
)

func TestFollowerDurablyAppendsEntriesBeforeSuccess(t *testing.T) {
	store := &blockingLogStore{
		started: make(chan []raft.Entry, 1),
		release: make(chan struct{}),
	}
	node := newFollowerNode(t, raft.HardState{CurrentTerm: 2}, store)
	defer node.Close()
	request := raft.AppendEntriesRequest{
		Term: 2, LeaderID: "node-2",
		Entries: []raft.Entry{
			{Index: 1, Term: 2, Command: []byte("one")},
			{Index: 2, Term: 2, Command: []byte("two")},
		},
	}
	type rpcResult struct {
		response raft.AppendEntriesResponse
		err      error
	}
	finished := make(chan rpcResult, 1)
	go func() {
		response, err := node.HandleAppendEntries(context.Background(), request)
		finished <- rpcResult{response: response, err: err}
	}()

	if entries := <-store.started; !reflect.DeepEqual(entries, request.Entries) {
		t.Fatalf("Save entries = %+v, want %+v", entries, request.Entries)
	}
	select {
	case result := <-finished:
		t.Fatalf("AppendEntries returned before durable save: %+v", result)
	default:
	}
	close(store.release)
	result := <-finished
	if result.err != nil || !result.response.Success || result.response.MatchIndex != 2 {
		t.Fatalf("AppendEntries = %+v, %v", result.response, result.err)
	}
	request.Entries[0].Command[0] = 'X'
	if got := string(store.Entries()[0].Command); got != "one" {
		t.Fatalf("stored command changed to %q", got)
	}
	status := nodeStatus(t, node)
	if status.LastLogIndex != 2 || status.LastLogTerm != 2 || status.LeaderID != "node-2" {
		t.Fatalf("status after append = %+v", status)
	}
}

func TestFollowerReturnsConflictHintsWithoutChangingLog(t *testing.T) {
	initial := []raft.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 2},
		{Index: 3, Term: 2},
		{Index: 4, Term: 3},
	}
	store := newMemoryLogStore(initial)
	node := newFollowerNode(t, raft.HardState{CurrentTerm: 4}, store)
	defer node.Close()

	missing, err := node.HandleAppendEntries(context.Background(), raft.AppendEntriesRequest{
		Term: 4, LeaderID: "node-2", PrevLogIndex: 6, PrevLogTerm: 3,
	})
	if err != nil {
		t.Fatalf("missing-prefix AppendEntries: %v", err)
	}
	if missing.Success || missing.ConflictIndex != 5 || missing.ConflictTerm != 0 {
		t.Fatalf("missing-prefix response = %+v", missing)
	}

	mismatch, err := node.HandleAppendEntries(context.Background(), raft.AppendEntriesRequest{
		Term: 4, LeaderID: "node-2", PrevLogIndex: 3, PrevLogTerm: 1,
	})
	if err != nil {
		t.Fatalf("term-mismatch AppendEntries: %v", err)
	}
	if mismatch.Success || mismatch.ConflictIndex != 2 || mismatch.ConflictTerm != 2 {
		t.Fatalf("term-mismatch response = %+v", mismatch)
	}
	if got := store.Entries(); !reflect.DeepEqual(got, initial) {
		t.Fatalf("log changed after rejected appends: %+v", got)
	}
	if store.SaveCount() != 0 {
		t.Fatalf("Save count = %d, want 0", store.SaveCount())
	}
}

func TestFollowerAtomicallyRepairsDivergentSuffix(t *testing.T) {
	initial := []raft.Entry{
		{Index: 1, Term: 1, Command: []byte("one")},
		{Index: 2, Term: 1, Command: []byte("two")},
		{Index: 3, Term: 2, Command: []byte("old-three")},
		{Index: 4, Term: 2, Command: []byte("old-four")},
	}
	store := newMemoryLogStore(initial)
	hardState := newMemoryHardStateStore(raft.HardState{CurrentTerm: 3})
	node := newFollowerNodeWithStores(t, hardState, store)
	request := raft.AppendEntriesRequest{
		Term: 3, LeaderID: "node-2", PrevLogIndex: 2, PrevLogTerm: 1,
		Entries: []raft.Entry{
			{Index: 3, Term: 3, Command: []byte("new-three")},
			{Index: 4, Term: 3, Command: []byte("new-four")},
		},
	}
	response, err := node.HandleAppendEntries(context.Background(), request)
	if err != nil {
		t.Fatalf("HandleAppendEntries: %v", err)
	}
	if !response.Success || response.MatchIndex != 4 {
		t.Fatalf("response = %+v", response)
	}
	want := append(cloneTestEntries(initial[:2]), cloneTestEntries(request.Entries)...)
	if got := store.Entries(); !reflect.DeepEqual(got, want) {
		t.Fatalf("repaired log = %+v, want %+v", got, want)
	}
	if err := node.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	restarted := newFollowerNodeWithStores(t, hardState, store)
	defer restarted.Close()
	status := nodeStatus(t, restarted)
	if status.LastLogIndex != 4 || status.LastLogTerm != 3 || status.Role != raft.Follower {
		t.Fatalf("recovered status = %+v", status)
	}
}

func TestFollowerHeartbeatMatchesExistingPrefixWithoutRewrite(t *testing.T) {
	initial := []raft.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}}
	store := newMemoryLogStore(initial)
	node := newFollowerNode(t, raft.HardState{CurrentTerm: 3}, store)
	defer node.Close()
	response, err := node.HandleAppendEntries(context.Background(), raft.AppendEntriesRequest{
		Term: 3, LeaderID: "node-2", PrevLogIndex: 2, PrevLogTerm: 2,
	})
	if err != nil {
		t.Fatalf("HandleAppendEntries: %v", err)
	}
	if !response.Success || response.MatchIndex != 2 {
		t.Fatalf("heartbeat response = %+v", response)
	}
	if store.SaveCount() != 0 {
		t.Fatalf("heartbeat rewrote unchanged log %d times", store.SaveCount())
	}
}

func TestFollowerStopsAfterLogPersistenceFailure(t *testing.T) {
	saveFailure := errors.New("log disk failure")
	store := newMemoryLogStore(nil)
	store.saveErr = saveFailure
	node := newFollowerNode(t, raft.HardState{CurrentTerm: 1}, store)
	_, err := node.HandleAppendEntries(context.Background(), raft.AppendEntriesRequest{
		Term: 1, LeaderID: "node-2", Entries: []raft.Entry{{Index: 1, Term: 1}},
	})
	if !errors.Is(err, saveFailure) {
		t.Fatalf("HandleAppendEntries error = %v, want log disk failure", err)
	}
	if _, err := node.Status(context.Background()); !errors.Is(err, saveFailure) {
		t.Fatalf("Status error = %v, want log disk failure", err)
	}
	if err := node.Close(); !errors.Is(err, saveFailure) {
		t.Fatalf("Close error = %v, want log disk failure", err)
	}
}

func newFollowerNode(t *testing.T, hardState raft.HardState, logStore raft.LogStore) *raft.Node {
	t.Helper()
	return newFollowerNodeWithStores(t, newMemoryHardStateStore(hardState), logStore)
}

func newFollowerNodeWithStores(t *testing.T, hardState raft.HardStateStore, logStore raft.LogStore) *raft.Node {
	t.Helper()
	node, err := raft.NewNode(raft.NodeConfig{
		ID: "node-1", Members: testMembers, Transport: unavailableTransport{},
		ElectionTimeout: time.Second, Clock: manualClock{timer: newManualTimer()},
		Persistence: hardState, LogStore: logStore,
	})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return node
}

type blockingLogStore struct {
	entries []raft.Entry
	started chan []raft.Entry
	release chan struct{}
}

func (s *blockingLogStore) Load() ([]raft.Entry, error) {
	return cloneTestEntries(s.entries), nil
}

func (s *blockingLogStore) Save(entries []raft.Entry) error {
	s.started <- cloneTestEntries(entries)
	<-s.release
	s.entries = cloneTestEntries(entries)
	return nil
}

func (s *blockingLogStore) Entries() []raft.Entry {
	return cloneTestEntries(s.entries)
}
