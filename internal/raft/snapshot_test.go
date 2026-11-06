package raft_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
)

func TestTakeSnapshotPersistsBeforeCompacting(t *testing.T) {
	tests := []struct {
		name        string
		snapshotErr error
		compactErr  error
		wantBase    raft.LogIndex
		wantEntries int
		wantSaved   int
	}{
		{name: "snapshot publication fails", snapshotErr: errors.New("snapshot unavailable"), wantEntries: 1},
		{name: "compaction fails", compactErr: errors.New("log unavailable"), wantEntries: 1, wantSaved: 1},
		{name: "success", wantBase: 1, wantSaved: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logStore := newMemoryLogStore(nil)
			logStore.compactErr = test.compactErr
			snapshots := &memorySnapshotStore{saveErr: test.snapshotErr}
			timer := newManualTimer()
			node, err := raft.NewNode(raft.NodeConfig{
				ID: "node-1", Members: testMembers, Transport: unavailableTransport{},
				ElectionTimeout: time.Second, Clock: manualClock{timer: timer},
				Persistence: newMemoryHardStateStore(raft.HardState{}), LogStore: logStore,
				StateMachine: newMemoryStateMachine(), SnapshotStore: snapshots,
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = node.Close() })

			response, err := node.HandleAppendEntries(context.Background(), raft.AppendEntriesRequest{
				Term: 1, LeaderID: "node-2", Entries: []raft.Entry{{Index: 1, Term: 1, Command: []byte("set")}}, LeaderCommit: 1,
			})
			if err != nil || !response.Success {
				t.Fatalf("HandleAppendEntries = %+v, %v", response, err)
			}
			index, snapshotErr := node.TakeSnapshot(context.Background())
			if test.snapshotErr != nil || test.compactErr != nil {
				if snapshotErr == nil {
					t.Fatal("TakeSnapshot returned nil error")
				}
			} else if snapshotErr != nil || index != 1 {
				t.Fatalf("TakeSnapshot = %d, %v; want 1, nil", index, snapshotErr)
			}
			base, term := logStore.Base()
			if base != test.wantBase || (base != 0 && term != 1) {
				t.Fatalf("log base = (%d, %d), want index %d", base, term, test.wantBase)
			}
			if entries := logStore.Entries(); len(entries) != test.wantEntries {
				t.Fatalf("retained entries = %+v, want length %d", entries, test.wantEntries)
			}
			if got := snapshots.Snapshots(); len(got) != test.wantSaved {
				t.Fatalf("saved snapshots = %+v, want %d", got, test.wantSaved)
			} else if len(got) == 1 && !reflect.DeepEqual(got[0], raft.Snapshot{LastIncludedIndex: 1, LastIncludedTerm: 1, State: []byte("set")}) {
				t.Fatalf("snapshot = %+v", got[0])
			}
		})
	}
}

type memorySnapshotStore struct {
	snapshots []raft.Snapshot
	saveErr   error
}

func (s *memorySnapshotStore) Save(snapshot raft.Snapshot) error {
	if s.saveErr != nil {
		return s.saveErr
	}
	snapshot.State = append([]byte(nil), snapshot.State...)
	s.snapshots = append(s.snapshots, snapshot)
	return nil
}

func (s *memorySnapshotStore) Snapshots() []raft.Snapshot {
	return append([]raft.Snapshot(nil), s.snapshots...)
}
