package raft_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/transport"
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

func TestNodeRecoversSnapshotAndRetainedLog(t *testing.T) {
	for _, interrupted := range []bool{false, true} {
		t.Run(map[bool]string{false: "compacted log", true: "interrupted compaction"}[interrupted], func(t *testing.T) {
			entries := []raft.Entry{{Index: 1, Term: 1, Command: []byte("one")}, {Index: 2, Term: 1, Command: []byte("two")}}
			logStore := newMemoryLogStore(entries[1:])
			logStore.baseIndex, logStore.baseTerm = 1, 1
			if interrupted {
				logStore = newMemoryLogStore(entries)
			}
			snapshots := &memorySnapshotStore{snapshots: []raft.Snapshot{{LastIncludedIndex: 1, LastIncludedTerm: 1, State: []byte("one")}}}
			machine := newMemoryStateMachine()
			node, err := raft.NewNode(raft.NodeConfig{
				ID: "node-1", Members: testMembers, Transport: unavailableTransport{},
				ElectionTimeout: time.Second, Clock: manualClock{timer: newManualTimer()},
				Persistence: newMemoryHardStateStore(raft.HardState{CurrentTerm: 1}),
				LogStore:    logStore, StateMachine: machine, SnapshotStore: snapshots,
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = node.Close() })
			status, _ := node.Status(context.Background())
			if status.SnapshotIndex != 1 || status.LastApplied != 1 || status.LastLogIndex != 2 {
				t.Fatalf("recovered status = %+v", status)
			}
			response, err := node.HandleAppendEntries(context.Background(), raft.AppendEntriesRequest{
				Term: 1, LeaderID: "node-2", PrevLogIndex: 2, PrevLogTerm: 1, LeaderCommit: 2,
			})
			if err != nil || !response.Success {
				t.Fatalf("commit retained entry = %+v, %v", response, err)
			}
			if got := machine.Commands(); !reflect.DeepEqual(got, [][]byte{[]byte("one"), []byte("two")}) {
				t.Fatalf("restored commands = %q", got)
			}
			if base, term := logStore.Base(); base != 1 || term != 1 {
				t.Fatalf("recovered log base = (%d, %d)", base, term)
			}
		})
	}
}

func TestFollowerInstallsSnapshot(t *testing.T) {
	logStore := newMemoryLogStore([]raft.Entry{{Index: 1, Term: 1, Command: []byte("old")}})
	snapshots := &memorySnapshotStore{}
	machine := newMemoryStateMachine()
	node, err := raft.NewNode(raft.NodeConfig{
		ID: "node-1", Members: testMembers, Transport: unavailableTransport{},
		ElectionTimeout: time.Second, Clock: manualClock{timer: newManualTimer()},
		Persistence: newMemoryHardStateStore(raft.HardState{CurrentTerm: 1}),
		LogStore:    logStore, StateMachine: machine, SnapshotStore: snapshots,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Close() })
	response, err := node.HandleInstallSnapshot(context.Background(), raft.InstallSnapshotRequest{
		Term: 2, LeaderID: "node-2", LastIncludedIndex: 3, LastIncludedTerm: 2, State: []byte("current"),
	})
	if err != nil || response.MatchIndex != 3 {
		t.Fatalf("HandleInstallSnapshot = %+v, %v", response, err)
	}
	status, _ := node.Status(context.Background())
	if status.SnapshotIndex != 3 || status.LastApplied != 3 || status.CommitIndex != 3 {
		t.Fatalf("installed status = %+v", status)
	}
	if got := machine.Commands(); !reflect.DeepEqual(got, [][]byte{[]byte("current")}) {
		t.Fatalf("installed state = %q", got)
	}
}

func TestLeaderSendsSnapshotToLaggingFollower(t *testing.T) {
	cluster := newReplicationCluster(t, nil)
	t.Cleanup(cluster.Close)
	cluster.elect(t, "node-1")
	cluster.network.Unregister("node-3")
	if _, err := cluster.nodes["node-1"].Propose(context.Background(), []byte("covered")); err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.nodes["node-1"].TakeSnapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := cluster.network.Register("node-3", cluster.nodes["node-3"]); err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.nodes["node-1"].Propose(context.Background(), []byte("retained")); err != nil {
		t.Fatal(err)
	}
	waitForNodeStatus(t, cluster.nodes["node-3"], func(status raft.Status) bool {
		return status.SnapshotIndex >= 2 && status.LastApplied >= 3
	})
	if got := cluster.machines["node-3"].Commands(); !reflect.DeepEqual(got, [][]byte{[]byte("covered"), []byte("retained")}) {
		t.Fatalf("caught-up follower state = %q", got)
	}
}

func TestSnapshotRestartScenarios(t *testing.T) {
	tests := []struct {
		name  string
		nodes []raft.NodeID
		elect bool
	}{
		{name: "follower restart", nodes: []raft.NodeID{"node-2"}},
		{name: "former leader restarts as follower", nodes: []raft.NodeID{"node-1"}},
		{name: "full cluster restart", nodes: testMembers, elect: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			network := transport.NewMemoryNetwork()
			nodes := make(map[raft.NodeID]*raft.Node)
			machines := make(map[raft.NodeID]*memoryStateMachine)
			clocks := make(map[raft.NodeID]*timerClock)
			for _, id := range test.nodes {
				logStore := newMemoryLogStore([]raft.Entry{{Index: 2, Term: 2, Command: []byte("two")}})
				logStore.baseIndex, logStore.baseTerm = 1, 2
				machine := newMemoryStateMachine()
				clock := newTimerClock()
				node, err := raft.NewNode(raft.NodeConfig{
					ID: id, Members: testMembers, Transport: network,
					ElectionTimeout: time.Second, MaximumElectionTimeout: time.Second,
					HeartbeatInterval: 100 * time.Millisecond, Clock: clock,
					Persistence: newMemoryHardStateStore(raft.HardState{CurrentTerm: 2}),
					LogStore:    logStore, StateMachine: machine,
					SnapshotStore: &memorySnapshotStore{snapshots: []raft.Snapshot{{LastIncludedIndex: 1, LastIncludedTerm: 2, State: []byte("one")}}},
				})
				if err != nil {
					t.Fatal(err)
				}
				if err := network.Register(id, node); err != nil {
					t.Fatal(err)
				}
				nodes[id], machines[id], clocks[id] = node, machine, clock
			}
			t.Cleanup(func() {
				for id, node := range nodes {
					network.Unregister(id)
					_ = node.Close()
				}
			})
			for id, node := range nodes {
				status, _ := node.Status(context.Background())
				if status.Role != raft.Follower || status.SnapshotIndex != 1 || status.LastApplied != 1 || status.LastLogIndex != 2 {
					t.Fatalf("%s recovered status = %+v", id, status)
				}
			}
			if !test.elect {
				return
			}
			clocks["node-1"].Timer(0).Fire()
			waitForNodeStatus(t, nodes["node-1"], func(status raft.Status) bool { return status.Role == raft.Leader })
			for id, node := range nodes {
				waitForNodeStatus(t, node, func(status raft.Status) bool { return status.LastApplied >= 3 })
				if got := machines[id].Commands(); !reflect.DeepEqual(got, [][]byte{[]byte("one"), []byte("two")}) {
					t.Fatalf("%s recovered state = %q", id, got)
				}
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

func (s *memorySnapshotStore) Load() (raft.Snapshot, error) {
	if len(s.snapshots) == 0 {
		return raft.Snapshot{}, nil
	}
	snapshot := s.snapshots[len(s.snapshots)-1]
	snapshot.State = append([]byte(nil), snapshot.State...)
	return snapshot, nil
}

func (s *memorySnapshotStore) Snapshots() []raft.Snapshot {
	return append([]raft.Snapshot(nil), s.snapshots...)
}
