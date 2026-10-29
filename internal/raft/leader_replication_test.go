package raft_test

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/transport"
)

func TestLeaderRepairsDivergentFollowerAndCommitsCurrentTermEntry(t *testing.T) {
	leaderLog := []raft.Entry{
		{Index: 1, Term: 1, Command: []byte("one")},
		{Index: 2, Term: 2, Command: []byte("two")},
		{Index: 3, Term: 2, Command: []byte("three")},
	}
	divergentLog := []raft.Entry{
		{Index: 1, Term: 1, Command: []byte("one")},
		{Index: 2, Term: 1, Command: []byte("wrong")},
	}
	cluster := newReplicationCluster(t, map[raft.NodeID][]raft.Entry{
		"node-1": leaderLog,
		"node-2": divergentLog,
		"node-3": leaderLog,
	})
	defer cluster.Close()

	cluster.elect(t, "node-1")
	waitForCondition(t, func() bool {
		return slices.EqualFunc(cluster.stores["node-2"].Entries(), leaderLog, entriesEqual)
	}, "divergent follower repair")

	if status := nodeStatus(t, cluster.nodes["node-1"]); status.CommitIndex != 0 {
		t.Fatalf("old-term entries committed without a current-term entry: %+v", status)
	}
	if got := cluster.transport.previousIndexes("node-2"); !slices.Contains(got, 3) || !slices.Contains(got, 2) || !slices.Contains(got, 1) {
		t.Fatalf("follower retry previous indexes = %v, want conflict-hint jumps through 3, 2, and 1", got)
	}

	command := []byte("current-term")
	index, err := cluster.nodes["node-1"].Propose(context.Background(), command)
	if err != nil || index != 4 {
		t.Fatalf("Propose = %d, %v; want index 4", index, err)
	}
	command[0] = 'X'
	leader := waitForNodeStatus(t, cluster.nodes["node-1"], func(status raft.Status) bool {
		return status.CommitIndex == 4
	})
	if leader.Term != 3 || leader.LastLogIndex != 4 || leader.LastLogTerm != 3 {
		t.Fatalf("leader status after commit = %+v", leader)
	}
	waitForNodeStatus(t, cluster.nodes["node-2"], func(status raft.Status) bool {
		return status.CommitIndex == 4
	})
	if got := cluster.stores["node-1"].Entries()[3].Command; string(got) != "current-term" {
		t.Fatalf("stored command = %q, want owned proposal bytes", got)
	}
}

func TestLeaderCommitsWithMajorityButNotMinority(t *testing.T) {
	cluster := newReplicationCluster(t, nil)
	defer cluster.Close()
	cluster.elect(t, "node-1")

	cluster.network.Unregister("node-3")
	if index, err := cluster.nodes["node-1"].Propose(context.Background(), []byte("majority")); err != nil || index != 1 {
		t.Fatalf("majority Propose = %d, %v", index, err)
	}
	waitForNodeStatus(t, cluster.nodes["node-1"], func(status raft.Status) bool {
		return status.CommitIndex == 1
	})
	waitForNodeStatus(t, cluster.nodes["node-2"], func(status raft.Status) bool {
		return status.CommitIndex == 1
	})

	cluster.network.Unregister("node-2")
	if index, err := cluster.nodes["node-1"].Propose(context.Background(), []byte("minority")); err != nil || index != 2 {
		t.Fatalf("minority Propose = %d, %v", index, err)
	}
	waitForCondition(t, func() bool {
		return cluster.transport.appendCount("node-2") >= 2 && cluster.transport.appendCount("node-3") >= 2
	}, "minority replication attempts")
	if status := nodeStatus(t, cluster.nodes["node-1"]); status.CommitIndex != 1 || status.LastLogIndex != 2 {
		t.Fatalf("minority advanced commit index: %+v", status)
	}
}

func TestProposalRequiresLeaderAndStopsOnLocalPersistenceFailure(t *testing.T) {
	node, _ := newTestNode(t)
	if _, err := node.Propose(context.Background(), []byte("no leader")); err != raft.ErrNotLeader {
		t.Fatalf("follower Propose error = %v, want ErrNotLeader", err)
	}
	_ = node.Close()

	store := newMemoryLogStore(nil)
	cluster := newReplicationClusterWithStores(t, map[raft.NodeID]*memoryLogStore{
		"node-1": store,
	})
	defer cluster.Close()
	cluster.elect(t, "node-1")
	store.saveErr = context.Canceled
	if _, err := cluster.nodes["node-1"].Propose(context.Background(), []byte("fails")); err == nil {
		t.Fatal("Propose returned nil persistence error")
	}
	if err := cluster.nodes["node-1"].Close(); err == nil {
		t.Fatal("Close returned nil after log persistence failure")
	}
}

func entriesEqual(left, right raft.Entry) bool {
	return left.Index == right.Index && left.Term == right.Term && slices.Equal(left.Command, right.Command)
}

type replicationCluster struct {
	network   *transport.MemoryNetwork
	transport *recordingTransport
	nodes     map[raft.NodeID]*raft.Node
	timers    map[raft.NodeID]*recordingTimer
	stores    map[raft.NodeID]*memoryLogStore
}

func newReplicationCluster(t *testing.T, logs map[raft.NodeID][]raft.Entry) *replicationCluster {
	t.Helper()
	stores := make(map[raft.NodeID]*memoryLogStore, len(testMembers))
	for _, id := range testMembers {
		stores[id] = newMemoryLogStore(logs[id])
	}
	return newReplicationClusterWithStores(t, stores)
}

func newReplicationClusterWithStores(t *testing.T, supplied map[raft.NodeID]*memoryLogStore) *replicationCluster {
	t.Helper()
	network := transport.NewMemoryNetwork()
	cluster := &replicationCluster{
		network: network, transport: &recordingTransport{network: network},
		nodes: make(map[raft.NodeID]*raft.Node), timers: make(map[raft.NodeID]*recordingTimer),
		stores: make(map[raft.NodeID]*memoryLogStore),
	}
	for _, id := range testMembers {
		store := supplied[id]
		if store == nil {
			store = newMemoryLogStore(nil)
		}
		clock := newTimerClock()
		node, err := raft.NewNode(raft.NodeConfig{
			ID: id, Members: testMembers, Transport: cluster.transport,
			ElectionTimeout: time.Second, MaximumElectionTimeout: time.Second,
			HeartbeatInterval: 100 * time.Millisecond, Clock: clock,
			Persistence: newMemoryHardStateStore(raft.HardState{CurrentTerm: 2}), LogStore: store,
		})
		if err != nil {
			t.Fatalf("NewNode(%q): %v", id, err)
		}
		if err := network.Register(id, node); err != nil {
			t.Fatalf("Register(%q): %v", id, err)
		}
		cluster.nodes[id], cluster.timers[id], cluster.stores[id] = node, clock.Timer(0), store
	}
	return cluster
}

func (c *replicationCluster) elect(t *testing.T, id raft.NodeID) {
	t.Helper()
	c.timers[id].Fire()
	waitForNodeStatus(t, c.nodes[id], func(status raft.Status) bool {
		return status.Role == raft.Leader && status.Term == 3
	})
}

func (c *replicationCluster) Close() {
	for id, node := range c.nodes {
		c.network.Unregister(id)
		_ = node.Close()
	}
}

type recordingTransport struct {
	network *transport.MemoryNetwork
	mu      sync.Mutex
	appends map[raft.NodeID][]raft.AppendEntriesRequest
}

func (t *recordingTransport) RequestVote(ctx context.Context, target raft.NodeID, request raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	return t.network.RequestVote(ctx, target, request)
}

func (t *recordingTransport) AppendEntries(ctx context.Context, target raft.NodeID, request raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	t.mu.Lock()
	if t.appends == nil {
		t.appends = make(map[raft.NodeID][]raft.AppendEntriesRequest)
	}
	t.appends[target] = append(t.appends[target], request)
	t.mu.Unlock()
	return t.network.AppendEntries(ctx, target, request)
}

func (t *recordingTransport) previousIndexes(target raft.NodeID) []raft.LogIndex {
	t.mu.Lock()
	defer t.mu.Unlock()
	indexes := make([]raft.LogIndex, len(t.appends[target]))
	for index, request := range t.appends[target] {
		indexes[index] = request.PrevLogIndex
	}
	return indexes
}

func (t *recordingTransport) appendCount(target raft.NodeID) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.appends[target])
}
