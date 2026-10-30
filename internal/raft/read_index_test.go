package raft_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
)

func TestReadIndexConfirmsQuorumAndAppliedState(t *testing.T) {
	cluster := newReplicationCluster(t, nil)
	defer cluster.Close()
	cluster.elect(t, "node-1")

	index, err := cluster.nodes["node-1"].ReadIndex(context.Background())
	if err != nil {
		t.Fatalf("ReadIndex: %v", err)
	}
	if index != 1 {
		t.Fatalf("ReadIndex = %d, want committed leader no-op index 1", index)
	}
	status := nodeStatus(t, cluster.nodes["node-1"])
	if status.CommitIndex != index || status.LastApplied < index {
		t.Fatalf("read barrier returned before application: %+v", status)
	}
}

func TestFollowerReadReturnsKnownLeaderHint(t *testing.T) {
	cluster := newReplicationCluster(t, nil)
	defer cluster.Close()
	cluster.elect(t, "node-1")
	waitForNodeStatus(t, cluster.nodes["node-2"], func(status raft.Status) bool {
		return status.LeaderID == "node-1"
	})

	_, err := cluster.nodes["node-2"].ReadIndex(context.Background())
	if !errors.Is(err, raft.ErrNotLeader) {
		t.Fatalf("follower ReadIndex error = %v, want ErrNotLeader", err)
	}
	var notLeader raft.NotLeaderError
	if !errors.As(err, &notLeader) || notLeader.LeaderID != "node-1" {
		t.Fatalf("follower leader hint = %+v, want node-1", notLeader)
	}
}

func TestIsolatedLeaderCannotServeRead(t *testing.T) {
	cluster := newReplicationCluster(t, nil)
	defer cluster.Close()
	cluster.elect(t, "node-1")
	waitForNodeStatus(t, cluster.nodes["node-1"], func(status raft.Status) bool {
		return status.CommitIndex == 1
	})
	cluster.network.Unregister("node-2")
	cluster.network.Unregister("node-3")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if index, err := cluster.nodes["node-1"].ReadIndex(ctx); index != 0 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("isolated ReadIndex = %d, %v; want deadline", index, err)
	}
}
