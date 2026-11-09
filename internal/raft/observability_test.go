package raft_test

import (
	"context"
	"slices"
	"sync"
	"testing"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
)

func TestNodeEmitsStructuredEventsAndMetrics(t *testing.T) {
	transport := &scriptedTransport{grantFromTerm: 1}
	var mu sync.Mutex
	var kinds []raft.EventKind
	node, timer, _ := newElectionTestNode(t, electionNodeOptions{
		transport: transport,
		observer: func(event raft.Event) {
			mu.Lock()
			kinds = append(kinds, event.Kind)
			mu.Unlock()
		},
	})
	defer node.Close()
	timer.Fire()
	waitForNodeStatus(t, node, func(status raft.Status) bool { return status.Role == raft.Leader })
	waitForCondition(t, func() bool {
		metrics, err := node.Metrics(context.Background())
		return err == nil && len(metrics.PeerReplication) == 2 && metrics.PeerReplication[0].Lag == 0 && metrics.PeerReplication[1].Lag == 0
	}, "replication metrics")
	metrics, err := node.Metrics(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if metrics.NodeID != "node-1" || metrics.Role != "leader" || metrics.Term != 1 || metrics.LastLogIndex != 1 {
		t.Fatalf("metrics = %+v", metrics)
	}
	mu.Lock()
	defer mu.Unlock()
	if !slices.Contains(kinds, raft.EventElectionStarted) || !slices.Contains(kinds, raft.EventBecameLeader) || !slices.Contains(kinds, raft.EventReplicationProgress) {
		t.Fatalf("event kinds = %v", kinds)
	}
}
