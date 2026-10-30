package raft_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
)

func TestFiveNodeClusterProgressesWithEveryUnavailablePair(t *testing.T) {
	members := []raft.NodeID{"node-1", "node-2", "node-3", "node-4", "node-5"}
	for first := 0; first < len(members); first++ {
		for second := first + 1; second < len(members); second++ {
			down := map[raft.NodeID]bool{members[first]: true, members[second]: true}
			name := fmt.Sprintf("%s_%s", members[first], members[second])
			t.Run(name, func(t *testing.T) {
				cluster := newReplicationClusterWithMembers(t, members, nil, nil)
				defer cluster.Close()
				cluster.elect(t, "node-1")
				baselineIndex, err := cluster.nodes["node-1"].Propose(context.Background(), []byte("before-failure"))
				if err != nil {
					t.Fatalf("baseline Propose: %v", err)
				}
				for _, id := range members {
					waitForNodeStatus(t, cluster.nodes[id], func(status raft.Status) bool {
						return status.LastLogIndex >= baselineIndex
					})
				}
				for _, id := range members {
					if down[id] {
						cluster.stop(id)
					}
				}

				leader := raft.NodeID("node-1")
				if down[leader] {
					for _, candidate := range members {
						if !down[candidate] {
							leader = candidate
							break
						}
					}
					cluster.elect(t, leader)
				}
				index, err := cluster.nodes[leader].Propose(context.Background(), []byte("after-failure"))
				if err != nil {
					t.Fatalf("surviving majority Propose: %v", err)
				}
				readIndex, err := cluster.nodes[leader].ReadIndex(context.Background())
				if err != nil || readIndex < index {
					t.Fatalf("surviving majority ReadIndex = %d, %v; want at least %d", readIndex, err, index)
				}
			})
		}
	}
}
