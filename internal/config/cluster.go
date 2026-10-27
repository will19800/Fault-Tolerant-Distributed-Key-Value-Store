package config

import (
	"fmt"
	"strings"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
)

// Member maps a stable Raft node ID to its node-to-node address.
type Member struct {
	ID          raft.NodeID
	RaftAddress string
}

// Cluster describes fixed membership and identifies the local member.
type Cluster struct {
	LocalID raft.NodeID
	Members []Member
}

// ParseCluster parses comma-separated ID=host:port members and validates the
// resulting fixed membership.
func ParseCluster(localID, members string) (Cluster, error) {
	cluster := Cluster{LocalID: raft.NodeID(strings.TrimSpace(localID))}
	for _, encodedMember := range strings.Split(members, ",") {
		parts := strings.SplitN(strings.TrimSpace(encodedMember), "=", 2)
		if len(parts) != 2 {
			return Cluster{}, fmt.Errorf("invalid member %q: expected ID=host:port", encodedMember)
		}
		cluster.Members = append(cluster.Members, Member{
			ID:          raft.NodeID(strings.TrimSpace(parts[0])),
			RaftAddress: strings.TrimSpace(parts[1]),
		})
	}
	if err := cluster.Validate(); err != nil {
		return Cluster{}, err
	}
	return cluster, nil
}

// Validate checks fixed-membership invariants for the supported cluster sizes.
func (c Cluster) Validate() error {
	if err := c.LocalID.Validate(); err != nil {
		return fmt.Errorf("local node ID: %w", err)
	}
	if len(c.Members) != 3 && len(c.Members) != 5 {
		return fmt.Errorf("cluster must contain exactly 3 or 5 members, got %d", len(c.Members))
	}

	ids := make(map[raft.NodeID]struct{}, len(c.Members))
	addresses := make(map[string]struct{}, len(c.Members))
	localFound := false
	for position, member := range c.Members {
		if err := member.ID.Validate(); err != nil {
			return fmt.Errorf("member %d ID: %w", position, err)
		}
		if _, exists := ids[member.ID]; exists {
			return fmt.Errorf("duplicate member ID %q", member.ID)
		}
		ids[member.ID] = struct{}{}

		if err := validateAddress(member.RaftAddress); err != nil {
			return fmt.Errorf("member %q Raft address: %w", member.ID, err)
		}
		if _, exists := addresses[member.RaftAddress]; exists {
			return fmt.Errorf("duplicate Raft address %q", member.RaftAddress)
		}
		addresses[member.RaftAddress] = struct{}{}
		localFound = localFound || member.ID == c.LocalID
	}
	if !localFound {
		return fmt.Errorf("local node ID %q is not a cluster member", c.LocalID)
	}
	return nil
}
