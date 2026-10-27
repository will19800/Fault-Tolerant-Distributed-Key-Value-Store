package transport

import (
	"context"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
)

// PeerTransport sends Raft RPCs to another cluster member.
type PeerTransport interface {
	RequestVote(context.Context, raft.NodeID, raft.RequestVoteRequest) (raft.RequestVoteResponse, error)
	AppendEntries(context.Context, raft.NodeID, raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error)
}

// Handler receives Raft RPCs addressed to one cluster member.
type Handler interface {
	HandleRequestVote(context.Context, raft.RequestVoteRequest) (raft.RequestVoteResponse, error)
	HandleAppendEntries(context.Context, raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error)
}
