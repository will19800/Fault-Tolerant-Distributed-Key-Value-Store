package transport

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
)

var (
	// ErrPeerNotFound means no handler is registered for the target node.
	ErrPeerNotFound = errors.New("peer not found")
	// ErrPeerRegistered means the node ID already has a handler.
	ErrPeerRegistered = errors.New("peer already registered")
)

// MemoryNetwork routes calls directly to registered handlers in the caller's
// goroutine. Sequential calls are therefore delivered in caller order.
type MemoryNetwork struct {
	mu       sync.RWMutex
	handlers map[raft.NodeID]Handler
}

// NewMemoryNetwork returns an empty in-memory peer network.
func NewMemoryNetwork() *MemoryNetwork {
	return &MemoryNetwork{handlers: make(map[raft.NodeID]Handler)}
}

// Register adds the handler for id. An ID can only be registered once.
func (n *MemoryNetwork) Register(id raft.NodeID, handler Handler) error {
	if err := id.Validate(); err != nil {
		return fmt.Errorf("peer ID: %w", err)
	}
	if handler == nil {
		return fmt.Errorf("peer %q: handler must not be nil", id)
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if _, exists := n.handlers[id]; exists {
		return fmt.Errorf("peer %q: %w", id, ErrPeerRegistered)
	}
	n.handlers[id] = handler
	return nil
}

// Unregister removes the handler for id. Calls already in progress continue.
func (n *MemoryNetwork) Unregister(id raft.NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.handlers, id)
}

// RequestVote delivers a vote request to target.
func (n *MemoryNetwork) RequestVote(ctx context.Context, target raft.NodeID, request raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	if err := ctx.Err(); err != nil {
		return raft.RequestVoteResponse{}, err
	}
	if err := request.Validate(); err != nil {
		return raft.RequestVoteResponse{}, fmt.Errorf("request vote request: %w", err)
	}
	handler, err := n.handler(target)
	if err != nil {
		return raft.RequestVoteResponse{}, err
	}

	response, err := handler.HandleRequestVote(ctx, request)
	if err != nil {
		return raft.RequestVoteResponse{}, err
	}
	if err := response.Validate(); err != nil {
		return raft.RequestVoteResponse{}, fmt.Errorf("request vote response from %q: %w", target, err)
	}
	return response, nil
}

// AppendEntries delivers an append request to target.
func (n *MemoryNetwork) AppendEntries(ctx context.Context, target raft.NodeID, request raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	if err := ctx.Err(); err != nil {
		return raft.AppendEntriesResponse{}, err
	}
	if err := request.Validate(); err != nil {
		return raft.AppendEntriesResponse{}, fmt.Errorf("append entries request: %w", err)
	}
	handler, err := n.handler(target)
	if err != nil {
		return raft.AppendEntriesResponse{}, err
	}

	response, err := handler.HandleAppendEntries(ctx, cloneAppendEntriesRequest(request))
	if err != nil {
		return raft.AppendEntriesResponse{}, err
	}
	if err := response.Validate(); err != nil {
		return raft.AppendEntriesResponse{}, fmt.Errorf("append entries response from %q: %w", target, err)
	}
	return response, nil
}

func (n *MemoryNetwork) handler(target raft.NodeID) (Handler, error) {
	if err := target.Validate(); err != nil {
		return nil, fmt.Errorf("target peer ID: %w", err)
	}
	n.mu.RLock()
	handler, exists := n.handlers[target]
	n.mu.RUnlock()
	if !exists {
		return nil, fmt.Errorf("peer %q: %w", target, ErrPeerNotFound)
	}
	return handler, nil
}

func cloneAppendEntriesRequest(request raft.AppendEntriesRequest) raft.AppendEntriesRequest {
	request.Entries = slices.Clone(request.Entries)
	for index := range request.Entries {
		request.Entries[index].Command = slices.Clone(request.Entries[index].Command)
	}
	return request
}
