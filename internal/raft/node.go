package raft

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// DefaultElectionTimeout is the fixed timeout used until randomized election
// timing is added with leader election.
const DefaultElectionTimeout = 300 * time.Millisecond

// ErrNodeStopped is returned when an operation targets a closed node.
var ErrNodeStopped = errors.New("Raft node stopped")

// NodeConfig contains the dependencies needed by the Raft event loop.
type NodeConfig struct {
	ID              NodeID
	ElectionTimeout time.Duration
	Clock           Clock
}

// Node serializes timers, RPCs, and state queries through one event loop.
type Node struct {
	events   chan any
	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
}

// NewNode validates config and starts a follower event loop.
func NewNode(config NodeConfig) (*Node, error) {
	if err := config.ID.Validate(); err != nil {
		return nil, fmt.Errorf("node ID: %w", err)
	}
	if config.ElectionTimeout <= 0 {
		return nil, fmt.Errorf("election timeout must be positive: %s", config.ElectionTimeout)
	}
	if config.Clock == nil {
		config.Clock = systemClock{}
	}
	timer := config.Clock.NewTimer(config.ElectionTimeout)
	if timer == nil {
		return nil, errors.New("clock returned a nil timer")
	}

	node := &Node{
		events: make(chan any),
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	go node.run(config, timer)
	return node, nil
}

// Status returns a consistent snapshot from the event loop.
func (n *Node) Status(ctx context.Context) (Status, error) {
	reply := make(chan Status, 1)
	if err := n.submit(ctx, statusEvent{reply: reply}); err != nil {
		return Status{}, err
	}
	select {
	case status := <-reply:
		return status, nil
	case <-ctx.Done():
		return Status{}, ctx.Err()
	case <-n.done:
		return Status{}, ErrNodeStopped
	}
}

// HandleRequestVote serializes a vote request through the event loop. Voting
// policy is added with the leader-election implementation.
func (n *Node) HandleRequestVote(ctx context.Context, request RequestVoteRequest) (RequestVoteResponse, error) {
	if err := request.Validate(); err != nil {
		return RequestVoteResponse{}, fmt.Errorf("request vote request: %w", err)
	}
	reply := make(chan voteResult, 1)
	if err := n.submit(ctx, requestVoteEvent{request: request, reply: reply}); err != nil {
		return RequestVoteResponse{}, err
	}
	select {
	case result := <-reply:
		return result.response, result.err
	case <-ctx.Done():
		return RequestVoteResponse{}, ctx.Err()
	case <-n.done:
		return RequestVoteResponse{}, ErrNodeStopped
	}
}

// HandleAppendEntries serializes an append request through the event loop. Log
// matching and replication are added with the follower-log implementation.
func (n *Node) HandleAppendEntries(ctx context.Context, request AppendEntriesRequest) (AppendEntriesResponse, error) {
	if err := request.Validate(); err != nil {
		return AppendEntriesResponse{}, fmt.Errorf("append entries request: %w", err)
	}
	reply := make(chan appendResult, 1)
	if err := n.submit(ctx, appendEntriesEvent{request: request, reply: reply}); err != nil {
		return AppendEntriesResponse{}, err
	}
	select {
	case result := <-reply:
		return result.response, result.err
	case <-ctx.Done():
		return AppendEntriesResponse{}, ctx.Err()
	case <-n.done:
		return AppendEntriesResponse{}, ErrNodeStopped
	}
}

// Close stops the event loop and its timer. It is safe to call more than once.
func (n *Node) Close() error {
	n.stopOnce.Do(func() { close(n.stop) })
	<-n.done
	return nil
}

func (n *Node) submit(ctx context.Context, event any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case n.events <- event:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return ErrNodeStopped
	}
}

func (n *Node) run(config NodeConfig, electionTimer Timer) {
	defer close(n.done)
	defer electionTimer.Stop()

	state := Status{ID: config.ID, Role: Follower}
	for {
		select {
		case <-electionTimer.C():
			state.Term++
			state.Role = Candidate
			state.LeaderID = ""
			electionTimer.Reset(config.ElectionTimeout)
		case rawEvent := <-n.events:
			switch event := rawEvent.(type) {
			case statusEvent:
				event.reply <- state
			case requestVoteEvent:
				if event.request.Term > state.Term {
					state.Term = event.request.Term
					state.Role = Follower
					state.LeaderID = ""
					electionTimer.Reset(config.ElectionTimeout)
				}
				event.reply <- voteResult{response: RequestVoteResponse{Term: state.Term}}
			case appendEntriesEvent:
				if event.request.Term >= state.Term {
					state.Term = event.request.Term
					state.Role = Follower
					state.LeaderID = event.request.LeaderID
					electionTimer.Reset(config.ElectionTimeout)
				}
				event.reply <- appendResult{response: AppendEntriesResponse{
					Term: state.Term, ConflictIndex: 1,
				}}
			}
		case <-n.stop:
			return
		}
	}
}

type statusEvent struct {
	reply chan<- Status
}

type requestVoteEvent struct {
	request RequestVoteRequest
	reply   chan<- voteResult
}

type voteResult struct {
	response RequestVoteResponse
	err      error
}

type appendEntriesEvent struct {
	request AppendEntriesRequest
	reply   chan<- appendResult
}

type appendResult struct {
	response AppendEntriesResponse
	err      error
}
