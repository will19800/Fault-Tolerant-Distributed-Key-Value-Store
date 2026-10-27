package raft_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
)

func TestNodeStartsAsFollower(t *testing.T) {
	node, _ := newTestNode(t)
	defer node.Close()

	status := nodeStatus(t, node)
	if want := (raft.Status{ID: "node-1", Role: raft.Follower}); status != want {
		t.Fatalf("status = %+v, want %+v", status, want)
	}
}

func TestElectionTimeoutTransitionsToCandidate(t *testing.T) {
	node, timer := newTestNode(t)
	defer node.Close()

	timer.Fire()
	status := nodeStatus(t, node)
	if status.Role != raft.Candidate || status.Term != 1 || status.LeaderID != "" {
		t.Fatalf("status after timeout = %+v", status)
	}
	timer.Fire()
	status = nodeStatus(t, node)
	if status.Role != raft.Candidate || status.Term != 2 {
		t.Fatalf("status after second timeout = %+v", status)
	}
	if got := timer.ResetCount(); got != 2 {
		t.Fatalf("timer reset count = %d, want 2", got)
	}
}

func TestHigherTermRPCTransitionsNodeToFollower(t *testing.T) {
	node, timer := newTestNode(t)
	defer node.Close()
	timer.Fire()

	appendResponse, err := node.HandleAppendEntries(context.Background(), raft.AppendEntriesRequest{Term: 5, LeaderID: "node-2"})
	if err != nil {
		t.Fatalf("HandleAppendEntries: %v", err)
	}
	if appendResponse.Term != 5 || appendResponse.Success || appendResponse.ConflictIndex != 1 {
		t.Fatalf("append response = %+v", appendResponse)
	}
	status := nodeStatus(t, node)
	if status.Role != raft.Follower || status.Term != 5 || status.LeaderID != "node-2" {
		t.Fatalf("status after append = %+v", status)
	}

	voteResponse, err := node.HandleRequestVote(context.Background(), raft.RequestVoteRequest{Term: 7, CandidateID: "node-3"})
	if err != nil {
		t.Fatalf("HandleRequestVote: %v", err)
	}
	if voteResponse.Term != 7 || voteResponse.VoteGranted {
		t.Fatalf("vote response = %+v", voteResponse)
	}
	status = nodeStatus(t, node)
	if status.Role != raft.Follower || status.Term != 7 || status.LeaderID != "" {
		t.Fatalf("status after vote request = %+v", status)
	}
}

func TestStaleRPCDoesNotMoveTermBackward(t *testing.T) {
	node, _ := newTestNode(t)
	defer node.Close()
	if _, err := node.HandleRequestVote(context.Background(), raft.RequestVoteRequest{Term: 5, CandidateID: "node-2"}); err != nil {
		t.Fatalf("higher-term HandleRequestVote: %v", err)
	}
	response, err := node.HandleAppendEntries(context.Background(), raft.AppendEntriesRequest{Term: 3, LeaderID: "node-3"})
	if err != nil {
		t.Fatalf("stale HandleAppendEntries: %v", err)
	}
	if response.Term != 5 || response.Success {
		t.Fatalf("stale append response = %+v", response)
	}
	status := nodeStatus(t, node)
	if status.Term != 5 || status.LeaderID != "" {
		t.Fatalf("status after stale append = %+v", status)
	}
}

func TestNodeSerializesConcurrentTermUpdates(t *testing.T) {
	node, _ := newTestNode(t)
	defer node.Close()

	var wait sync.WaitGroup
	for term := raft.Term(1); term <= 50; term++ {
		wait.Add(1)
		go func(term raft.Term) {
			defer wait.Done()
			response, err := node.HandleRequestVote(context.Background(), raft.RequestVoteRequest{Term: term, CandidateID: "node-2"})
			if err != nil {
				t.Errorf("HandleRequestVote term %d: %v", term, err)
				return
			}
			if response.Term < term {
				t.Errorf("response term = %d, want at least %d", response.Term, term)
			}
		}(term)
	}
	wait.Wait()
	if status := nodeStatus(t, node); status.Term != 50 {
		t.Fatalf("final term = %d, want 50", status.Term)
	}
}

func TestNodeHonorsCancellationAndStopsTimer(t *testing.T) {
	node, timer := newTestNode(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := node.Status(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Status error = %v, want context.Canceled", err)
	}
	if err := node.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !timer.Stopped() {
		t.Fatal("timer was not stopped")
	}
	if _, err := node.Status(context.Background()); !errors.Is(err, raft.ErrNodeStopped) {
		t.Fatalf("Status after Close error = %v, want ErrNodeStopped", err)
	}
	if err := node.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestNewNodeValidatesConfig(t *testing.T) {
	invalid := []raft.NodeConfig{
		{ElectionTimeout: time.Second},
		{ID: "node-1"},
		{ID: "node-1", ElectionTimeout: -time.Second},
	}
	for _, config := range invalid {
		if node, err := raft.NewNode(config); err == nil {
			node.Close()
			t.Fatalf("NewNode(%+v) returned nil error", config)
		}
	}
}

func newTestNode(t *testing.T) (*raft.Node, *manualTimer) {
	t.Helper()
	timer := newManualTimer()
	node, err := raft.NewNode(raft.NodeConfig{
		ID:              "node-1",
		ElectionTimeout: time.Second,
		Clock:           manualClock{timer: timer},
	})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return node, timer
}

func nodeStatus(t *testing.T, node *raft.Node) raft.Status {
	t.Helper()
	status, err := node.Status(context.Background())
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	return status
}

type manualClock struct {
	timer *manualTimer
}

func (c manualClock) NewTimer(time.Duration) raft.Timer {
	return c.timer
}

type manualTimer struct {
	channel chan time.Time
	mu      sync.Mutex
	resets  int
	stopped bool
}

func newManualTimer() *manualTimer {
	return &manualTimer{channel: make(chan time.Time)}
}

func (t *manualTimer) C() <-chan time.Time {
	return t.channel
}

func (t *manualTimer) Reset(time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.resets++
}

func (t *manualTimer) Stop() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	wasRunning := !t.stopped
	t.stopped = true
	return wasRunning
}

func (t *manualTimer) Fire() {
	t.channel <- time.Time{}
}

func (t *manualTimer) ResetCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.resets
}

func (t *manualTimer) Stopped() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.stopped
}
