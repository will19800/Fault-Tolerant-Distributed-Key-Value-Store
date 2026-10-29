package raft_test

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/transport"
)

var _ raft.PeerTransport = (*transport.MemoryNetwork)(nil)

func TestThreeNodeClusterElectsAndReplacesLeader(t *testing.T) {
	cluster := newElectionCluster(t)
	defer cluster.Close()

	cluster.electionTimers["node-1"].Fire()
	leader := waitForNodeStatus(t, cluster.nodes["node-1"], func(status raft.Status) bool {
		return status.Role == raft.Leader && status.Term == 1
	})
	if leader.VotedFor != "node-1" || leader.LeaderID != "node-1" {
		t.Fatalf("first leader status = %+v", leader)
	}
	followerVotes := 0
	for _, id := range []raft.NodeID{"node-2", "node-3"} {
		follower := waitForNodeStatus(t, cluster.nodes[id], func(status raft.Status) bool {
			return status.Term == 1 && status.LeaderID == "node-1"
		})
		if follower.Role != raft.Follower {
			t.Fatalf("follower %q status = %+v", id, follower)
		}
		if follower.VotedFor == "node-1" {
			followerVotes++
		}
	}
	if followerVotes < 1 {
		t.Fatal("leader did not receive a follower vote for its majority")
	}

	cluster.network.Unregister("node-1")
	if err := cluster.nodes["node-1"].Close(); err != nil {
		t.Fatalf("close first leader: %v", err)
	}
	cluster.electionTimers["node-2"].Fire()
	replacement := waitForNodeStatus(t, cluster.nodes["node-2"], func(status raft.Status) bool {
		return status.Role == raft.Leader && status.Term == 2
	})
	if replacement.VotedFor != "node-2" || replacement.LeaderID != "node-2" {
		t.Fatalf("replacement leader status = %+v", replacement)
	}
	witness := waitForNodeStatus(t, cluster.nodes["node-3"], func(status raft.Status) bool {
		return status.Term == 2 && status.LeaderID == "node-2"
	})
	if witness.VotedFor != "node-2" {
		t.Fatalf("replacement follower status = %+v", witness)
	}
}

func TestCandidateRetriesAfterSplitVote(t *testing.T) {
	peerTransport := &scriptedTransport{grantFromTerm: 2}
	node, timer, clock := newElectionTestNode(t, electionNodeOptions{transport: peerTransport})
	defer node.Close()

	timer.Fire()
	firstRound := waitForNodeStatus(t, node, func(status raft.Status) bool {
		return status.Role == raft.Candidate && status.Term == 1
	})
	if firstRound.VotedFor != "node-1" {
		t.Fatalf("first election status = %+v", firstRound)
	}

	timer.Fire()
	secondRound := waitForNodeStatus(t, node, func(status raft.Status) bool {
		return status.Role == raft.Leader && status.Term == 2
	})
	if secondRound.LeaderID != "node-1" {
		t.Fatalf("second election status = %+v", secondRound)
	}
	if got := peerTransport.voteCalls(); got < 4 {
		t.Fatalf("vote RPC count = %d, want at least 4 across two rounds", got)
	}
	if clock.TimerCount() != 2 {
		t.Fatalf("timer count = %d, want election and heartbeat timers", clock.TimerCount())
	}
}

func TestLeaderSendsHeartbeatsAndStepsDownForHigherTerm(t *testing.T) {
	peerTransport := &scriptedTransport{grantFromTerm: 1}
	node, electionTimer, clock := newElectionTestNode(t, electionNodeOptions{transport: peerTransport})
	defer node.Close()

	electionTimer.Fire()
	waitForNodeStatus(t, node, func(status raft.Status) bool { return status.Role == raft.Leader })
	waitForCondition(t, func() bool { return peerTransport.appendCalls() >= 2 }, "initial heartbeats")
	heartbeatTimer := clock.Timer(1)
	before := peerTransport.appendCalls()
	heartbeatTimer.Fire()
	waitForCondition(t, func() bool { return peerTransport.appendCalls() >= before+2 }, "periodic heartbeats")

	peerTransport.setAppendResponseTerm(3)
	heartbeatTimer.Fire()
	status := waitForNodeStatus(t, node, func(status raft.Status) bool {
		return status.Role == raft.Follower && status.Term == 3
	})
	if status.LeaderID != "" || status.VotedFor != "" {
		t.Fatalf("status after higher-term heartbeat response = %+v", status)
	}
}

func TestRequestVoteEnforcesLogFreshnessAndOneVotePerTerm(t *testing.T) {
	node, _, _ := newElectionTestNode(t, electionNodeOptions{
		lastLogIndex: 8,
		lastLogTerm:  3,
	})
	defer node.Close()

	requests := []struct {
		name    string
		request raft.RequestVoteRequest
		granted bool
	}{
		{name: "older term", request: raft.RequestVoteRequest{Term: 4, CandidateID: "node-2", LastLogIndex: 20, LastLogTerm: 2}},
		{name: "shorter same term", request: raft.RequestVoteRequest{Term: 4, CandidateID: "node-2", LastLogIndex: 7, LastLogTerm: 3}},
		{name: "equally fresh", request: raft.RequestVoteRequest{Term: 4, CandidateID: "node-2", LastLogIndex: 8, LastLogTerm: 3}, granted: true},
		{name: "second candidate", request: raft.RequestVoteRequest{Term: 4, CandidateID: "node-3", LastLogIndex: 9, LastLogTerm: 3}},
	}
	for _, test := range requests {
		t.Run(test.name, func(t *testing.T) {
			response, err := node.HandleRequestVote(context.Background(), test.request)
			if err != nil {
				t.Fatalf("HandleRequestVote: %v", err)
			}
			if response.VoteGranted != test.granted || response.Term != 4 {
				t.Fatalf("response = %+v, want granted=%t term=4", response, test.granted)
			}
		})
	}
	status := nodeStatus(t, node)
	if status.VotedFor != "node-2" || status.Term != 4 {
		t.Fatalf("final status = %+v", status)
	}
}

func TestNodeRejectsRPCsFromOutsideFixedMembership(t *testing.T) {
	node, _, _ := newElectionTestNode(t, electionNodeOptions{})
	defer node.Close()

	_, voteErr := node.HandleRequestVote(context.Background(), raft.RequestVoteRequest{Term: 9, CandidateID: "outsider"})
	if !errors.Is(voteErr, raft.ErrUnknownMember) {
		t.Fatalf("RequestVote error = %v, want ErrUnknownMember", voteErr)
	}
	_, appendErr := node.HandleAppendEntries(context.Background(), raft.AppendEntriesRequest{Term: 9, LeaderID: "outsider"})
	if !errors.Is(appendErr, raft.ErrUnknownMember) {
		t.Fatalf("AppendEntries error = %v, want ErrUnknownMember", appendErr)
	}
	if status := nodeStatus(t, node); status.Term != 0 || status.VotedFor != "" || status.LeaderID != "" {
		t.Fatalf("outsider changed node state: %+v", status)
	}
}

func TestNodeRejectsRecoveredVoteForUnknownMember(t *testing.T) {
	_, err := raft.NewNode(raft.NodeConfig{
		ID: "node-1", Members: testMembers, Transport: unavailableTransport{},
		ElectionTimeout: time.Second,
		Persistence:     newMemoryHardStateStore(raft.HardState{CurrentTerm: 2, VotedFor: "outsider"}),
		LogStore:        newMemoryLogStore(nil),
	})
	if !errors.Is(err, raft.ErrUnknownMember) {
		t.Fatalf("NewNode error = %v, want ErrUnknownMember", err)
	}
}

func TestElectionTimeoutsUseInjectedRandomJitter(t *testing.T) {
	random := &sequenceRandom{values: []int64{int64(25 * time.Millisecond), int64(75 * time.Millisecond)}}
	clock := newTimerClock()
	persistence := newMemoryHardStateStore(raft.HardState{})
	node, err := raft.NewNode(raft.NodeConfig{
		ID: "node-1", Members: testMembers, Transport: unavailableTransport{},
		ElectionTimeout: 100 * time.Millisecond, MaximumElectionTimeout: 200 * time.Millisecond,
		HeartbeatInterval: 20 * time.Millisecond, Clock: clock, Random: random, Persistence: persistence,
		LogStore: newMemoryLogStore(nil),
	})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	defer node.Close()
	electionTimer := clock.Timer(0)
	if got := electionTimer.InitialDuration(); got != 125*time.Millisecond {
		t.Fatalf("initial timeout = %s, want %s", got, 125*time.Millisecond)
	}
	electionTimer.Fire()
	waitForNodeStatus(t, node, func(status raft.Status) bool { return status.Term == 1 })
	if got := electionTimer.LastResetDuration(); got != 175*time.Millisecond {
		t.Fatalf("reset timeout = %s, want %s", got, 175*time.Millisecond)
	}
}

type electionCluster struct {
	network        *transport.MemoryNetwork
	nodes          map[raft.NodeID]*raft.Node
	electionTimers map[raft.NodeID]*recordingTimer
}

func newElectionCluster(t *testing.T) *electionCluster {
	t.Helper()
	cluster := &electionCluster{
		network:        transport.NewMemoryNetwork(),
		nodes:          make(map[raft.NodeID]*raft.Node),
		electionTimers: make(map[raft.NodeID]*recordingTimer),
	}
	for _, id := range testMembers {
		clock := newTimerClock()
		node, err := raft.NewNode(raft.NodeConfig{
			ID: id, Members: testMembers, Transport: cluster.network,
			ElectionTimeout: time.Second, MaximumElectionTimeout: time.Second,
			HeartbeatInterval: 100 * time.Millisecond, Clock: clock,
			Persistence: newMemoryHardStateStore(raft.HardState{}),
			LogStore:    newMemoryLogStore(nil),
		})
		if err != nil {
			t.Fatalf("NewNode(%q): %v", id, err)
		}
		if err := cluster.network.Register(id, node); err != nil {
			t.Fatalf("Register(%q): %v", id, err)
		}
		cluster.nodes[id] = node
		cluster.electionTimers[id] = clock.Timer(0)
	}
	return cluster
}

func (c *electionCluster) Close() {
	for id, node := range c.nodes {
		c.network.Unregister(id)
		_ = node.Close()
	}
}

type electionNodeOptions struct {
	transport    raft.PeerTransport
	lastLogIndex raft.LogIndex
	lastLogTerm  raft.Term
}

func newElectionTestNode(t *testing.T, options electionNodeOptions) (*raft.Node, *recordingTimer, *timerClock) {
	t.Helper()
	if options.transport == nil {
		options.transport = unavailableTransport{}
	}
	clock := newTimerClock()
	entries := make([]raft.Entry, options.lastLogIndex)
	for index := range entries {
		entries[index] = raft.Entry{Index: raft.LogIndex(index + 1), Term: options.lastLogTerm}
	}
	node, err := raft.NewNode(raft.NodeConfig{
		ID: "node-1", Members: testMembers, Transport: options.transport,
		ElectionTimeout: time.Second, MaximumElectionTimeout: time.Second,
		HeartbeatInterval: 100 * time.Millisecond, Clock: clock,
		Persistence: newMemoryHardStateStore(raft.HardState{}),
		LogStore:    newMemoryLogStore(entries),
	})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return node, clock.Timer(0), clock
}

func waitForNodeStatus(t *testing.T, node *raft.Node, matches func(raft.Status) bool) raft.Status {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		status, err := node.Status(context.Background())
		if err != nil {
			t.Fatalf("Status: %v", err)
		}
		if matches(status) {
			return status
		}
		runtime.Gosched()
	}
	status, err := node.Status(context.Background())
	t.Fatalf("status condition not reached; final status=%+v error=%v", status, err)
	return raft.Status{}
}

func waitForCondition(t *testing.T, condition func() bool, description string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("timed out waiting for %s", description)
}

type scriptedTransport struct {
	mu                 sync.Mutex
	grantFromTerm      raft.Term
	appendResponseTerm raft.Term
	votes              int
	appends            int
}

func (t *scriptedTransport) RequestVote(_ context.Context, _ raft.NodeID, request raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.votes++
	return raft.RequestVoteResponse{Term: request.Term, VoteGranted: request.Term >= t.grantFromTerm}, nil
}

func (t *scriptedTransport) AppendEntries(_ context.Context, _ raft.NodeID, request raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.appends++
	term := request.Term
	if t.appendResponseTerm > term {
		term = t.appendResponseTerm
	}
	return raft.AppendEntriesResponse{Term: term, ConflictIndex: 1}, nil
}

func (t *scriptedTransport) voteCalls() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.votes
}

func (t *scriptedTransport) appendCalls() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.appends
}

func (t *scriptedTransport) setAppendResponseTerm(term raft.Term) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.appendResponseTerm = term
}

type timerClock struct {
	mu     sync.Mutex
	timers []*recordingTimer
}

func newTimerClock() *timerClock {
	return &timerClock{}
}

func (c *timerClock) NewTimer(duration time.Duration) raft.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	timer := &recordingTimer{channel: make(chan time.Time), initial: duration}
	c.timers = append(c.timers, timer)
	return timer
}

func (c *timerClock) Timer(index int) *recordingTimer {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		if len(c.timers) > index {
			timer := c.timers[index]
			c.mu.Unlock()
			return timer
		}
		c.mu.Unlock()
		runtime.Gosched()
	}
	panic("timer was not created")
}

func (c *timerClock) TimerCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.timers)
}

type recordingTimer struct {
	channel chan time.Time
	mu      sync.Mutex
	initial time.Duration
	resets  []time.Duration
	stopped bool
}

func (t *recordingTimer) C() <-chan time.Time {
	return t.channel
}

func (t *recordingTimer) Reset(duration time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.stopped = false
	t.resets = append(t.resets, duration)
}

func (t *recordingTimer) Stop() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	wasRunning := !t.stopped
	t.stopped = true
	return wasRunning
}

func (t *recordingTimer) Fire() {
	t.channel <- time.Time{}
}

func (t *recordingTimer) InitialDuration() time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.initial
}

func (t *recordingTimer) LastResetDuration() time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.resets) == 0 {
		return 0
	}
	return t.resets[len(t.resets)-1]
}

type sequenceRandom struct {
	mu     sync.Mutex
	values []int64
	next   int
}

func (r *sequenceRandom) Int63n(limit int64) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	value := r.values[r.next%len(r.values)]
	r.next++
	return value % limit
}
