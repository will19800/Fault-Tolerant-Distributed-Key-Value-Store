package raft

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand"
	"slices"
	"sync"
	"time"
)

const (
	// DefaultElectionTimeout is the minimum randomized election timeout.
	DefaultElectionTimeout = 300 * time.Millisecond
	// DefaultHeartbeatInterval controls how often a leader contacts followers.
	DefaultHeartbeatInterval = 100 * time.Millisecond
)

var (
	// ErrNodeStopped is returned when an operation targets a closed node.
	ErrNodeStopped = errors.New("Raft node stopped")
	// ErrUnknownMember is returned for RPCs from outside fixed membership.
	ErrUnknownMember = errors.New("unknown Raft member")
	// ErrNotLeader is returned when a proposal targets a follower or candidate.
	ErrNotLeader = errors.New("Raft node is not the leader")
)

// RandomSource supplies election-timeout jitter.
type RandomSource interface {
	Int63n(int64) int64
}

// PeerTransport is the outbound Raft RPC boundary used by Node.
type PeerTransport interface {
	RequestVote(context.Context, NodeID, RequestVoteRequest) (RequestVoteResponse, error)
	AppendEntries(context.Context, NodeID, AppendEntriesRequest) (AppendEntriesResponse, error)
}

// NodeConfig contains the dependencies needed by the Raft event loop.
type NodeConfig struct {
	ID                     NodeID
	Members                []NodeID
	Transport              PeerTransport
	ElectionTimeout        time.Duration
	MaximumElectionTimeout time.Duration
	HeartbeatInterval      time.Duration
	Clock                  Clock
	Random                 RandomSource
	Persistence            HardStateStore
	LogStore               LogStore
}

// Node serializes timers, RPCs, and state queries through one event loop.
type Node struct {
	events   chan any
	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
	runErr   error
	members  map[NodeID]struct{}
}

// NewNode validates config, recovers hard state, and starts a follower event
// loop.
func NewNode(config NodeConfig) (*Node, error) {
	if err := normalizeNodeConfig(&config); err != nil {
		return nil, err
	}
	hardState, err := config.Persistence.Load()
	if err != nil {
		return nil, fmt.Errorf("load Raft hard state: %w", err)
	}
	if err := hardState.Validate(); err != nil {
		return nil, fmt.Errorf("validate Raft hard state: %w", err)
	}
	if hardState.VotedFor != "" && !slices.Contains(config.Members, hardState.VotedFor) {
		return nil, fmt.Errorf("recovered vote for %q: %w", hardState.VotedFor, ErrUnknownMember)
	}
	logEntries, err := config.LogStore.Load()
	if err != nil {
		return nil, fmt.Errorf("load Raft log: %w", err)
	}
	if err := validateLog(logEntries); err != nil {
		return nil, fmt.Errorf("validate Raft log: %w", err)
	}
	logEntries = cloneEntries(logEntries)
	electionTimer := config.Clock.NewTimer(nextElectionTimeout(config))
	if electionTimer == nil {
		return nil, errors.New("clock returned a nil election timer")
	}

	node := &Node{
		events:  make(chan any),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
		members: make(map[NodeID]struct{}, len(config.Members)),
	}
	for _, member := range config.Members {
		node.members[member] = struct{}{}
	}
	go func() {
		node.runErr = node.run(config, electionTimer, hardState, logEntries)
		close(node.done)
	}()
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
		return Status{}, n.stoppedError()
	}
}

// Propose durably appends a command to the leader's local log and starts
// replication. The returned index is not necessarily committed yet.
func (n *Node) Propose(ctx context.Context, command []byte) (LogIndex, error) {
	if len(command) > MaxCommandBytes {
		return 0, fmt.Errorf("command exceeds %d bytes", MaxCommandBytes)
	}
	reply := make(chan proposalResult, 1)
	event := proposalEvent{command: slices.Clone(command), reply: reply}
	if err := n.submit(ctx, event); err != nil {
		return 0, err
	}
	select {
	case result := <-reply:
		return result.index, result.err
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-n.done:
		return 0, n.stoppedError()
	}
}

// HandleRequestVote serializes a vote request through the event loop.
func (n *Node) HandleRequestVote(ctx context.Context, request RequestVoteRequest) (RequestVoteResponse, error) {
	if err := request.Validate(); err != nil {
		return RequestVoteResponse{}, fmt.Errorf("request vote request: %w", err)
	}
	if _, exists := n.members[request.CandidateID]; !exists {
		return RequestVoteResponse{}, fmt.Errorf("candidate %q: %w", request.CandidateID, ErrUnknownMember)
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
		return RequestVoteResponse{}, n.stoppedError()
	}
}

// HandleAppendEntries serializes an append request through the event loop. Log
// matching and replication are added with the follower-log implementation.
func (n *Node) HandleAppendEntries(ctx context.Context, request AppendEntriesRequest) (AppendEntriesResponse, error) {
	if err := request.Validate(); err != nil {
		return AppendEntriesResponse{}, fmt.Errorf("append entries request: %w", err)
	}
	if _, exists := n.members[request.LeaderID]; !exists {
		return AppendEntriesResponse{}, fmt.Errorf("leader %q: %w", request.LeaderID, ErrUnknownMember)
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
		return AppendEntriesResponse{}, n.stoppedError()
	}
}

// Close stops the event loop and its timers. It is safe to call more than once.
func (n *Node) Close() error {
	n.stopOnce.Do(func() { close(n.stop) })
	<-n.done
	return n.runErr
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
		return n.stoppedError()
	}
}

func (n *Node) stoppedError() error {
	if n.runErr != nil {
		return n.runErr
	}
	return ErrNodeStopped
}

func (n *Node) run(config NodeConfig, electionTimer Timer, hardState HardState, logEntries []Entry) error {
	defer electionTimer.Stop()

	state := Status{
		ID:       config.ID,
		Role:     Follower,
		Term:     hardState.CurrentTerm,
		VotedFor: hardState.VotedFor,
	}
	state.LastLogIndex, state.LastLogTerm = lastLogPosition(logEntries)
	peers := peerIDs(config.Members, config.ID)
	quorum := len(config.Members)/2 + 1
	var votes map[NodeID]struct{}
	var electionCancel context.CancelFunc
	var leaderContext context.Context
	var leaderCancel context.CancelFunc
	var heartbeatTimer Timer
	var heartbeatC <-chan time.Time
	nextIndex := make(map[NodeID]LogIndex, len(peers))
	matchIndex := make(map[NodeID]LogIndex, len(peers))
	appendInFlight := make(map[NodeID]bool, len(peers))

	cancelElection := func() {
		if electionCancel != nil {
			electionCancel()
			electionCancel = nil
		}
	}
	cancelLeader := func() {
		if leaderCancel != nil {
			leaderCancel()
			leaderCancel = nil
			leaderContext = nil
		}
		if heartbeatTimer != nil {
			heartbeatTimer.Stop()
		}
		heartbeatC = nil
	}
	defer func() {
		cancelElection()
		cancelLeader()
	}()

	persist := func(next HardState) error {
		if err := config.Persistence.Save(next); err != nil {
			return err
		}
		hardState = next
		state.Term = next.CurrentTerm
		state.VotedFor = next.VotedFor
		return nil
	}
	persistTerm := func(term Term) error {
		next := hardState
		next.CurrentTerm = term
		next.VotedFor = ""
		return persist(next)
	}
	resetElectionTimer := func() {
		electionTimer.Reset(nextElectionTimeout(config))
	}
	becomeFollower := func(leader NodeID) {
		cancelElection()
		cancelLeader()
		votes = nil
		state.Role = Follower
		state.LeaderID = leader
		clear(nextIndex)
		clear(matchIndex)
		clear(appendInFlight)
		resetElectionTimer()
	}

	sendVoteRequests := func(ctx context.Context, term Term) {
		lastIndex, lastTerm := lastLogPosition(logEntries)
		request := RequestVoteRequest{
			Term: term, CandidateID: config.ID,
			LastLogIndex: lastIndex, LastLogTerm: lastTerm,
		}
		for _, peer := range peers {
			go func(peer NodeID) {
				response, err := config.Transport.RequestVote(ctx, peer, request)
				if err == nil {
					err = response.Validate()
				}
				_ = n.submit(ctx, voteRPCEvent{peer: peer, requestTerm: term, response: response, err: err})
			}(peer)
		}
	}
	var sendReplication func(context.Context, NodeID, Term)
	sendReplication = func(ctx context.Context, peer NodeID, term Term) {
		if appendInFlight[peer] {
			return
		}
		next := nextIndex[peer]
		if next == 0 {
			next = 1
		}
		lastIndex, _ := lastLogPosition(logEntries)
		if next > lastIndex+1 {
			next = lastIndex + 1
		}
		prevIndex := next - 1
		prevTerm := Term(0)
		if prevIndex > 0 {
			prevTerm = logEntries[prevIndex-1].Term
		}
		request := AppendEntriesRequest{
			Term: term, LeaderID: config.ID, PrevLogIndex: prevIndex,
			PrevLogTerm: prevTerm, Entries: cloneEntries(logEntries[next-1:]),
			LeaderCommit: state.CommitIndex,
		}
		appendInFlight[peer] = true
		go func() {
			response, err := config.Transport.AppendEntries(ctx, peer, request)
			if err == nil {
				err = response.Validate()
			}
			_ = n.submit(ctx, appendRPCEvent{
				peer: peer, requestTerm: term, requestNext: next,
				requestCommit: request.LeaderCommit, response: response, err: err,
			})
		}()
	}
	sendAllReplication := func(ctx context.Context, term Term) {
		for _, peer := range peers {
			sendReplication(ctx, peer, term)
		}
	}
	becomeLeader := func() error {
		cancelElection()
		state.Role = Leader
		state.LeaderID = config.ID
		electionTimer.Stop()
		leaderContext, leaderCancel = context.WithCancel(context.Background())
		lastIndex, _ := lastLogPosition(logEntries)
		for _, peer := range peers {
			nextIndex[peer] = lastIndex + 1
			matchIndex[peer] = 0
			appendInFlight[peer] = false
		}
		if heartbeatTimer == nil {
			heartbeatTimer = config.Clock.NewTimer(config.HeartbeatInterval)
			if heartbeatTimer == nil {
				return errors.New("clock returned a nil heartbeat timer")
			}
		} else {
			heartbeatTimer.Reset(config.HeartbeatInterval)
		}
		heartbeatC = heartbeatTimer.C()
		sendAllReplication(leaderContext, state.Term)
		return nil
	}
	startElection := func() error {
		if state.Term == ^Term(0) {
			return errors.New("Raft term overflow")
		}
		cancelElection()
		cancelLeader()
		next := HardState{CurrentTerm: state.Term + 1, VotedFor: config.ID}
		if err := persist(next); err != nil {
			return fmt.Errorf("persist election term %d and self-vote: %w", next.CurrentTerm, err)
		}
		state.Role = Candidate
		state.LeaderID = ""
		votes = map[NodeID]struct{}{config.ID: {}}
		resetElectionTimer()
		electionContext, cancel := context.WithCancel(context.Background())
		electionCancel = cancel
		sendVoteRequests(electionContext, state.Term)
		if len(votes) >= quorum {
			return becomeLeader()
		}
		return nil
	}

	for {
		select {
		case <-electionTimer.C():
			if err := startElection(); err != nil {
				return err
			}
		case <-heartbeatC:
			if state.Role == Leader {
				heartbeatTimer.Reset(config.HeartbeatInterval)
				sendAllReplication(leaderContext, state.Term)
			}
		case rawEvent := <-n.events:
			switch event := rawEvent.(type) {
			case statusEvent:
				event.reply <- state
			case proposalEvent:
				if state.Role != Leader {
					event.reply <- proposalResult{err: ErrNotLeader}
					continue
				}
				if state.LastLogIndex == ^LogIndex(0) {
					event.reply <- proposalResult{err: errors.New("Raft log index overflow")}
					continue
				}
				entry := Entry{Index: state.LastLogIndex + 1, Term: state.Term, Command: slices.Clone(event.command)}
				nextLog := append(cloneEntries(logEntries), entry)
				if err := config.LogStore.Save(nextLog); err != nil {
					persistenceErr := fmt.Errorf("persist proposed entry %d: %w", entry.Index, err)
					event.reply <- proposalResult{err: persistenceErr}
					return persistenceErr
				}
				logEntries = nextLog
				state.LastLogIndex, state.LastLogTerm = lastLogPosition(logEntries)
				event.reply <- proposalResult{index: entry.Index}
				sendAllReplication(leaderContext, state.Term)
			case requestVoteEvent:
				if event.request.Term > state.Term {
					if err := persistTerm(event.request.Term); err != nil {
						persistenceErr := fmt.Errorf("persist request-vote term %d: %w", event.request.Term, err)
						event.reply <- voteResult{err: persistenceErr}
						return persistenceErr
					}
					becomeFollower("")
				}
				grant := false
				if event.request.Term == state.Term && candidateLogIsFresh(event.request, logEntries) &&
					(state.VotedFor == "" || state.VotedFor == event.request.CandidateID) {
					if state.VotedFor == "" {
						next := hardState
						next.VotedFor = event.request.CandidateID
						if err := persist(next); err != nil {
							persistenceErr := fmt.Errorf("persist vote for %q in term %d: %w", event.request.CandidateID, state.Term, err)
							event.reply <- voteResult{err: persistenceErr}
							return persistenceErr
						}
					}
					grant = true
					resetElectionTimer()
				}
				event.reply <- voteResult{response: RequestVoteResponse{Term: state.Term, VoteGranted: grant}}
			case appendEntriesEvent:
				if event.request.Term > state.Term {
					if err := persistTerm(event.request.Term); err != nil {
						persistenceErr := fmt.Errorf("persist append-entries term %d: %w", event.request.Term, err)
						event.reply <- appendResult{err: persistenceErr}
						return persistenceErr
					}
				}
				if event.request.Term < state.Term {
					event.reply <- appendResult{response: AppendEntriesResponse{
						Term: state.Term, ConflictIndex: LogIndex(len(logEntries)) + 1,
					}}
					continue
				}
				becomeFollower(event.request.LeaderID)
				nextLog, response, changed := reconcileLog(logEntries, event.request)
				response.Term = state.Term
				if changed {
					if err := config.LogStore.Save(nextLog); err != nil {
						persistenceErr := fmt.Errorf("persist entries through index %d: %w", response.MatchIndex, err)
						event.reply <- appendResult{err: persistenceErr}
						return persistenceErr
					}
					logEntries = nextLog
					state.LastLogIndex, state.LastLogTerm = lastLogPosition(logEntries)
				}
				if response.Success && event.request.LeaderCommit > state.CommitIndex {
					commit := min(event.request.LeaderCommit, response.MatchIndex)
					if commit > state.CommitIndex {
						state.CommitIndex = commit
					}
				}
				event.reply <- appendResult{response: response}
			case voteRPCEvent:
				if event.err != nil {
					continue
				}
				if event.response.Term > state.Term {
					if err := persistTerm(event.response.Term); err != nil {
						return fmt.Errorf("persist vote-response term %d: %w", event.response.Term, err)
					}
					becomeFollower("")
					continue
				}
				if state.Role != Candidate || event.requestTerm != state.Term || !event.response.VoteGranted {
					continue
				}
				votes[event.peer] = struct{}{}
				if len(votes) >= quorum {
					if err := becomeLeader(); err != nil {
						return err
					}
				}
			case appendRPCEvent:
				if state.Role != Leader || event.requestTerm != state.Term {
					continue
				}
				appendInFlight[event.peer] = false
				if event.err == nil && event.response.Term > state.Term {
					if err := persistTerm(event.response.Term); err != nil {
						return fmt.Errorf("persist append-response term %d: %w", event.response.Term, err)
					}
					becomeFollower("")
					continue
				}
				if event.err != nil {
					continue
				}
				if event.response.Success {
					if event.response.MatchIndex > matchIndex[event.peer] {
						matchIndex[event.peer] = event.response.MatchIndex
						nextIndex[event.peer] = event.response.MatchIndex + 1
					}
					previousCommit := state.CommitIndex
					state.CommitIndex = committedIndex(logEntries, matchIndex, quorum, state.Term, state.CommitIndex)
					if state.CommitIndex > previousCommit {
						sendAllReplication(leaderContext, state.Term)
					} else if nextIndex[event.peer] <= state.LastLogIndex || event.requestCommit < state.CommitIndex {
						sendReplication(leaderContext, event.peer, state.Term)
					}
					continue
				}
				if event.requestNext != nextIndex[event.peer] {
					continue
				}
				retryAt := conflictNextIndex(logEntries, event.response)
				if retryAt < nextIndex[event.peer] {
					nextIndex[event.peer] = retryAt
					sendReplication(leaderContext, event.peer, state.Term)
				}
			}
		case <-n.stop:
			return nil
		}
	}
}

func normalizeNodeConfig(config *NodeConfig) error {
	if err := config.ID.Validate(); err != nil {
		return fmt.Errorf("node ID: %w", err)
	}
	if len(config.Members) != 3 && len(config.Members) != 5 {
		return fmt.Errorf("Raft membership must contain exactly 3 or 5 nodes, got %d", len(config.Members))
	}
	seen := make(map[NodeID]struct{}, len(config.Members))
	localFound := false
	for _, member := range config.Members {
		if err := member.Validate(); err != nil {
			return fmt.Errorf("member ID: %w", err)
		}
		if _, exists := seen[member]; exists {
			return fmt.Errorf("duplicate member ID %q", member)
		}
		seen[member] = struct{}{}
		localFound = localFound || member == config.ID
	}
	if !localFound {
		return fmt.Errorf("local node ID %q is not a member", config.ID)
	}
	config.Members = slices.Clone(config.Members)
	if config.Transport == nil {
		return errors.New("peer transport must not be nil")
	}
	if config.Persistence == nil {
		return errors.New("hard-state persistence must not be nil")
	}
	if config.LogStore == nil {
		return errors.New("Raft log store must not be nil")
	}
	if config.ElectionTimeout <= 0 {
		return fmt.Errorf("election timeout must be positive: %s", config.ElectionTimeout)
	}
	if config.MaximumElectionTimeout == 0 {
		config.MaximumElectionTimeout = 2 * config.ElectionTimeout
	}
	if config.MaximumElectionTimeout < config.ElectionTimeout {
		return fmt.Errorf("maximum election timeout %s is below minimum %s", config.MaximumElectionTimeout, config.ElectionTimeout)
	}
	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = DefaultHeartbeatInterval
	}
	if config.HeartbeatInterval <= 0 || config.HeartbeatInterval >= config.ElectionTimeout {
		return errors.New("heartbeat interval must be positive and below the minimum election timeout")
	}
	if config.Clock == nil {
		config.Clock = systemClock{}
	}
	if config.Random == nil {
		hash := fnv.New64a()
		_, _ = hash.Write([]byte(config.ID))
		config.Random = rand.New(rand.NewSource(time.Now().UnixNano() + int64(hash.Sum64())))
	}
	return nil
}

func nextElectionTimeout(config NodeConfig) time.Duration {
	window := config.MaximumElectionTimeout - config.ElectionTimeout
	if window == 0 {
		return config.ElectionTimeout
	}
	return config.ElectionTimeout + time.Duration(config.Random.Int63n(int64(window)))
}

func peerIDs(members []NodeID, localID NodeID) []NodeID {
	peers := make([]NodeID, 0, len(members)-1)
	for _, member := range members {
		if member != localID {
			peers = append(peers, member)
		}
	}
	return peers
}

type statusEvent struct {
	reply chan<- Status
}

type proposalEvent struct {
	command []byte
	reply   chan<- proposalResult
}

type proposalResult struct {
	index LogIndex
	err   error
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

type voteRPCEvent struct {
	peer        NodeID
	requestTerm Term
	response    RequestVoteResponse
	err         error
}

type appendRPCEvent struct {
	peer          NodeID
	requestTerm   Term
	requestNext   LogIndex
	requestCommit LogIndex
	response      AppendEntriesResponse
	err           error
}
