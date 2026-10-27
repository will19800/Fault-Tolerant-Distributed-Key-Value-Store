package transport_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/transport"
)

func TestMemoryNetworkDeliversRPCsToTarget(t *testing.T) {
	network := transport.NewMemoryNetwork()
	handler := &recordingHandler{}
	if err := network.Register("node-2", handler); err != nil {
		t.Fatalf("Register: %v", err)
	}

	voteRequest := raft.RequestVoteRequest{Term: 2, CandidateID: "node-1"}
	voteResponse, err := network.RequestVote(context.Background(), "node-2", voteRequest)
	if err != nil {
		t.Fatalf("RequestVote: %v", err)
	}
	if want := (raft.RequestVoteResponse{Term: 2, VoteGranted: true}); voteResponse != want {
		t.Fatalf("RequestVote response = %+v, want %+v", voteResponse, want)
	}

	appendRequest := raft.AppendEntriesRequest{
		Term: 2, LeaderID: "node-1",
		Entries: []raft.Entry{{Index: 1, Term: 2, Command: []byte("put a=1")}},
	}
	appendResponse, err := network.AppendEntries(context.Background(), "node-2", appendRequest)
	if err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if want := (raft.AppendEntriesResponse{Term: 2, Success: true, MatchIndex: 1}); appendResponse != want {
		t.Fatalf("AppendEntries response = %+v, want %+v", appendResponse, want)
	}

	if got, want := handler.calls(), []string{"vote:node-1", "append:put a=1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("delivery order = %v, want %v", got, want)
	}
}

func TestMemoryNetworkCopiesAppendEntriesPayload(t *testing.T) {
	network := transport.NewMemoryNetwork()
	handler := &recordingHandler{mutateCommand: true}
	if err := network.Register("node-2", handler); err != nil {
		t.Fatalf("Register: %v", err)
	}
	request := raft.AppendEntriesRequest{
		Term: 1, LeaderID: "node-1",
		Entries: []raft.Entry{{Index: 1, Term: 1, Command: []byte("put")}},
	}

	if _, err := network.AppendEntries(context.Background(), "node-2", request); err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if got := string(request.Entries[0].Command); got != "put" {
		t.Fatalf("caller command changed to %q", got)
	}
}

func TestMemoryNetworkHonorsCanceledContextBeforeDelivery(t *testing.T) {
	network := transport.NewMemoryNetwork()
	handler := &recordingHandler{}
	if err := network.Register("node-2", handler); err != nil {
		t.Fatalf("Register: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := network.RequestVote(ctx, "node-2", raft.RequestVoteRequest{Term: 1, CandidateID: "node-1"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RequestVote error = %v, want context.Canceled", err)
	}
	if got := handler.calls(); len(got) != 0 {
		t.Fatalf("delivered canceled request: %v", got)
	}
}

func TestMemoryNetworkForwardsDeadlineToHandler(t *testing.T) {
	network := transport.NewMemoryNetwork()
	if err := network.Register("node-2", blockingHandler{}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err := network.AppendEntries(ctx, "node-2", raft.AppendEntriesRequest{Term: 1, LeaderID: "node-1"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("AppendEntries error = %v, want context.DeadlineExceeded", err)
	}
}

func TestMemoryNetworkRegistrationAndMissingPeers(t *testing.T) {
	network := transport.NewMemoryNetwork()
	handler := &recordingHandler{}
	if err := network.Register("node-2", handler); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := network.Register("node-2", handler); !errors.Is(err, transport.ErrPeerRegistered) {
		t.Fatalf("duplicate Register error = %v, want ErrPeerRegistered", err)
	}

	network.Unregister("node-2")
	_, err := network.RequestVote(context.Background(), "node-2", raft.RequestVoteRequest{Term: 1, CandidateID: "node-1"})
	if !errors.Is(err, transport.ErrPeerNotFound) {
		t.Fatalf("RequestVote error = %v, want ErrPeerNotFound", err)
	}
}

func TestMemoryNetworkRejectsInvalidMessages(t *testing.T) {
	network := transport.NewMemoryNetwork()
	if err := network.Register("node-2", invalidResponseHandler{}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if _, err := network.RequestVote(context.Background(), "node-2", raft.RequestVoteRequest{}); err == nil {
		t.Fatal("RequestVote accepted invalid request")
	}
	if _, err := network.RequestVote(context.Background(), "node-2", raft.RequestVoteRequest{Term: 1, CandidateID: "node-1"}); err == nil {
		t.Fatal("RequestVote accepted invalid response")
	}
}

type recordingHandler struct {
	mu            sync.Mutex
	deliveries    []string
	mutateCommand bool
}

func (h *recordingHandler) HandleRequestVote(_ context.Context, request raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.deliveries = append(h.deliveries, "vote:"+string(request.CandidateID))
	return raft.RequestVoteResponse{Term: request.Term, VoteGranted: true}, nil
}

func (h *recordingHandler) HandleAppendEntries(_ context.Context, request raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	command := ""
	if len(request.Entries) != 0 {
		command = string(request.Entries[0].Command)
		if h.mutateCommand && len(request.Entries[0].Command) != 0 {
			request.Entries[0].Command[0] = 'X'
		}
	}
	h.deliveries = append(h.deliveries, "append:"+command)
	return raft.AppendEntriesResponse{Term: request.Term, Success: true, MatchIndex: request.PrevLogIndex + raft.LogIndex(len(request.Entries))}, nil
}

func (h *recordingHandler) calls() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.deliveries...)
}

type blockingHandler struct{}

func (blockingHandler) HandleRequestVote(ctx context.Context, _ raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	<-ctx.Done()
	return raft.RequestVoteResponse{}, ctx.Err()
}

func (blockingHandler) HandleAppendEntries(ctx context.Context, _ raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	<-ctx.Done()
	return raft.AppendEntriesResponse{}, ctx.Err()
}

type invalidResponseHandler struct{}

func (invalidResponseHandler) HandleRequestVote(context.Context, raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	return raft.RequestVoteResponse{}, nil
}

func (invalidResponseHandler) HandleAppendEntries(context.Context, raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	return raft.AppendEntriesResponse{}, nil
}
