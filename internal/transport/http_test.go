package transport_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/transport"
)

func TestHTTPTransportExchangesRaftRPCs(t *testing.T) {
	receiver := &recordingHandler{}
	server := newPeerServer(t, receiver, transport.DefaultMaxRPCBytes)
	defer server.Close()
	sender := newPeerTransport(t, "node-2", server.URL, time.Second)

	vote, err := sender.RequestVote(context.Background(), "node-2", raft.RequestVoteRequest{Term: 3, CandidateID: "node-1"})
	if err != nil {
		t.Fatalf("RequestVote: %v", err)
	}
	if want := (raft.RequestVoteResponse{Term: 3, VoteGranted: true}); vote != want {
		t.Fatalf("RequestVote response = %+v, want %+v", vote, want)
	}

	appendResponse, err := sender.AppendEntries(context.Background(), "node-2", raft.AppendEntriesRequest{
		Term: 3, LeaderID: "node-1", Entries: []raft.Entry{{Index: 1, Term: 3, Command: []byte("put a=1")}},
	})
	if err != nil {
		t.Fatalf("AppendEntries: %v", err)
	}
	if want := (raft.AppendEntriesResponse{Term: 3, Success: true, MatchIndex: 1}); appendResponse != want {
		t.Fatalf("AppendEntries response = %+v, want %+v", appendResponse, want)
	}
	if got, want := receiver.calls(), []string{"vote:node-1", "append:put a=1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("received calls = %v, want %v", got, want)
	}
}

func TestHTTPHandlerRejectsMalformedMessages(t *testing.T) {
	server := newPeerServer(t, &recordingHandler{}, 128)
	defer server.Close()

	tests := []struct {
		name   string
		path   string
		body   string
		status int
	}{
		{name: "broken JSON", path: transport.RequestVotePath, body: `{`, status: http.StatusBadRequest},
		{name: "unknown field", path: transport.RequestVotePath, body: `{"term":1,"candidate_id":"node-1","extra":true}`, status: http.StatusBadRequest},
		{name: "invalid contract", path: transport.RequestVotePath, body: `{"term":0,"candidate_id":"node-1"}`, status: http.StatusBadRequest},
		{name: "multiple values", path: transport.AppendEntriesPath, body: `{} {}`, status: http.StatusBadRequest},
		{name: "too large", path: transport.AppendEntriesPath, body: `{"term":1,"leader_id":"node-1","padding":"` + strings.Repeat("x", 128) + `"}`, status: http.StatusRequestEntityTooLarge},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response, err := http.Post(server.URL+test.path, "application/json", strings.NewReader(test.body))
			if err != nil {
				t.Fatalf("Post: %v", err)
			}
			defer response.Body.Close()
			if response.StatusCode != test.status {
				t.Fatalf("status = %d, want %d", response.StatusCode, test.status)
			}
		})
	}
}

func TestHTTPHandlerRejectsWrongMethod(t *testing.T) {
	server := newPeerServer(t, &recordingHandler{}, transport.DefaultMaxRPCBytes)
	defer server.Close()
	response, err := http.Get(server.URL + transport.RequestVotePath)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusMethodNotAllowed)
	}
}

func TestHTTPTransportReportsUnavailablePeer(t *testing.T) {
	server := newPeerServer(t, &recordingHandler{}, transport.DefaultMaxRPCBytes)
	address := server.URL
	server.Close()
	sender := newPeerTransport(t, "node-2", address, time.Second)

	_, err := sender.RequestVote(context.Background(), "node-2", raft.RequestVoteRequest{Term: 1, CandidateID: "node-1"})
	if !errors.Is(err, transport.ErrPeerUnavailable) {
		t.Fatalf("RequestVote error = %v, want ErrPeerUnavailable", err)
	}
}

func TestHTTPTransportEnforcesTimeout(t *testing.T) {
	server := newPeerServer(t, blockingHandler{}, transport.DefaultMaxRPCBytes)
	defer server.Close()
	sender := newPeerTransport(t, "node-2", server.URL, 20*time.Millisecond)

	_, err := sender.AppendEntries(context.Background(), "node-2", raft.AppendEntriesRequest{Term: 1, LeaderID: "node-1"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("AppendEntries error = %v, want context.DeadlineExceeded", err)
	}
	if !errors.Is(err, transport.ErrPeerUnavailable) {
		t.Fatalf("AppendEntries error = %v, want ErrPeerUnavailable", err)
	}
}

func TestHTTPTransportRejectsMalformedResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"term":0,"vote_granted":true}`))
	}))
	defer server.Close()
	sender := newPeerTransport(t, "node-2", server.URL, time.Second)

	_, err := sender.RequestVote(context.Background(), "node-2", raft.RequestVoteRequest{Term: 1, CandidateID: "node-1"})
	if !errors.Is(err, transport.ErrInvalidResponse) {
		t.Fatalf("RequestVote error = %v, want ErrInvalidResponse", err)
	}
}

func TestHTTPTransportReportsHandlerFailure(t *testing.T) {
	server := newPeerServer(t, failingHandler{}, transport.DefaultMaxRPCBytes)
	defer server.Close()
	sender := newPeerTransport(t, "node-2", server.URL, time.Second)

	_, err := sender.RequestVote(context.Background(), "node-2", raft.RequestVoteRequest{Term: 1, CandidateID: "node-1"})
	if !errors.Is(err, transport.ErrPeerUnavailable) {
		t.Fatalf("RequestVote error = %v, want ErrPeerUnavailable", err)
	}
}

func TestNewHTTPTransportValidatesConfiguration(t *testing.T) {
	invalid := []struct {
		name    string
		peers   map[raft.NodeID]string
		timeout time.Duration
	}{
		{name: "zero timeout", peers: map[raft.NodeID]string{"node-1": "127.0.0.1:9001"}},
		{name: "invalid ID", peers: map[raft.NodeID]string{"node 1": "127.0.0.1:9001"}, timeout: time.Second},
		{name: "unsupported scheme", peers: map[raft.NodeID]string{"node-1": "ftp://127.0.0.1:9001"}, timeout: time.Second},
		{name: "address path", peers: map[raft.NodeID]string{"node-1": "http://127.0.0.1:9001/path"}, timeout: time.Second},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			if _, err := transport.NewHTTPTransport(test.peers, test.timeout); err == nil {
				t.Fatal("NewHTTPTransport returned nil error")
			}
		})
	}
}

func newPeerServer(t *testing.T, receiver transport.Handler, maxBodyBytes int64) *httptest.Server {
	t.Helper()
	handler, err := transport.NewHTTPHandler(receiver, maxBodyBytes)
	if err != nil {
		t.Fatalf("NewHTTPHandler: %v", err)
	}
	return httptest.NewServer(handler.Routes())
}

func newPeerTransport(t *testing.T, id raft.NodeID, address string, timeout time.Duration) *transport.HTTPTransport {
	t.Helper()
	sender, err := transport.NewHTTPTransport(map[raft.NodeID]string{id: address}, timeout)
	if err != nil {
		t.Fatalf("NewHTTPTransport: %v", err)
	}
	return sender
}

type failingHandler struct{}

func (failingHandler) HandleRequestVote(context.Context, raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	return raft.RequestVoteResponse{}, errors.New("failed")
}

func (failingHandler) HandleAppendEntries(context.Context, raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	return raft.AppendEntriesResponse{}, errors.New("failed")
}
