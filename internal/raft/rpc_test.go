package raft_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
)

func TestNodeIDValidation(t *testing.T) {
	for _, id := range []raft.NodeID{"node-1", "NODE_2", "us.east.3"} {
		if err := id.Validate(); err != nil {
			t.Fatalf("NodeID(%q).Validate: %v", id, err)
		}
	}
	for _, id := range []raft.NodeID{"", "node 1", "node/1", raft.NodeID(strings.Repeat("n", raft.MaxNodeIDBytes+1))} {
		if err := id.Validate(); err == nil {
			t.Fatalf("NodeID(%q).Validate returned nil error", id)
		}
	}
}

func TestRequestValidation(t *testing.T) {
	vote := raft.RequestVoteRequest{Term: 3, CandidateID: "node-1", LastLogIndex: 7, LastLogTerm: 2}
	if err := vote.Validate(); err != nil {
		t.Fatalf("valid vote request: %v", err)
	}

	heartbeat := raft.AppendEntriesRequest{Term: 3, LeaderID: "node-2", PrevLogIndex: 7, PrevLogTerm: 2, LeaderCommit: 6}
	if err := heartbeat.Validate(); err != nil {
		t.Fatalf("valid heartbeat: %v", err)
	}

	appendRequest := raft.AppendEntriesRequest{
		Term:         3,
		LeaderID:     "node-2",
		PrevLogIndex: 7,
		PrevLogTerm:  2,
		Entries: []raft.Entry{
			{Index: 8, Term: 3, Command: []byte("put")},
			{Index: 9, Term: 3, Command: []byte("delete")},
		},
		LeaderCommit: 8,
	}
	if err := appendRequest.Validate(); err != nil {
		t.Fatalf("valid append request: %v", err)
	}
}

func TestRequestValidationRejectsInvalidPositions(t *testing.T) {
	tests := []struct {
		name    string
		request raft.AppendEntriesRequest
		want    string
	}{
		{
			name:    "zero term",
			request: raft.AppendEntriesRequest{LeaderID: "node-1"},
			want:    "term must be positive",
		},
		{
			name:    "term without previous index",
			request: raft.AppendEntriesRequest{Term: 1, LeaderID: "node-1", PrevLogTerm: 1},
			want:    "term must be zero when index is zero",
		},
		{
			name: "noncontiguous entries",
			request: raft.AppendEntriesRequest{
				Term: 1, LeaderID: "node-1", Entries: []raft.Entry{{Index: 2, Term: 1}},
			},
			want: "want contiguous index 1",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.request.Validate()
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Validate error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestRPCJSONRoundTrip(t *testing.T) {
	want := raft.AppendEntriesRequest{
		Term:         4,
		LeaderID:     "node-1",
		PrevLogIndex: 10,
		PrevLogTerm:  3,
		Entries:      []raft.Entry{{Index: 11, Term: 4, Command: []byte{0x00, 0xff}}},
		LeaderCommit: 9,
	}

	encoded, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var got raft.AppendEntriesRequest
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if err := got.Validate(); err != nil {
		t.Fatalf("round-tripped request invalid: %v", err)
	}
	if got.Term != want.Term || got.LeaderID != want.LeaderID || len(got.Entries) != 1 || string(got.Entries[0].Command) != string(want.Entries[0].Command) {
		t.Fatalf("round trip = %+v, want %+v", got, want)
	}
}

func TestResponseValidation(t *testing.T) {
	valid := []interface{ Validate() error }{
		raft.RequestVoteResponse{Term: 2, VoteGranted: true},
		raft.AppendEntriesResponse{Term: 2, Success: true, MatchIndex: 4},
		raft.AppendEntriesResponse{Term: 2, ConflictIndex: 3, ConflictTerm: 1},
	}
	for _, response := range valid {
		if err := response.Validate(); err != nil {
			t.Fatalf("valid response %+v: %v", response, err)
		}
	}

	invalid := []interface{ Validate() error }{
		raft.RequestVoteResponse{},
		raft.AppendEntriesResponse{Term: 2, Success: true, ConflictIndex: 1},
		raft.AppendEntriesResponse{Term: 2},
	}
	for _, response := range invalid {
		if err := response.Validate(); err == nil {
			t.Fatalf("invalid response %+v returned nil error", response)
		}
	}
}
