package api_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/api"
	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/database"
	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
)

func TestHandlerWritesThroughReplicatedStore(t *testing.T) {
	state := database.NewReplicatedStateMachine()
	proposer := &handlerProposer{state: state}
	store, err := database.NewReplicatedStore(proposer, state)
	if err != nil {
		t.Fatalf("NewReplicatedStore: %v", err)
	}
	handler, err := api.NewHandler(store, api.DefaultMaxValueBytes)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	server := httptest.NewServer(handler.Routes())
	defer server.Close()

	assertRequest(t, server, http.MethodPut, "/v1/keys/replicated", "value", http.StatusNoContent, "")
	assertRequest(t, server, http.MethodGet, "/v1/keys/replicated", "", http.StatusOK, "value")
	assertRequest(t, server, http.MethodDelete, "/v1/keys/replicated", "", http.StatusNoContent, "")
	if proposer.proposals != 2 {
		t.Fatalf("Raft proposal count = %d, want PUT and DELETE", proposer.proposals)
	}
}

type handlerProposer struct {
	state     *database.ReplicatedStateMachine
	proposals int
}

func (p *handlerProposer) Propose(_ context.Context, command []byte) (raft.LogIndex, error) {
	p.proposals++
	if err := p.state.Apply(command); err != nil {
		return 0, err
	}
	return raft.LogIndex(p.proposals), nil
}

func (p *handlerProposer) ReadIndex(context.Context) (raft.LogIndex, error) {
	return raft.LogIndex(p.proposals), nil
}
