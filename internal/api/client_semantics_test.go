package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/api"
	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
)

func TestFollowerResponseIncludesLeaderHint(t *testing.T) {
	store := &contextErrorStore{err: raft.NotLeaderError{LeaderID: "node-2"}}
	server := newStoreServer(t, store)
	response := request(t, server, http.MethodPut, "/v1/keys/key", "value")
	defer response.Body.Close()

	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", response.StatusCode)
	}
	body := decodeClientError(t, response)
	if body.Code != "not_leader" || body.LeaderID != "node-2" || !body.Retryable {
		t.Fatalf("not-leader response = %+v", body)
	}
}

func TestTimeoutDescribesWriteAmbiguityAndReadRetry(t *testing.T) {
	store := &contextErrorStore{err: context.DeadlineExceeded}
	server := newStoreServer(t, store)

	write := request(t, server, http.MethodDelete, "/v1/keys/key", "")
	defer write.Body.Close()
	writeBody := decodeClientError(t, write)
	if write.StatusCode != http.StatusGatewayTimeout || writeBody.Outcome != "unknown" || writeBody.Retryable {
		t.Fatalf("write timeout = status %d body %+v", write.StatusCode, writeBody)
	}

	read := request(t, server, http.MethodGet, "/v1/keys/key", "")
	defer read.Body.Close()
	readBody := decodeClientError(t, read)
	if read.StatusCode != http.StatusGatewayTimeout || readBody.Outcome != "" || !readBody.Retryable {
		t.Fatalf("read timeout = status %d body %+v", read.StatusCode, readBody)
	}
}

type clientErrorBody struct {
	Code      string `json:"code"`
	LeaderID  string `json:"leader_id"`
	Retryable bool   `json:"retryable"`
	Outcome   string `json:"outcome"`
}

func decodeClientError(t *testing.T, response *http.Response) clientErrorBody {
	t.Helper()
	var body clientErrorBody
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return body
}

func newStoreServer(t *testing.T, store api.Store) *httptest.Server {
	t.Helper()
	handler, err := api.NewHandler(store, api.DefaultMaxValueBytes)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	server := httptest.NewServer(handler.Routes())
	t.Cleanup(server.Close)
	return server
}

type contextErrorStore struct {
	err error
}

func (s *contextErrorStore) Put(string, []byte) error                           { return errors.New("legacy Put called") }
func (s *contextErrorStore) Get(string) ([]byte, error)                         { return nil, errors.New("legacy Get called") }
func (s *contextErrorStore) Delete(string) error                                { return errors.New("legacy Delete called") }
func (s *contextErrorStore) PutContext(context.Context, string, []byte) error   { return s.err }
func (s *contextErrorStore) GetContext(context.Context, string) ([]byte, error) { return nil, s.err }
func (s *contextErrorStore) DeleteContext(context.Context, string) error        { return s.err }
