package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
)

const (
	RequestVotePath          = "/raft/request-vote"
	AppendEntriesPath        = "/raft/append-entries"
	DefaultMaxRPCBytes int64 = 4 << 20 // 4 MiB
)

var (
	// ErrPeerUnavailable means an RPC could not reach or be served by a peer.
	ErrPeerUnavailable = errors.New("peer unavailable")
	// ErrInvalidResponse means a peer returned an invalid RPC response.
	ErrInvalidResponse = errors.New("invalid peer response")
)

// HTTPTransport sends Raft RPCs as JSON over HTTP.
type HTTPTransport struct {
	peers  map[raft.NodeID]string
	client *http.Client
}

// NewHTTPTransport validates peer addresses and configures a per-RPC timeout.
// Addresses may be host:port pairs or complete HTTP(S) base URLs.
func NewHTTPTransport(peers map[raft.NodeID]string, timeout time.Duration) (*HTTPTransport, error) {
	if timeout <= 0 {
		return nil, fmt.Errorf("RPC timeout must be positive: %s", timeout)
	}
	normalized := make(map[raft.NodeID]string, len(peers))
	for id, address := range peers {
		if err := id.Validate(); err != nil {
			return nil, fmt.Errorf("peer ID: %w", err)
		}
		baseURL, err := normalizePeerURL(address)
		if err != nil {
			return nil, fmt.Errorf("peer %q address: %w", id, err)
		}
		normalized[id] = baseURL
	}
	return &HTTPTransport{
		peers:  normalized,
		client: &http.Client{Timeout: timeout},
	}, nil
}

// RequestVote sends a vote request to target.
func (t *HTTPTransport) RequestVote(ctx context.Context, target raft.NodeID, request raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	if err := request.Validate(); err != nil {
		return raft.RequestVoteResponse{}, fmt.Errorf("request vote request: %w", err)
	}
	var response raft.RequestVoteResponse
	if err := t.call(ctx, target, RequestVotePath, request, &response); err != nil {
		return raft.RequestVoteResponse{}, err
	}
	if err := response.Validate(); err != nil {
		return raft.RequestVoteResponse{}, fmt.Errorf("%w from %q: %v", ErrInvalidResponse, target, err)
	}
	return response, nil
}

// AppendEntries sends an append request to target.
func (t *HTTPTransport) AppendEntries(ctx context.Context, target raft.NodeID, request raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	if err := request.Validate(); err != nil {
		return raft.AppendEntriesResponse{}, fmt.Errorf("append entries request: %w", err)
	}
	var response raft.AppendEntriesResponse
	if err := t.call(ctx, target, AppendEntriesPath, request, &response); err != nil {
		return raft.AppendEntriesResponse{}, err
	}
	if err := response.Validate(); err != nil {
		return raft.AppendEntriesResponse{}, fmt.Errorf("%w from %q: %v", ErrInvalidResponse, target, err)
	}
	return response, nil
}

func (t *HTTPTransport) call(ctx context.Context, target raft.NodeID, path string, request, response any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	baseURL, exists := t.peers[target]
	if !exists {
		return fmt.Errorf("peer %q: %w", target, ErrPeerNotFound)
	}
	body, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("encode RPC for peer %q: %w", target, err)
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+path, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create RPC for peer %q: %w", target, err)
	}
	httpRequest.Header.Set("Content-Type", "application/json")

	httpResponse, err := t.client.Do(httpRequest)
	if err != nil {
		return fmt.Errorf("%w %q: %w", ErrPeerUnavailable, target, err)
	}
	defer httpResponse.Body.Close()
	if httpResponse.StatusCode != http.StatusOK {
		message := readHTTPError(httpResponse.Body)
		if httpResponse.StatusCode >= 500 {
			return fmt.Errorf("%w %q: HTTP %d: %s", ErrPeerUnavailable, target, httpResponse.StatusCode, message)
		}
		return fmt.Errorf("peer %q rejected RPC with HTTP %d: %s", target, httpResponse.StatusCode, message)
	}
	if err := decodeLimitedJSON(httpResponse.Body, DefaultMaxRPCBytes, response); err != nil {
		return fmt.Errorf("%w from %q: %v", ErrInvalidResponse, target, err)
	}
	return nil
}

func normalizePeerURL(address string) (string, error) {
	address = strings.TrimSpace(address)
	if !strings.Contains(address, "://") {
		address = "http://" + address
	}
	parsed, err := url.Parse(address)
	if err != nil {
		return "", err
	}
	if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return "", fmt.Errorf("expected an HTTP(S) host:port address")
	}
	if (parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.User != nil {
		return "", fmt.Errorf("base address must not contain credentials, a path, query, or fragment")
	}
	return strings.TrimSuffix(parsed.String(), "/"), nil
}

func decodeLimitedJSON(reader io.Reader, limit int64, destination any) error {
	limited := &io.LimitedReader{R: reader, N: limit + 1}
	decoder := json.NewDecoder(limited)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if limited.N == 0 {
		return fmt.Errorf("JSON body exceeds %d bytes", limit)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return fmt.Errorf("JSON body contains multiple values")
		}
		return fmt.Errorf("JSON body contains trailing data: %w", err)
	}
	return nil
}

func readHTTPError(reader io.Reader) string {
	var response struct {
		Error string `json:"error"`
	}
	if err := decodeLimitedJSON(reader, DefaultMaxRPCBytes, &response); err == nil && response.Error != "" {
		return response.Error
	}
	return "peer returned an error"
}
