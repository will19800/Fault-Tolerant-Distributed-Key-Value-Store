package transport

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
)

// HTTPHandler exposes a Handler on the node-to-node HTTP surface.
type HTTPHandler struct {
	handler      Handler
	maxBodyBytes int64
}

// NewHTTPHandler constructs a validated peer HTTP handler.
func NewHTTPHandler(handler Handler, maxBodyBytes int64) (*HTTPHandler, error) {
	if handler == nil {
		return nil, errors.New("Raft handler must not be nil")
	}
	if maxBodyBytes <= 0 {
		return nil, errors.New("maximum RPC body size must be positive")
	}
	return &HTTPHandler{handler: handler, maxBodyBytes: maxBodyBytes}, nil
}

// Routes returns the node-to-node HTTP routing table.
func (h *HTTPHandler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+RequestVotePath, h.requestVote)
	mux.HandleFunc("POST "+AppendEntriesPath, h.appendEntries)
	return mux
}

func (h *HTTPHandler) requestVote(w http.ResponseWriter, request *http.Request) {
	var message raft.RequestVoteRequest
	if !h.decodeRequest(w, request, &message) {
		return
	}
	if err := message.Validate(); err != nil {
		writeRPCError(w, http.StatusBadRequest, fmt.Sprintf("invalid request vote request: %v", err))
		return
	}
	response, err := h.handler.HandleRequestVote(request.Context(), message)
	if err != nil {
		writeRPCError(w, http.StatusInternalServerError, "request vote handler failed")
		return
	}
	if err := response.Validate(); err != nil {
		writeRPCError(w, http.StatusInternalServerError, "request vote handler returned an invalid response")
		return
	}
	writeRPCJSON(w, http.StatusOK, response)
}

func (h *HTTPHandler) appendEntries(w http.ResponseWriter, request *http.Request) {
	var message raft.AppendEntriesRequest
	if !h.decodeRequest(w, request, &message) {
		return
	}
	if err := message.Validate(); err != nil {
		writeRPCError(w, http.StatusBadRequest, fmt.Sprintf("invalid append entries request: %v", err))
		return
	}
	response, err := h.handler.HandleAppendEntries(request.Context(), message)
	if err != nil {
		writeRPCError(w, http.StatusInternalServerError, "append entries handler failed")
		return
	}
	if err := response.Validate(); err != nil {
		writeRPCError(w, http.StatusInternalServerError, "append entries handler returned an invalid response")
		return
	}
	writeRPCJSON(w, http.StatusOK, response)
}

func (h *HTTPHandler) decodeRequest(w http.ResponseWriter, request *http.Request, destination any) bool {
	request.Body = http.MaxBytesReader(w, request.Body, h.maxBodyBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeRPCError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("RPC body exceeds %d bytes", h.maxBodyBytes))
			return false
		}
		writeRPCError(w, http.StatusBadRequest, "RPC body must contain one valid JSON object")
		return false
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		writeRPCError(w, http.StatusBadRequest, "RPC body must contain exactly one JSON object")
		return false
	}
	return true
}

func writeRPCError(w http.ResponseWriter, status int, message string) {
	writeRPCJSON(w, status, struct {
		Error string `json:"error"`
	}{Error: message})
}

func writeRPCJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
