package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/storage"
)

const (
	// DefaultMaxValueBytes is the default maximum PUT body size.
	DefaultMaxValueBytes int64 = 1 << 20 // 1 MiB
	// MaxKeyBytes is the maximum key size accepted by the HTTP API.
	MaxKeyBytes = 1 << 10 // 1 KiB
)

// Store describes the storage behavior required by the HTTP API.
type Store interface {
	Put(key string, value []byte) error
	Get(key string) ([]byte, error)
	Delete(key string) error
}

// Handler serves the single-node key-value HTTP API.
type Handler struct {
	store         Store
	maxValueBytes int64
}

// NewHandler constructs an HTTP handler for store.
func NewHandler(store Store, maxValueBytes int64) (*Handler, error) {
	if store == nil {
		return nil, errors.New("store must not be nil")
	}
	if maxValueBytes <= 0 {
		return nil, errors.New("maximum value size must be positive")
	}

	return &Handler{
		store:         store,
		maxValueBytes: maxValueBytes,
	}, nil
}

// Routes returns the complete HTTP routing table.
func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /v1/keys/{key}", h.put)
	mux.HandleFunc("GET /v1/keys/{key}", h.get)
	mux.HandleFunc("DELETE /v1/keys/{key}", h.delete)
	mux.HandleFunc("GET /healthz", h.health)
	return mux
}

func (h *Handler) put(w http.ResponseWriter, r *http.Request) {
	key, ok := validKey(w, r)
	if !ok {
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, h.maxValueBytes)
	value, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, "value_too_large", fmt.Sprintf("value exceeds %d bytes", h.maxValueBytes))
			return
		}
		writeError(w, http.StatusBadRequest, "invalid_body", "request body could not be read")
		return
	}

	if err := h.store.Put(key, value); err != nil {
		writeStoreError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	key, ok := validKey(w, r)
	if !ok {
		return
	}

	value, err := h.store.Get(key)
	if err != nil {
		writeStoreError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(value)
}

func (h *Handler) delete(w http.ResponseWriter, r *http.Request) {
	key, ok := validKey(w, r)
	if !ok {
		return
	}

	if err := h.store.Delete(key); err != nil {
		writeStoreError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func validKey(w http.ResponseWriter, r *http.Request) (string, bool) {
	key := r.PathValue("key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "empty_key", "key must not be empty")
		return "", false
	}
	if len(key) > MaxKeyBytes {
		writeError(w, http.StatusBadRequest, "key_too_large", fmt.Sprintf("key exceeds %d bytes", MaxKeyBytes))
		return "", false
	}
	return key, true
}

func writeStoreError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, storage.ErrEmptyKey):
		writeError(w, http.StatusBadRequest, "empty_key", "key must not be empty")
	case errors.Is(err, storage.ErrKeyNotFound):
		writeError(w, http.StatusNotFound, "key_not_found", "key does not exist")
	default:
		writeError(w, http.StatusInternalServerError, "internal_error", "internal server error")
	}
}

type errorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorResponse{Code: code, Message: message})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
