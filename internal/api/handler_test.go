package api_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/api"
	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/storage"
)

func TestKeyLifecycle(t *testing.T) {
	server := newTestServer(t, api.DefaultMaxValueBytes)

	assertRequest(t, server, http.MethodPut, "/v1/keys/user:123:preferences", `{"theme":"light"}`, http.StatusNoContent, "")
	assertRequest(t, server, http.MethodGet, "/v1/keys/user:123:preferences", "", http.StatusOK, `{"theme":"light"}`)

	assertRequest(t, server, http.MethodPut, "/v1/keys/user:123:preferences", `{"theme":"dark"}`, http.StatusNoContent, "")
	assertRequest(t, server, http.MethodGet, "/v1/keys/user:123:preferences", "", http.StatusOK, `{"theme":"dark"}`)

	assertRequest(t, server, http.MethodDelete, "/v1/keys/user:123:preferences", "", http.StatusNoContent, "")
	assertRequest(t, server, http.MethodDelete, "/v1/keys/user:123:preferences", "", http.StatusNoContent, "")

	response := request(t, server, http.MethodGet, "/v1/keys/user:123:preferences", "")
	defer response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("GET deleted key status = %d, want %d", response.StatusCode, http.StatusNotFound)
	}
	assertAPIError(t, response, "key_not_found", "key does not exist")
}

func TestHealth(t *testing.T) {
	server := newTestServer(t, api.DefaultMaxValueBytes)
	response := request(t, server, http.MethodGet, "/healthz", "")
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		t.Fatalf("health status = %d, want %d", response.StatusCode, http.StatusOK)
	}
	if got := response.Header.Get("Content-Type"); got != "application/json" {
		t.Fatalf("health Content-Type = %q, want application/json", got)
	}
	var body map[string]string
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode health response: %v", err)
	}
	if got := body["status"]; got != "ok" {
		t.Fatalf("health status body = %q, want ok", got)
	}
}

func TestRejectsOversizedValue(t *testing.T) {
	server := newTestServer(t, 4)
	response := request(t, server, http.MethodPut, "/v1/keys/key", "12345")
	defer response.Body.Close()

	if response.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized PUT status = %d, want %d", response.StatusCode, http.StatusRequestEntityTooLarge)
	}
	assertAPIError(t, response, "value_too_large", "value exceeds 4 bytes")

	assertRequest(t, server, http.MethodGet, "/v1/keys/key", "", http.StatusNotFound, `{"code":"key_not_found","message":"key does not exist"}`+"\n")
}

func TestRejectsOversizedKey(t *testing.T) {
	server := newTestServer(t, api.DefaultMaxValueBytes)
	response := request(t, server, http.MethodPut, "/v1/keys/"+strings.Repeat("k", api.MaxKeyBytes+1), "value")
	defer response.Body.Close()

	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("oversized key status = %d, want %d", response.StatusCode, http.StatusBadRequest)
	}
	assertAPIError(t, response, "key_too_large", "key exceeds 1024 bytes")
}

func TestRejectsUnsupportedMethod(t *testing.T) {
	server := newTestServer(t, api.DefaultMaxValueBytes)
	response := request(t, server, http.MethodPost, "/v1/keys/key", "value")
	defer response.Body.Close()

	if response.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST status = %d, want %d", response.StatusCode, http.StatusMethodNotAllowed)
	}
}

func TestNewHandlerValidatesDependencies(t *testing.T) {
	if _, err := api.NewHandler(nil, api.DefaultMaxValueBytes); err == nil {
		t.Fatal("NewHandler nil store returned nil error")
	}
	if _, err := api.NewHandler(storage.NewMemoryStore(), 0); err == nil {
		t.Fatal("NewHandler zero maximum value size returned nil error")
	}
}

func newTestServer(t *testing.T, maxValueBytes int64) *httptest.Server {
	t.Helper()
	handler, err := api.NewHandler(storage.NewMemoryStore(), maxValueBytes)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	server := httptest.NewServer(handler.Routes())
	t.Cleanup(server.Close)
	return server
}

func request(t *testing.T, server *httptest.Server, method, path, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, server.URL+path, bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	response, err := server.Client().Do(req)
	if err != nil {
		t.Fatalf("request %s %s: %v", method, path, err)
	}
	return response
}

func assertRequest(t *testing.T, server *httptest.Server, method, path, body string, wantStatus int, wantBody string) {
	t.Helper()
	response := request(t, server, method, path, body)
	defer response.Body.Close()

	gotBody, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if response.StatusCode != wantStatus {
		t.Fatalf("%s %s status = %d, want %d; body = %s", method, path, response.StatusCode, wantStatus, gotBody)
	}
	if string(gotBody) != wantBody {
		t.Fatalf("%s %s body = %q, want %q", method, path, gotBody, wantBody)
	}
}

func assertAPIError(t *testing.T, response *http.Response, wantCode, wantMessage string) {
	t.Helper()
	var body struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if body.Code != wantCode || body.Message != wantMessage {
		t.Fatalf("error response = %+v, want code %q and message %q", body, wantCode, wantMessage)
	}
}
