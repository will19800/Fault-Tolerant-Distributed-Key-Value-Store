package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/api"
	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/storage"
)

func TestClientDisplaysLeaderHintAndUnknownWriteOutcome(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("timeout") == "true" {
			w.WriteHeader(http.StatusGatewayTimeout)
			_, _ = w.Write([]byte(`{"code":"timeout","message":"request timed out; write outcome may be unknown","outcome":"unknown"}`))
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"code":"not_leader","message":"request must be sent to the current leader","leader_id":"node-2"}`))
	}))
	defer server.Close()

	assertRun(t, server.URL, []string{"put", "key", "value"}, exitFailure, "", "not_leader: request must be sent to the current leader\nknown leader: node-2\n")

	var stdout, stderr bytes.Buffer
	client := server.Client()
	if got := request(context.Background(), client, http.MethodPut, server.URL+"/?timeout=true", strings.NewReader("value"), &stdout, &stderr); got != exitFailure {
		t.Fatalf("timeout exit = %d", got)
	}
	if !strings.Contains(stderr.String(), "did not retry because this write may already have committed") {
		t.Fatalf("timeout stderr = %q", stderr.String())
	}
}

func TestKeyLifecycle(t *testing.T) {
	server := newTestServer(t)
	key := "user/profile ?"
	value := `{"theme":"dark"}`

	assertRun(t, server.URL, []string{"put", key, value}, exitOK, "", "")
	assertRun(t, server.URL, []string{"get", key}, exitOK, value, "")
	assertRun(t, server.URL, []string{"put", key, "replacement"}, exitOK, "", "")
	assertRun(t, server.URL, []string{"get", key}, exitOK, "replacement", "")
	assertRun(t, server.URL, []string{"delete", key}, exitOK, "", "")
	assertRun(t, server.URL, []string{"get", key}, exitNotFound, "", "key_not_found: key does not exist\n")
}

func TestHealth(t *testing.T) {
	server := newTestServer(t)
	assertRun(t, server.URL, []string{"health"}, exitOK, `{"status":"ok"}`+"\n", "")
}

func TestUsageErrors(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "missing command", want: "usage:"},
		{name: "unknown command", args: []string{"unknown"}, want: `unknown command "unknown"`},
		{name: "missing put value", args: []string{"put", "key"}, want: "put <key> <value>"},
		{name: "invalid timeout", args: []string{"-timeout", "0s", "health"}, want: "timeout must be positive"},
		{name: "invalid server", args: []string{"-server", "localhost:8080", "health"}, want: "invalid server URL"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if got := run(context.Background(), test.args, &stdout, &stderr); got != exitUsage {
				t.Fatalf("exit code = %d, want %d", got, exitUsage)
			}
			if !strings.Contains(stderr.String(), test.want) {
				t.Fatalf("stderr = %q, want containing %q", stderr.String(), test.want)
			}
		})
	}
}

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	handler, err := api.NewHandler(storage.NewMemoryStore(), api.DefaultMaxValueBytes)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	server := httptest.NewServer(handler.Routes())
	t.Cleanup(server.Close)
	return server
}

func assertRun(t *testing.T, serverURL string, args []string, wantExit int, wantStdout, wantStderr string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	fullArgs := append([]string{"-server", serverURL}, args...)
	if got := run(context.Background(), fullArgs, &stdout, &stderr); got != wantExit {
		t.Fatalf("run(%v) exit code = %d, want %d; stderr = %q", args, got, wantExit, stderr.String())
	}
	if stdout.String() != wantStdout {
		t.Fatalf("run(%v) stdout = %q, want %q", args, stdout.String(), wantStdout)
	}
	if stderr.String() != wantStderr {
		t.Fatalf("run(%v) stderr = %q, want %q", args, stderr.String(), wantStderr)
	}
}
