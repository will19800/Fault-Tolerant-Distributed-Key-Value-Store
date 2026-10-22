package main

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/api"
	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/storage"
)

func TestServeLifecycle(t *testing.T) {
	store := storage.NewMemoryStore()
	handler, err := api.NewHandler(store, api.DefaultMaxValueBytes)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}

	server := &http.Server{
		Handler:           handler.Routes(),
		ReadHeaderTimeout: readHeaderTimeout,
	}
	ctx, cancel := context.WithCancel(context.Background())
	var logs bytes.Buffer
	done := make(chan error, 1)
	go func() {
		done <- serve(ctx, server, listener, time.Second, newLogger(&logs))
	}()

	client := &http.Client{Timeout: time.Second}
	baseURL := "http://" + listener.Addr().String()
	response, err := client.Get(baseURL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	if err := response.Body.Close(); err != nil {
		t.Fatalf("close health response: %v", err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("health status = %d, want %d", response.StatusCode, http.StatusOK)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serve: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not stop before test timeout")
	}

	if got := logs.String(); !strings.Contains(got, `"msg":"server started"`) || !strings.Contains(got, `"msg":"server stopped"`) {
		t.Fatalf("lifecycle logs missing start or stop event: %s", got)
	}
}

func TestRunRejectsInvalidConfiguration(t *testing.T) {
	err := run(context.Background(), []string{"-max-value-bytes", "0"}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "maximum value size must be positive") {
		t.Fatalf("run error = %v, want invalid maximum value size", err)
	}
}

func TestRunHelp(t *testing.T) {
	var output strings.Builder
	if err := run(context.Background(), []string{"-help"}, &output); err != nil {
		t.Fatalf("run help: %v", err)
	}
	if !strings.Contains(output.String(), "-shutdown-timeout") {
		t.Fatalf("help output missing shutdown timeout: %q", output.String())
	}
}
