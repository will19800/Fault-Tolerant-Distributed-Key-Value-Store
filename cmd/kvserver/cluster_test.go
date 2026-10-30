package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRunLaunchesThreeNodeHTTPCluster(t *testing.T) {
	addresses := reserveAddresses(t, 6)
	clientAddresses, peerAddresses := addresses[:3], addresses[3:]
	peerItems := make([]string, 3)
	for index := range peerItems {
		peerItems[index] = fmt.Sprintf("node-%d=%s", index+1, peerAddresses[index])
	}
	peers := strings.Join(peerItems, ",")

	ctx, cancel := context.WithCancel(context.Background())
	done := make([]chan error, 3)
	for index := range 3 {
		done[index] = make(chan error, 1)
		args := []string{
			"-addr", clientAddresses[index], "-peer-addr", peerAddresses[index],
			"-node-id", fmt.Sprintf("node-%d", index+1), "-peers", peers,
			"-data-dir", filepath.Join(t.TempDir(), fmt.Sprintf("node-%d", index+1)),
			"-election-timeout", "150ms", "-heartbeat-interval", "30ms", "-rpc-timeout", "100ms",
			"-shutdown-timeout", "1s",
		}
		go func(index int) { done[index] <- run(ctx, args, io.Discard) }(index)
	}
	t.Cleanup(func() {
		cancel()
		for index := range done {
			select {
			case err := <-done[index]:
				if err != nil {
					t.Errorf("node-%d shutdown: %v", index+1, err)
				}
			case <-time.After(3 * time.Second):
				t.Errorf("node-%d did not stop", index+1)
			}
		}
	})

	client := &http.Client{Timeout: 750 * time.Millisecond}
	deadline := time.Now().Add(8 * time.Second)
	leaderURL := ""
	for time.Now().Before(deadline) && leaderURL == "" {
		for index, address := range clientAddresses {
			select {
			case err := <-done[index]:
				t.Fatalf("node-%d exited during startup: %v", index+1, err)
			default:
			}
			request, _ := http.NewRequest(http.MethodPut, "http://"+address+"/v1/keys/cluster", bytes.NewBufferString("working"))
			response, err := client.Do(request)
			if err != nil {
				continue
			}
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
			if response.StatusCode == http.StatusNoContent {
				leaderURL = "http://" + address
				break
			}
		}
		if leaderURL == "" {
			time.Sleep(20 * time.Millisecond)
		}
	}
	if leaderURL == "" {
		t.Fatal("three-node cluster did not elect a writable leader")
	}

	response, err := client.Get(leaderURL + "/v1/keys/cluster")
	if err != nil {
		t.Fatalf("linearizable GET: %v", err)
	}
	value, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK || string(value) != "working" {
		t.Fatalf("GET = status %d value %q", response.StatusCode, value)
	}
}

func reserveAddresses(t *testing.T, count int) []string {
	t.Helper()
	listeners := make([]net.Listener, count)
	addresses := make([]string, count)
	for index := range count {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("reserve address: %v", err)
		}
		listeners[index] = listener
		addresses[index] = listener.Addr().String()
	}
	for _, listener := range listeners {
		_ = listener.Close()
	}
	return addresses
}
