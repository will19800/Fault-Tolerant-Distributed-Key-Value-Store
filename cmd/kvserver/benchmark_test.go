package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/observability"
)

type clusterBenchmarkReport struct {
	MeasuredAt  string `json:"measured_at"`
	Environment struct {
		OS           string `json:"os"`
		Architecture string `json:"architecture"`
		GoVersion    string `json:"go_version"`
		CPUs         int    `json:"cpus"`
	} `json:"environment"`
	Workload struct {
		Nodes          int    `json:"nodes"`
		Operations     int    `json:"operations"`
		Concurrency    int    `json:"concurrency"`
		ValueBytes     int    `json:"value_bytes"`
		FailoverTrials int    `json:"failover_trials"`
		Durability     string `json:"durability"`
	} `json:"workload"`
	Throughput struct {
		OperationsPerSecond float64 `json:"operations_per_second"`
		P50MS               float64 `json:"p50_ms"`
		P95MS               float64 `json:"p95_ms"`
		P99MS               float64 `json:"p99_ms"`
		MaxMS               float64 `json:"max_ms"`
	} `json:"throughput"`
	Failover struct {
		Metric  string  `json:"metric"`
		Samples int     `json:"samples"`
		P50MS   float64 `json:"p50_ms"`
		P95MS   float64 `json:"p95_ms"`
		P99MS   float64 `json:"p99_ms"`
		MaxMS   float64 `json:"max_ms"`
	} `json:"failover"`
	Limitations []string `json:"limitations"`
}

func TestClusterBenchmark(t *testing.T) {
	if os.Getenv("KV_BENCHMARK") != "1" {
		t.Skip("set KV_BENCHMARK=1 to run local cluster measurements")
	}
	const operations, concurrency, valueBytes, failoverTrials = 200, 4, 128, 5
	cluster := launchBenchmarkCluster(t)
	leader := cluster.waitLeader(t)
	latencies := make(chan time.Duration, operations)
	started := time.Now()
	var next atomic.Int64
	var group sync.WaitGroup
	for range concurrency {
		group.Add(1)
		go func() {
			defer group.Done()
			for {
				index := int(next.Add(1)) - 1
				if index >= operations {
					return
				}
				requestStarted := time.Now()
				if status, err := cluster.put(leader, fmt.Sprintf("bench-%d", index), bytes.Repeat([]byte{'v'}, valueBytes)); err != nil || status != http.StatusNoContent {
					t.Errorf("benchmark PUT %d: status=%d error=%v", index, status, err)
					return
				}
				latencies <- time.Since(requestStarted)
			}
		}()
	}
	group.Wait()
	close(latencies)
	elapsed := time.Since(started)
	throughputSamples := collectDurations(latencies)
	if len(throughputSamples) != operations {
		t.Fatalf("completed operations = %d, want %d", len(throughputSamples), operations)
	}
	cluster.close()

	failoverSamples := make([]time.Duration, 0, failoverTrials)
	for trial := range failoverTrials {
		trialCluster := launchBenchmarkCluster(t)
		oldLeader := trialCluster.waitLeader(t)
		started := time.Now()
		trialCluster.stop(oldLeader)
		deadline := time.Now().Add(5 * time.Second)
		for {
			for node := range trialCluster.addresses {
				if node == oldLeader {
					continue
				}
				if status, _ := trialCluster.put(node, fmt.Sprintf("failover-%d", trial), []byte("committed")); status == http.StatusNoContent {
					failoverSamples = append(failoverSamples, time.Since(started))
					trialCluster.close()
					goto nextTrial
				}
			}
			if time.Now().After(deadline) {
				trialCluster.close()
				t.Fatalf("trial %d did not commit after leader stop", trial)
			}
			time.Sleep(5 * time.Millisecond)
		}
	nextTrial:
	}
	sort.Slice(failoverSamples, func(i, j int) bool { return failoverSamples[i] < failoverSamples[j] })

	report := clusterBenchmarkReport{MeasuredAt: time.Now().UTC().Format(time.RFC3339)}
	report.Environment.OS, report.Environment.Architecture = runtime.GOOS, runtime.GOARCH
	report.Environment.GoVersion, report.Environment.CPUs = runtime.Version(), runtime.NumCPU()
	report.Workload.Nodes, report.Workload.Operations = 3, operations
	report.Workload.Concurrency, report.Workload.ValueBytes = concurrency, valueBytes
	report.Workload.FailoverTrials = failoverTrials
	report.Workload.Durability = "checksummed Raft files; fsync and atomic rename before acknowledgement"
	report.Throughput.OperationsPerSecond = float64(operations) / elapsed.Seconds()
	report.Throughput.P50MS, report.Throughput.P95MS = percentileMS(throughputSamples, .50), percentileMS(throughputSamples, .95)
	report.Throughput.P99MS, report.Throughput.MaxMS = percentileMS(throughputSamples, .99), percentileMS(throughputSamples, 1)
	report.Failover.Metric = "graceful leader stop to first successful committed HTTP PUT on a replacement leader"
	report.Failover.Samples = len(failoverSamples)
	report.Failover.P50MS, report.Failover.P95MS = percentileMS(failoverSamples, .50), percentileMS(failoverSamples, .95)
	report.Failover.P99MS, report.Failover.MaxMS = percentileMS(failoverSamples, .99), percentileMS(failoverSamples, 1)
	report.Limitations = []string{
		"All nodes and clients run on one machine, so results do not include real network latency or independent disks.",
		"Failover uses graceful process cancellation, not SIGKILL or power loss.",
		"The throughput workload sends PUTs to the known leader and does not measure leader discovery or mixed reads.",
	}
	output := os.Getenv("KV_BENCHMARK_OUTPUT")
	if output == "" {
		output = filepath.Join("..", "..", "benchmarks", "cluster-local.json")
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(output, append(data, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("wrote %s", output)
}

type benchmarkCluster struct {
	client    *http.Client
	addresses map[string]string
	cancels   map[string]context.CancelFunc
	done      map[string]chan error
	closed    bool
}

func launchBenchmarkCluster(t *testing.T) *benchmarkCluster {
	t.Helper()
	reserved := reserveAddresses(t, 6)
	clients, peers := reserved[:3], reserved[3:]
	peerItems := make([]string, 3)
	for index := range peerItems {
		peerItems[index] = fmt.Sprintf("node-%d=%s", index+1, peers[index])
	}
	cluster := &benchmarkCluster{
		client: &http.Client{Timeout: 250 * time.Millisecond}, addresses: make(map[string]string),
		cancels: make(map[string]context.CancelFunc), done: make(map[string]chan error),
	}
	root := t.TempDir()
	for index := range 3 {
		id := fmt.Sprintf("node-%d", index+1)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		args := []string{
			"-addr", clients[index], "-peer-addr", peers[index], "-node-id", id,
			"-peers", strings.Join(peerItems, ","), "-data-dir", filepath.Join(root, id),
			"-election-timeout", "120ms", "-heartbeat-interval", "25ms", "-rpc-timeout", "80ms", "-shutdown-timeout", "500ms",
		}
		go func() { done <- run(ctx, args, io.Discard) }()
		cluster.addresses[id], cluster.cancels[id], cluster.done[id] = "http://"+clients[index], cancel, done
	}
	return cluster
}

func (c *benchmarkCluster) waitLeader(t *testing.T) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for node, address := range c.addresses {
			response, err := c.client.Get(address + "/metrics")
			if err != nil {
				continue
			}
			var report observability.Report
			err = json.NewDecoder(response.Body).Decode(&report)
			_ = response.Body.Close()
			if err == nil && report.Raft != nil && report.Raft.Role == "leader" {
				return node
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("cluster did not elect a leader")
	return ""
}

func (c *benchmarkCluster) put(node, key string, value []byte) (int, error) {
	request, _ := http.NewRequest(http.MethodPut, c.addresses[node]+"/v1/keys/"+key, bytes.NewReader(value))
	response, err := c.client.Do(request)
	if err != nil {
		return 0, err
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	return response.StatusCode, nil
}

func (c *benchmarkCluster) stop(node string) {
	if cancel := c.cancels[node]; cancel != nil {
		cancel()
		<-c.done[node]
		delete(c.cancels, node)
	}
}

func (c *benchmarkCluster) close() {
	if c.closed {
		return
	}
	c.closed = true
	for node := range c.cancels {
		c.stop(node)
	}
}

func collectDurations(input <-chan time.Duration) []time.Duration {
	var samples []time.Duration
	for sample := range input {
		samples = append(samples, sample)
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	return samples
}

func percentileMS(samples []time.Duration, percentile float64) float64 {
	index := int(float64(len(samples)-1) * percentile)
	return float64(samples[index]) / float64(time.Millisecond)
}
