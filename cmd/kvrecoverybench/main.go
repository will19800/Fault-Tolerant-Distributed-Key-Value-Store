// Command kvrecoverybench measures durable follower restart recovery.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/database"
	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/snapshot"
	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/wal"
)

type options struct {
	trials          int
	keys            int
	valueBytes      int
	retainedEntries int
	target          time.Duration
	hardware        string
	storage         string
}

type report struct {
	Metric      string                         `json:"metric"`
	Acceptance  string                         `json:"acceptance"`
	Passed      bool                           `json:"passed"`
	Workload    workloadReport                 `json:"workload"`
	Environment environmentReport              `json:"environment"`
	Results     resultReport                   `json:"results"`
	PhaseP50MS  map[raft.RecoveryPhase]float64 `json:"phase_p50_ms"`
	Limitations []string                       `json:"limitations"`
}

type workloadReport struct {
	Trials           int           `json:"trials"`
	Keys             int           `json:"keys"`
	ValueBytes       int           `json:"value_bytes"`
	RetainedEntries  int           `json:"retained_log_entries"`
	SnapshotBytes    int64         `json:"snapshot_bytes"`
	LogBytes         int64         `json:"retained_log_bytes"`
	ReferenceIndex   raft.LogIndex `json:"reference_index"`
	ClientLoad       string        `json:"client_load"`
	ElectionSettings string        `json:"election_settings"`
	Durability       string        `json:"durability"`
}

type environmentReport struct {
	Hardware        string `json:"hardware"`
	OS              string `json:"os"`
	Architecture    string `json:"architecture"`
	GoVersion       string `json:"go_version"`
	CPUs            int    `json:"cpus"`
	TimestampMethod string `json:"timestamp_method"`
	Storage         string `json:"storage"`
}

type resultReport struct {
	Samples int     `json:"samples"`
	P50MS   float64 `json:"p50_ms"`
	P95MS   float64 `json:"p95_ms"`
	P99MS   float64 `json:"p99_ms"`
	MaxMS   float64 `json:"max_ms"`
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, output io.Writer) error {
	flags := flag.NewFlagSet("kvrecoverybench", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	configuration := options{}
	flags.IntVar(&configuration.trials, "trials", 50, "number of restart trials")
	flags.IntVar(&configuration.keys, "keys", 1000, "keys represented by the snapshot")
	flags.IntVar(&configuration.valueBytes, "value-bytes", 256, "bytes per value")
	flags.IntVar(&configuration.retainedEntries, "retained-entries", 128, "log entries after the snapshot")
	flags.DurationVar(&configuration.target, "target", 300*time.Millisecond, "p99 acceptance target")
	flags.StringVar(&configuration.hardware, "hardware", "unspecified; pass -hardware", "hardware description for the report")
	flags.StringVar(&configuration.storage, "storage", "host temporary filesystem", "storage device/filesystem description")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if configuration.trials < 1 || configuration.keys < 1 || configuration.valueBytes < 1 || configuration.retainedEntries < 1 || configuration.target <= 0 {
		return errors.New("trials, keys, value-bytes, retained-entries, and target must be positive")
	}

	root, err := os.MkdirTemp("", "kv-recovery-benchmark-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(root)

	durations := make([]time.Duration, 0, configuration.trials)
	phaseDurations := make(map[raft.RecoveryPhase][]time.Duration)
	var workload workloadReport
	for trial := 0; trial < configuration.trials; trial++ {
		trialReport, elapsed, phases, err := runTrial(root, trial, configuration)
		if err != nil {
			return fmt.Errorf("trial %d: %w", trial+1, err)
		}
		workload = trialReport
		durations = append(durations, elapsed)
		for phase, phaseElapsed := range phases {
			phaseDurations[phase] = append(phaseDurations[phase], phaseElapsed)
		}
	}
	result := summarize(durations)
	p99 := percentile(durations, .99)
	phaseP50 := make(map[raft.RecoveryPhase]float64, len(phaseDurations))
	for phase, values := range phaseDurations {
		phaseP50[phase] = milliseconds(percentile(values, 0.50))
	}
	workload.Trials = configuration.trials
	benchmarkReport := report{
		Metric:     "replacement follower initialization to applied fixed reference index",
		Acceptance: fmt.Sprintf("p99 <= %s", configuration.target),
		Passed:     p99 <= configuration.target,
		Workload:   workload,
		Environment: environmentReport{
			Hardware: configuration.hardware,
			OS:       runtime.GOOS, Architecture: runtime.GOARCH, GoVersion: runtime.Version(), CPUs: runtime.NumCPU(),
			TimestampMethod: "Go time.Now monotonic elapsed time", Storage: configuration.storage,
		},
		Results: result, PhaseP50MS: phaseP50,
		Limitations: []string{
			"This benchmark measures in-process follower initialization with a synthetic leader heartbeat; it excludes OS process launch and election time.",
			"The healthy majority's first post-restart client commit and full-cluster convergence are separate metrics and are not measured here.",
		},
	}
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	return encoder.Encode(benchmarkReport)
}

func runTrial(root string, trial int, configuration options) (workloadReport, time.Duration, map[raft.RecoveryPhase]time.Duration, error) {
	directory := filepath.Join(root, fmt.Sprintf("trial-%04d", trial))
	if err := os.Mkdir(directory, 0o700); err != nil {
		return workloadReport{}, 0, nil, err
	}
	machine := database.NewReplicatedStateMachine()
	expected := make(map[string][]byte, configuration.keys)
	for index := 0; index < configuration.keys; index++ {
		key := fmt.Sprintf("key-%06d", index)
		value := fixedValue("snapshot", index, configuration.valueBytes)
		if err := applyPut(machine, key, value); err != nil {
			return workloadReport{}, 0, nil, err
		}
		expected[key] = value
	}
	snapshotState, err := machine.Snapshot()
	if err != nil {
		return workloadReport{}, 0, nil, err
	}
	baseIndex := raft.LogIndex(configuration.keys)
	snapshotStore, err := snapshot.NewFileStore(filepath.Join(directory, "state.snapshot"))
	if err != nil {
		return workloadReport{}, 0, nil, err
	}
	if err := snapshotStore.Save(raft.Snapshot{LastIncludedIndex: baseIndex, LastIncludedTerm: 1, State: snapshotState}); err != nil {
		return workloadReport{}, 0, nil, err
	}
	entries := make([]raft.Entry, 0, configuration.retainedEntries)
	for offset := 1; offset <= configuration.retainedEntries; offset++ {
		keyIndex := (offset - 1) % configuration.keys
		key := fmt.Sprintf("key-%06d", keyIndex)
		value := fixedValue("retained", offset, configuration.valueBytes)
		command, encodeErr := wal.Encode(wal.Record{Operation: wal.OperationPut, Key: key, Value: value})
		if encodeErr != nil {
			return workloadReport{}, 0, nil, encodeErr
		}
		entries = append(entries, raft.Entry{Index: baseIndex + raft.LogIndex(offset), Term: 1, Command: command})
		expected[key] = value
	}
	logStore, err := raft.NewFileLogStore(filepath.Join(directory, "raft-log.bin"))
	if err != nil {
		return workloadReport{}, 0, nil, err
	}
	if err := logStore.InstallSnapshot(baseIndex, 1, entries); err != nil {
		return workloadReport{}, 0, nil, err
	}
	hardState, err := raft.NewFileHardStateStore(filepath.Join(directory, "raft-hard-state.bin"))
	if err != nil {
		return workloadReport{}, 0, nil, err
	}
	if err := hardState.Save(raft.HardState{CurrentTerm: 1}); err != nil {
		return workloadReport{}, 0, nil, err
	}
	snapshotInfo, err := os.Stat(filepath.Join(directory, "state.snapshot"))
	if err != nil {
		return workloadReport{}, 0, nil, err
	}
	logInfo, err := os.Stat(filepath.Join(directory, "raft-log.bin"))
	if err != nil {
		return workloadReport{}, 0, nil, err
	}
	referenceIndex := baseIndex + raft.LogIndex(len(entries))

	recovered := database.NewReplicatedStateMachine()
	collector := &phaseCollector{events: make(map[raft.RecoveryPhase]time.Duration)}
	started := time.Now()
	node, err := raft.NewNode(raft.NodeConfig{
		ID: "node-1", Members: []raft.NodeID{"node-1", "node-2", "node-3"}, Transport: offlineTransport{},
		ElectionTimeout: time.Hour, MaximumElectionTimeout: time.Hour, HeartbeatInterval: time.Second,
		Persistence: hardState, LogStore: logStore, StateMachine: recovered, SnapshotStore: snapshotStore,
		RecoveryStartedAt: started, RecoveryReferenceIndex: referenceIndex, RecoveryObserver: collector.Observe,
	})
	if err != nil {
		return workloadReport{}, 0, nil, err
	}
	defer node.Close()
	response, err := node.HandleAppendEntries(context.Background(), raft.AppendEntriesRequest{
		Term: 1, LeaderID: "node-2", PrevLogIndex: referenceIndex, PrevLogTerm: 1, LeaderCommit: referenceIndex,
	})
	if err != nil || !response.Success {
		return workloadReport{}, 0, nil, fmt.Errorf("catch-up heartbeat = %+v, %v", response, err)
	}
	phases := collector.Copy()
	elapsed, ok := phases[raft.RecoveryPeerCaughtUp]
	if !ok {
		return workloadReport{}, 0, nil, errors.New("peer catch-up phase was not recorded")
	}
	for key, want := range expected {
		got, getErr := recovered.Get(key)
		if getErr != nil || !bytes.Equal(got, want) {
			return workloadReport{}, 0, nil, fmt.Errorf("independent state check failed for %q", key)
		}
	}
	return workloadReport{
		Keys: configuration.keys, ValueBytes: configuration.valueBytes, RetainedEntries: len(entries),
		SnapshotBytes: snapshotInfo.Size(), LogBytes: logInfo.Size(), ReferenceIndex: referenceIndex,
		ClientLoad: "none during recovery", ElectionSettings: "1h timeout; election excluded",
		Durability: "checksummed files; fsync before atomic rename; parent directory fsync",
	}, elapsed, phases, nil
}

func applyPut(machine *database.ReplicatedStateMachine, key string, value []byte) error {
	command, err := wal.Encode(wal.Record{Operation: wal.OperationPut, Key: key, Value: value})
	if err != nil {
		return err
	}
	return machine.Apply(command)
}

func fixedValue(prefix string, index, size int) []byte {
	seed := fmt.Sprintf("%s-%08d-", prefix, index)
	return []byte(strings.Repeat(seed, (size+len(seed)-1)/len(seed))[:size])
}

func summarize(samples []time.Duration) resultReport {
	return resultReport{Samples: len(samples), P50MS: milliseconds(percentile(samples, .50)), P95MS: milliseconds(percentile(samples, .95)), P99MS: milliseconds(percentile(samples, .99)), MaxMS: milliseconds(percentile(samples, 1))}
}

func percentile(samples []time.Duration, quantile float64) time.Duration {
	ordered := append([]time.Duration(nil), samples...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	index := int(math.Ceil(float64(len(ordered))*quantile)) - 1
	if index < 0 {
		index = 0
	}
	return ordered[index]
}

func milliseconds(duration time.Duration) float64 {
	return float64(duration) / float64(time.Millisecond)
}

type phaseCollector struct {
	mu     sync.Mutex
	events map[raft.RecoveryPhase]time.Duration
}

func (c *phaseCollector) Observe(event raft.RecoveryEvent) {
	c.mu.Lock()
	c.events[event.Phase] = event.Elapsed
	c.mu.Unlock()
}
func (c *phaseCollector) Copy() map[raft.RecoveryPhase]time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make(map[raft.RecoveryPhase]time.Duration, len(c.events))
	for phase, elapsed := range c.events {
		result[phase] = elapsed
	}
	return result
}

type offlineTransport struct{}

func (offlineTransport) RequestVote(context.Context, raft.NodeID, raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	return raft.RequestVoteResponse{}, errors.New("peer unavailable")
}
func (offlineTransport) AppendEntries(context.Context, raft.NodeID, raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	return raft.AppendEntriesResponse{}, errors.New("peer unavailable")
}
