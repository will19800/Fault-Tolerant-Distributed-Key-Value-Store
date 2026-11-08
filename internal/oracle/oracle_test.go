package oracle

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
)

func TestOracleRecordsOperationOutcomesAndOwnsBytes(t *testing.T) {
	oracle, err := New("operation-outcomes", 42)
	if err != nil {
		t.Fatal(err)
	}
	value := []byte("first")
	putID, err := oracle.Issue("node-1", Put, "key", value)
	if err != nil {
		t.Fatal(err)
	}
	value[0] = 'X'
	if err := oracle.Acknowledge(putID, nil); err != nil {
		t.Fatal(err)
	}
	getID, err := oracle.Issue("node-2", Get, "key", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := oracle.Timeout(getID); err != nil {
		t.Fatal(err)
	}
	if err := oracle.Timeout(getID); err == nil {
		t.Fatal("second outcome unexpectedly succeeded")
	}

	trace := oracle.Trace(nil)
	if got := string(trace.Operations[0].Value); got != "first" {
		t.Fatalf("recorded value = %q", got)
	}
	if trace.Operations[0].Outcome != Acknowledged || trace.Operations[1].Outcome != TimedOut {
		t.Fatalf("operation outcomes = %+v", trace.Operations)
	}
	wantEvents := []string{"issued", "acknowledged", "issued", "timed_out"}
	for index, want := range wantEvents {
		if event := trace.Events[index]; event.Type != want || event.Sequence != uint64(index+1) {
			t.Fatalf("event %d = %+v, want type %q", index, event, want)
		}
	}
}

func TestOracleSupportsConcurrentClients(t *testing.T) {
	oracle, _ := New("concurrent-clients", 7)
	const clients = 32
	errors := make(chan error, clients)
	var group sync.WaitGroup
	for client := 0; client < clients; client++ {
		group.Add(1)
		go func() {
			defer group.Done()
			id, err := oracle.Issue("node-1", Put, "key", []byte("value"))
			if err == nil {
				err = oracle.Acknowledge(id, nil)
			}
			if err != nil {
				errors <- err
			}
			_ = oracle.Trace(nil)
		}()
	}
	group.Wait()
	close(errors)
	for err := range errors {
		t.Fatal(err)
	}
	trace := oracle.Trace(nil)
	if len(trace.Operations) != clients || len(trace.Events) != clients*2 {
		t.Fatalf("recorded %d operations and %d events", len(trace.Operations), len(trace.Events))
	}
	for _, operation := range trace.Operations {
		if operation.Outcome != Acknowledged {
			t.Fatalf("operation %d outcome = %q", operation.ID, operation.Outcome)
		}
	}
}

func TestCheckCommittedPrefixAcceptsDifferentPrefixLengths(t *testing.T) {
	oracle, _ := New("prefixes", 1)
	entries := []raft.Entry{
		{Index: 1, Term: 1, Command: []byte("one")},
		{Index: 2, Term: 1, Command: []byte("two")},
	}
	observe(t, oracle, ReplicaObservation{Node: "node-1", CommitIndex: 2, Entries: entries})
	observe(t, oracle, ReplicaObservation{Node: "node-2", CommitIndex: 1, Entries: entries})
	observe(t, oracle, ReplicaObservation{Node: "node-3", BaseIndex: 1, BaseTerm: 1, CommitIndex: 2, Entries: entries[1:]})
	if err := oracle.CheckCommittedPrefix(); err != nil {
		t.Fatal(err)
	}
}

func TestCheckCommittedPrefixDetectsCommandAndCompactedTermConflicts(t *testing.T) {
	tests := []struct {
		name  string
		left  ReplicaObservation
		right ReplicaObservation
	}{
		{
			name:  "command",
			left:  ReplicaObservation{Node: "node-1", CommitIndex: 1, Entries: []raft.Entry{{Index: 1, Term: 1, Command: []byte("one")}}},
			right: ReplicaObservation{Node: "node-2", CommitIndex: 1, Entries: []raft.Entry{{Index: 1, Term: 1, Command: []byte("other")}}},
		},
		{
			name:  "compacted term",
			left:  ReplicaObservation{Node: "node-1", CommitIndex: 1, Entries: []raft.Entry{{Index: 1, Term: 1, Command: []byte("one")}}},
			right: ReplicaObservation{Node: "node-2", BaseIndex: 1, BaseTerm: 2, CommitIndex: 1},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			oracle, _ := New(test.name, 1)
			observe(t, oracle, test.left)
			observe(t, oracle, test.right)
			var violation *Violation
			if err := oracle.CheckCommittedPrefix(); !errors.As(err, &violation) || violation.Kind != CommittedPrefixViolation {
				t.Fatalf("CheckCommittedPrefix error = %v", err)
			}
		})
	}
}

func TestCheckConvergence(t *testing.T) {
	oracle, _ := New("convergence", 1)
	observe(t, oracle, ReplicaObservation{Node: "node-2", State: map[string][]byte{"b": []byte("two"), "a": []byte("one")}})
	observe(t, oracle, ReplicaObservation{Node: "node-1", State: map[string][]byte{"a": []byte("one"), "b": []byte("two")}})
	if err := oracle.CheckConvergence(); err != nil {
		t.Fatal(err)
	}
	observe(t, oracle, ReplicaObservation{Node: "node-2", State: map[string][]byte{"a": []byte("changed"), "b": []byte("two")}})
	var violation *Violation
	if err := oracle.CheckConvergence(); !errors.As(err, &violation) || violation.Kind != ConvergenceViolation || !strings.Contains(err.Error(), `key "a"`) {
		t.Fatalf("CheckConvergence error = %v", err)
	}
}

func TestTraceIsDeterministicAndCanBeWritten(t *testing.T) {
	oracle, _ := New("partition-recovery", 99)
	id, _ := oracle.Issue("node-2", Put, "key", []byte("value"))
	if err := oracle.Timeout(id); err != nil {
		t.Fatal(err)
	}
	entry := raft.Entry{Index: 1, Term: 1, Command: []byte("command")}
	observe(t, oracle, ReplicaObservation{Node: "node-2", CommitIndex: 1, Entries: []raft.Entry{entry}, State: map[string][]byte{"z": []byte("last"), "a": []byte("first")}})
	observe(t, oracle, ReplicaObservation{Node: "node-1", CommitIndex: 1, Entries: []raft.Entry{entry}, State: map[string][]byte{"a": []byte("first"), "z": []byte("last")}})
	cause := &Violation{Kind: ConvergenceViolation, Text: "example"}
	first, err := oracle.MarshalTrace(cause)
	if err != nil {
		t.Fatal(err)
	}
	second, _ := oracle.MarshalTrace(cause)
	if !bytes.Equal(first, second) {
		t.Fatal("unchanged oracle produced different traces")
	}
	trace := oracle.Trace(cause)
	if trace.Scenario != "partition-recovery" || trace.Seed != 99 || trace.Replicas[0].Node != "node-1" {
		t.Fatalf("trace metadata or order = %+v", trace)
	}
	wantState := []StateEntry{{Key: "a", Value: []byte("first")}, {Key: "z", Value: []byte("last")}}
	if !reflect.DeepEqual(trace.Replicas[0].State, wantState) {
		t.Fatalf("ordered state = %+v", trace.Replicas[0].State)
	}

	path := filepath.Join(t.TempDir(), "failure.json")
	if err := oracle.WriteTrace(path, cause); err != nil {
		t.Fatal(err)
	}
	written, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(written, first) {
		t.Fatal("written trace differs from marshaled trace")
	}
}

func TestObserveReplicaRejectsInvalidCommitRange(t *testing.T) {
	oracle, _ := New("invalid", 1)
	err := oracle.ObserveReplica(ReplicaObservation{
		Node: "node-1", CommitIndex: 2,
		Entries: []raft.Entry{{Index: 1, Term: 1, Command: []byte("one")}},
	})
	if err == nil || !strings.Contains(err.Error(), "outside retained range") {
		t.Fatalf("ObserveReplica error = %v", err)
	}
}

func observe(t *testing.T, oracle *Oracle, observation ReplicaObservation) {
	t.Helper()
	if err := oracle.ObserveReplica(observation); err != nil {
		t.Fatal(err)
	}
}
