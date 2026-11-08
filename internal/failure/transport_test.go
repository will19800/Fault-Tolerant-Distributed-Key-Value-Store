package failure

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
)

func TestInjectorSupportsDirectionalMessageRules(t *testing.T) {
	scenario := Scenario{Network: []NetworkRule{{
		From: "node-1", To: "node-2", Message: AppendEntries, Drop: true,
	}}}
	injector, err := NewInjector(scenario)
	if err != nil {
		t.Fatal(err)
	}
	underlying := &recordingTransport{}
	fromOne, _ := injector.Wrap("node-1", underlying)
	fromTwo, _ := injector.Wrap("node-2", underlying)

	if _, err := fromOne.AppendEntries(context.Background(), "node-2", raft.AppendEntriesRequest{}); !errors.Is(err, ErrMessageDropped) {
		t.Fatalf("directed AppendEntries error = %v", err)
	}
	if _, err := fromOne.RequestVote(context.Background(), "node-2", raft.RequestVoteRequest{}); err != nil {
		t.Fatalf("message-specific RequestVote was blocked: %v", err)
	}
	if _, err := fromTwo.AppendEntries(context.Background(), "node-1", raft.AppendEntriesRequest{}); err != nil {
		t.Fatalf("reverse AppendEntries was blocked: %v", err)
	}
	if _, err := fromOne.InstallSnapshot(context.Background(), "node-2", raft.InstallSnapshotRequest{}); err != nil {
		t.Fatalf("message-specific InstallSnapshot was blocked: %v", err)
	}
	if got := underlying.calls; !reflect.DeepEqual(got, []string{"vote:node-2", "append:node-1", "snapshot:node-2"}) {
		t.Fatalf("forwarded calls = %v", got)
	}
}

func TestInjectorUsesReproducibleLossSequence(t *testing.T) {
	scenario := Scenario{Seed: 42, Network: []NetworkRule{{
		From: "node-1", To: "node-2", Message: AnyMessage, LossRate: .5,
	}}}
	sequence := func() []bool {
		injector, _ := NewInjector(scenario)
		wrapped, _ := injector.Wrap("node-1", &recordingTransport{})
		result := make([]bool, 20)
		for index := range result {
			_, err := wrapped.RequestVote(context.Background(), "node-2", raft.RequestVoteRequest{})
			result[index] = errors.Is(err, ErrMessageDropped)
		}
		return result
	}
	first, second := sequence(), sequence()
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("same seed produced different loss: %v and %v", first, second)
	}
	if reflect.DeepEqual(first, make([]bool, len(first))) {
		t.Fatalf("loss sequence contained no drops: %v", first)
	}
}

func TestInjectorHonorsRuleWindow(t *testing.T) {
	current := time.Unix(100, 0)
	injector, err := newInjector(Scenario{Network: []NetworkRule{{
		From: "node-1", To: "node-2", Message: AnyMessage, Drop: true,
		StartAfter: 10 * time.Second, Duration: 5 * time.Second,
	}}}, func() time.Time { return current })
	if err != nil {
		t.Fatal(err)
	}
	wrapped, _ := injector.Wrap("node-1", &recordingTransport{})
	call := func() error {
		_, err := wrapped.RequestVote(context.Background(), "node-2", raft.RequestVoteRequest{})
		return err
	}
	if err := call(); err != nil {
		t.Fatalf("rule active before start: %v", err)
	}
	current = current.Add(10 * time.Second)
	if err := call(); !errors.Is(err, ErrMessageDropped) {
		t.Fatalf("rule inactive during window: %v", err)
	}
	current = current.Add(5 * time.Second)
	if err := call(); err != nil {
		t.Fatalf("rule active after duration: %v", err)
	}
}

func TestInjectorDelayHonorsCancellation(t *testing.T) {
	injector, _ := NewInjector(Scenario{Network: []NetworkRule{{
		From: "node-1", To: "node-2", Message: RequestVote, Delay: time.Second,
	}}})
	wrapped, _ := injector.Wrap("node-1", &recordingTransport{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := wrapped.RequestVote(ctx, "node-2", raft.RequestVoteRequest{})
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > 200*time.Millisecond {
		t.Fatalf("delayed call = %v after %s", err, time.Since(started))
	}
}

func TestPartitionRulesCreateBidirectionalCut(t *testing.T) {
	rules, err := PartitionRules([][]raft.NodeID{{"node-1", "node-2"}, {"node-3"}}, time.Second, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(rules) != 4 {
		t.Fatalf("partition rules = %d, want 4", len(rules))
	}
	if err := (Scenario{Network: rules}).Validate(); err != nil {
		t.Fatal(err)
	}
}

type recordingTransport struct{ calls []string }

func (t *recordingTransport) RequestVote(_ context.Context, target raft.NodeID, _ raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	t.calls = append(t.calls, "vote:"+string(target))
	return raft.RequestVoteResponse{Term: 1}, nil
}

func (t *recordingTransport) AppendEntries(_ context.Context, target raft.NodeID, _ raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	t.calls = append(t.calls, "append:"+string(target))
	return raft.AppendEntriesResponse{Term: 1, Success: true}, nil
}

func (t *recordingTransport) InstallSnapshot(_ context.Context, target raft.NodeID, _ raft.InstallSnapshotRequest) (raft.InstallSnapshotResponse, error) {
	t.calls = append(t.calls, "snapshot:"+string(target))
	return raft.InstallSnapshotResponse{Term: 1}, nil
}
