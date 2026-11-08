package failure

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
)

var ErrMessageDropped = errors.New("message dropped by failure injector")

type peerTransport interface {
	RequestVote(context.Context, raft.NodeID, raft.RequestVoteRequest) (raft.RequestVoteResponse, error)
	AppendEntries(context.Context, raft.NodeID, raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error)
}

type snapshotTransport interface {
	InstallSnapshot(context.Context, raft.NodeID, raft.InstallSnapshotRequest) (raft.InstallSnapshotResponse, error)
}

// Injector owns reproducible random state shared by all wrapped node links.
// Rule order is significant: the first active matching rule is applied.
type Injector struct {
	mu      sync.Mutex
	random  *rand.Rand
	rules   []NetworkRule
	started time.Time
	now     func() time.Time
}

// NewInjector validates a scenario and initializes its network fault state.
func NewInjector(scenario Scenario) (*Injector, error) {
	return newInjector(scenario, time.Now)
}

func newInjector(scenario Scenario, now func() time.Time) (*Injector, error) {
	if err := scenario.Validate(); err != nil {
		return nil, err
	}
	started := now()
	return &Injector{
		random:  rand.New(rand.NewSource(scenario.Seed)),
		rules:   append([]NetworkRule(nil), scenario.Network...),
		started: started,
		now:     now,
	}, nil
}

// Wrap returns a source-aware transport that applies configured faults before
// forwarding to next.
func (i *Injector) Wrap(source raft.NodeID, next peerTransport) (*Transport, error) {
	if err := source.Validate(); err != nil {
		return nil, fmt.Errorf("source node: %w", err)
	}
	if next == nil {
		return nil, errors.New("wrapped transport must not be nil")
	}
	return &Transport{source: source, next: next, injector: i}, nil
}

// Transport injects faults for one source node.
type Transport struct {
	source   raft.NodeID
	next     peerTransport
	injector *Injector
}

func (t *Transport) RequestVote(ctx context.Context, target raft.NodeID, request raft.RequestVoteRequest) (raft.RequestVoteResponse, error) {
	if err := t.injector.before(ctx, t.source, target, RequestVote); err != nil {
		return raft.RequestVoteResponse{}, err
	}
	return t.next.RequestVote(ctx, target, request)
}

func (t *Transport) AppendEntries(ctx context.Context, target raft.NodeID, request raft.AppendEntriesRequest) (raft.AppendEntriesResponse, error) {
	if err := t.injector.before(ctx, t.source, target, AppendEntries); err != nil {
		return raft.AppendEntriesResponse{}, err
	}
	return t.next.AppendEntries(ctx, target, request)
}

func (t *Transport) InstallSnapshot(ctx context.Context, target raft.NodeID, request raft.InstallSnapshotRequest) (raft.InstallSnapshotResponse, error) {
	if err := t.injector.before(ctx, t.source, target, InstallSnapshot); err != nil {
		return raft.InstallSnapshotResponse{}, err
	}
	next, ok := t.next.(snapshotTransport)
	if !ok {
		return raft.InstallSnapshotResponse{}, errors.New("wrapped transport does not support snapshot installation")
	}
	return next.InstallSnapshot(ctx, target, request)
}

func (i *Injector) before(ctx context.Context, from, to raft.NodeID, message MessageType) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	i.mu.Lock()
	elapsed := i.now().Sub(i.started)
	var selected *NetworkRule
	for index := range i.rules {
		rule := &i.rules[index]
		if rule.From != from || rule.To != to || (rule.Message != AnyMessage && rule.Message != message) {
			continue
		}
		if elapsed < rule.StartAfter || (rule.Duration > 0 && elapsed >= rule.StartAfter+rule.Duration) {
			continue
		}
		selected = rule
		break
	}
	if selected == nil {
		i.mu.Unlock()
		return nil
	}
	drop := selected.Drop || (selected.LossRate > 0 && i.random.Float64() < selected.LossRate)
	delay := selected.Delay
	if selected.Jitter > 0 {
		delay += time.Duration(i.random.Int63n(int64(selected.Jitter) + 1))
	}
	i.mu.Unlock()
	if drop {
		return fmt.Errorf("%w: %s %q -> %q", ErrMessageDropped, message, from, to)
	}
	if delay == 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
