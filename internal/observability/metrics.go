// Package observability records bounded request metrics and combines them with
// a consistent Raft snapshot for the HTTP metrics endpoint.
package observability

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
)

// RaftSource supplies the consensus metrics owned by a node event loop.
type RaftSource interface {
	Metrics(context.Context) (raft.Metrics, error)
}

// RequestStats is the cumulative request count and latency for one operation.
type RequestStats struct {
	Operation      string         `json:"operation"`
	Requests       uint64         `json:"requests"`
	StatusCounts   map[int]uint64 `json:"status_counts"`
	AverageLatency float64        `json:"average_latency_ms"`
	MaximumLatency float64        `json:"maximum_latency_ms"`
}

// Report is the JSON document exposed by the metrics endpoint.
type Report struct {
	UptimeSeconds float64        `json:"uptime_seconds"`
	Requests      []RequestStats `json:"requests"`
	Raft          *raft.Metrics  `json:"raft,omitempty"`
}

type aggregate struct {
	count    uint64
	statuses map[int]uint64
	total    time.Duration
	maximum  time.Duration
}

// Recorder owns bounded, process-local request measurements.
type Recorder struct {
	mu      sync.Mutex
	started time.Time
	byOp    map[string]*aggregate
}

// NewRecorder starts an empty metrics interval.
func NewRecorder() *Recorder {
	return &Recorder{started: time.Now(), byOp: make(map[string]*aggregate)}
}

// ObserveRequest records one completed request.
func (r *Recorder) ObserveRequest(operation string, status int, elapsed time.Duration) {
	r.mu.Lock()
	metric := r.byOp[operation]
	if metric == nil {
		metric = &aggregate{statuses: make(map[int]uint64)}
		r.byOp[operation] = metric
	}
	metric.count++
	metric.statuses[status]++
	metric.total += elapsed
	metric.maximum = max(metric.maximum, elapsed)
	r.mu.Unlock()
}

// Snapshot returns request measurements and, when provided, current consensus
// state. It never resets counters.
func (r *Recorder) Snapshot(ctx context.Context, source RaftSource) (Report, error) {
	r.mu.Lock()
	report := Report{UptimeSeconds: time.Since(r.started).Seconds()}
	operations := make([]string, 0, len(r.byOp))
	for operation := range r.byOp {
		operations = append(operations, operation)
	}
	sort.Strings(operations)
	for _, operation := range operations {
		metric := r.byOp[operation]
		statuses := make(map[int]uint64, len(metric.statuses))
		for status, count := range metric.statuses {
			statuses[status] = count
		}
		report.Requests = append(report.Requests, RequestStats{
			Operation: operation, Requests: metric.count, StatusCounts: statuses,
			AverageLatency: milliseconds(metric.total) / float64(metric.count),
			MaximumLatency: milliseconds(metric.maximum),
		})
	}
	r.mu.Unlock()
	if source != nil {
		metrics, err := source.Metrics(ctx)
		if err != nil {
			return Report{}, err
		}
		report.Raft = &metrics
	}
	return report, nil
}

func milliseconds(duration time.Duration) float64 {
	return float64(duration) / float64(time.Millisecond)
}
