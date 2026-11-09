package observability

import (
	"context"
	"testing"
	"time"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
)

func TestRecorderReportsRequestsAndRaftState(t *testing.T) {
	recorder := NewRecorder()
	recorder.ObserveRequest("put", 204, 2*time.Millisecond)
	recorder.ObserveRequest("put", 503, 4*time.Millisecond)
	report, err := recorder.Snapshot(context.Background(), staticSource{})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Requests) != 1 || report.Requests[0].Requests != 2 || report.Requests[0].AverageLatency != 3 || report.Requests[0].MaximumLatency != 4 {
		t.Fatalf("request report = %+v", report.Requests)
	}
	if report.Requests[0].StatusCounts[204] != 1 || report.Requests[0].StatusCounts[503] != 1 {
		t.Fatalf("status counts = %+v", report.Requests[0].StatusCounts)
	}
	if report.Raft == nil || report.Raft.Term != 3 || report.Raft.CommitIndex != 8 {
		t.Fatalf("Raft report = %+v", report.Raft)
	}
}

type staticSource struct{}

func (staticSource) Metrics(context.Context) (raft.Metrics, error) {
	return raft.Metrics{NodeID: "node-1", Role: "leader", Term: 3, CommitIndex: 8}, nil
}
