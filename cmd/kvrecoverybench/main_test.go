package main

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
)

func TestRunProducesVerifiedPercentileReport(t *testing.T) {
	var output bytes.Buffer
	if err := run([]string{"-trials=3", "-keys=10", "-value-bytes=32", "-retained-entries=3", "-target=5s"}, &output); err != nil {
		t.Fatal(err)
	}
	var got report
	if err := json.Unmarshal(output.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if !got.Passed || got.Results.Samples != 3 || got.Results.P99MS <= 0 {
		t.Fatalf("report = %+v", got)
	}
	if _, ok := got.PhaseP50MS[raft.RecoveryPeerCaughtUp]; !ok {
		t.Fatalf("missing peer catch-up phase: %+v", got.PhaseP50MS)
	}
	if _, ok := got.PhaseP50MS[raft.RecoveryLogReplayed]; !ok {
		t.Fatalf("missing retained-log phase: %+v", got.PhaseP50MS)
	}
}

func TestPercentileUsesNearestRank(t *testing.T) {
	samples := make([]time.Duration, 100)
	for index := range samples {
		samples[index] = time.Duration(index+1) * time.Millisecond
	}
	if got := percentile(samples, .50); got != 50*time.Millisecond {
		t.Fatalf("p50 = %s", got)
	}
	if got := percentile(samples, .99); got != 99*time.Millisecond {
		t.Fatalf("p99 = %s", got)
	}
}
