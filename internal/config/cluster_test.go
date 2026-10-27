package config_test

import (
	"strings"
	"testing"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/config"
)

func TestParseCluster(t *testing.T) {
	cluster, err := config.ParseCluster("node-2", "node-1=127.0.0.1:9001,node-2=127.0.0.1:9002,node-3=127.0.0.1:9003")
	if err != nil {
		t.Fatalf("ParseCluster: %v", err)
	}
	if cluster.LocalID != "node-2" || len(cluster.Members) != 3 {
		t.Fatalf("cluster = %+v", cluster)
	}
	if cluster.Members[1].RaftAddress != "127.0.0.1:9002" {
		t.Fatalf("second member = %+v", cluster.Members[1])
	}
}

func TestClusterValidation(t *testing.T) {
	valid := "node-1=127.0.0.1:9001,node-2=127.0.0.1:9002,node-3=127.0.0.1:9003"
	tests := []struct {
		name    string
		localID string
		members string
		want    string
	}{
		{name: "malformed member", localID: "node-1", members: "node-1", want: "expected ID=host:port"},
		{name: "unsupported size", localID: "node-1", members: "node-1=:9001,node-2=:9002", want: "exactly 3 or 5"},
		{name: "local member missing", localID: "node-4", members: valid, want: "is not a cluster member"},
		{name: "duplicate ID", localID: "node-1", members: "node-1=:9001,node-1=:9002,node-3=:9003", want: "duplicate member ID"},
		{name: "duplicate address", localID: "node-1", members: "node-1=:9001,node-2=:9001,node-3=:9003", want: "duplicate Raft address"},
		{name: "invalid ID", localID: "node-1", members: "node-1=:9001,node/2=:9002,node-3=:9003", want: "unsupported character"},
		{name: "invalid address", localID: "node-1", members: "node-1=:9001,node-2=localhost,node-3=:9003", want: "expected host:port"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := config.ParseCluster(test.localID, test.members)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("ParseCluster error = %v, want containing %q", err, test.want)
			}
		})
	}
}
