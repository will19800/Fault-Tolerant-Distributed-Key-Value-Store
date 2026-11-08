package failure

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
)

func TestRunScheduleOrdersLifecycleActions(t *testing.T) {
	controller := &recordingController{}
	actions := []ProcessAction{
		{At: 2 * time.Millisecond, Node: "node-1", Kind: Restart},
		{Node: "node-1", Kind: Crash},
		{At: time.Millisecond, Node: "node-2", Kind: GracefulStop},
	}
	if err := RunSchedule(context.Background(), controller, actions); err != nil {
		t.Fatal(err)
	}
	if want := []string{"crash:node-1", "stop:node-2", "restart:node-1"}; !reflect.DeepEqual(controller.calls, want) {
		t.Fatalf("actions = %v, want %v", controller.calls, want)
	}
}

func TestRunScheduleHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := RunSchedule(ctx, &recordingController{}, []ProcessAction{{At: time.Hour, Node: "node-1", Kind: Crash}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunSchedule error = %v, want context canceled", err)
	}
}

func TestScenarioRejectsInvalidConfiguration(t *testing.T) {
	invalid := []Scenario{
		{Network: []NetworkRule{{From: "node-1", To: "node-1", Message: AnyMessage, Drop: true}}},
		{Network: []NetworkRule{{From: "node-1", To: "node-2", Message: AnyMessage, LossRate: 1.1}}},
		{Actions: []ProcessAction{{Node: "node-1", Kind: "pause"}}},
	}
	for _, scenario := range invalid {
		if err := scenario.Validate(); err == nil {
			t.Fatalf("Scenario.Validate(%+v) returned nil", scenario)
		}
	}
}

type recordingController struct{ calls []string }

func (c *recordingController) Crash(_ context.Context, node raft.NodeID) error {
	c.calls = append(c.calls, "crash:"+string(node))
	return nil
}
func (c *recordingController) Stop(_ context.Context, node raft.NodeID) error {
	c.calls = append(c.calls, "stop:"+string(node))
	return nil
}
func (c *recordingController) Restart(_ context.Context, node raft.NodeID) error {
	c.calls = append(c.calls, "restart:"+string(node))
	return nil
}
