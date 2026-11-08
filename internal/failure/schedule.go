package failure

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
)

// ProcessController supplies the lifecycle operations used by a scenario.
// Crash must model abrupt termination; Stop performs graceful shutdown.
type ProcessController interface {
	Crash(context.Context, raft.NodeID) error
	Stop(context.Context, raft.NodeID) error
	Restart(context.Context, raft.NodeID) error
}

// RunSchedule executes process actions at offsets from invocation time.
func RunSchedule(ctx context.Context, controller ProcessController, actions []ProcessAction) error {
	if controller == nil {
		return fmt.Errorf("process controller must not be nil")
	}
	ordered := append([]ProcessAction(nil), actions...)
	for index, action := range ordered {
		if err := action.validate(); err != nil {
			return fmt.Errorf("process action %d: %w", index, err)
		}
	}
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].At < ordered[j].At })
	started := time.Now()
	for _, action := range ordered {
		wait := action.At - time.Since(started)
		if wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-timer.C:
			case <-ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return ctx.Err()
			}
		}
		var err error
		switch action.Kind {
		case Crash:
			err = controller.Crash(ctx, action.Node)
		case GracefulStop:
			err = controller.Stop(ctx, action.Node)
		case Restart:
			err = controller.Restart(ctx, action.Node)
		}
		if err != nil {
			return fmt.Errorf("%s node %q at %s: %w", action.Kind, action.Node, action.At, err)
		}
	}
	return nil
}
