# Recovery Benchmarks

Run the repeatable follower recovery workload with:

```sh
go run ./cmd/kvrecoverybench \
  -trials=50 \
  -keys=1000 \
  -value-bytes=256 \
  -retained-entries=128 \
  -target=300ms \
  -hardware="your CPU and memory" \
  -storage="your storage device and filesystem"
```

The acceptance statistic is declared before measurement: p99 must be at or
below 300ms for this workload. Each trial prepares a checksummed snapshot of
1,000 keys and a durable 128-entry retained log, then starts a replacement
follower, restores the snapshot, and sends a synthetic leader heartbeat that
commits through a fixed reference index. Timing starts immediately before
`raft.NewNode` and ends when the follower applies that reference index.
Election time and preparation of the durable files are excluded.

Every trial compares all recovered keys and values with a separately maintained
expected-state map. A mismatch aborts the run instead of reporting a latency.
The JSON report includes sample count, p50, p95, p99, maximum, median phase
timestamps, snapshot and retained-log sizes, Go/OS/CPU information, durability
policy, election settings, client load, and timestamp method.

This in-process experiment does not include operating-system process launch,
the healthy majority's first client commit after restart, or full five-node
convergence. Its result therefore characterizes the declared local recovery
workload; it is not yet evidence for the broader claim that a full cluster
resumes commits within 300ms. Those experiments must remain separate.

`recovery-local.json` records one run on the development machine. Re-run the
command on the hardware used for final claims and retain results even when the
target is missed.
