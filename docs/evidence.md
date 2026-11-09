# Evidence report

This report separates implemented mechanisms, observed measurements, and target
claims. A green unit test proves its stated property under that test; it does
not prove every execution of a distributed system.

## Goal 1: Raft and two-node availability

**Status: partially supported; keep the broad resume statement qualified.**

- Election, vote freshness, replication, current-term commit, read barriers,
  and durable ordering are tested in the [Raft test suite](../internal/raft).
- The [five-node availability test](../internal/raft/five_node_availability_test.go) exercises every pair of
  unavailable nodes in a five-node in-memory cluster.
- The [HTTP cluster test](../cmd/kvserver/cluster_test.go) starts a real three-node HTTP cluster and
  completes a write/read workflow.
- The [local cluster report](../benchmarks/cluster-local.json) records real three-node durable HTTP throughput
  and five graceful leader failovers on one machine.

Missing evidence includes repeated real five-process two-failure trials,
simultaneous versus sequential failure reports, and a statistically stronger
five-node failover distribution.

## Goal 2: recovery within 300ms

**Status: the narrow follower benchmark passes; the full-cluster statement is
not supported.**

The [local recovery report](../benchmarks/recovery-local.json) records 50 trials for 1,000 keys, 256-byte
values, and 128 retained entries. Its p99 was 1.86ms against the declared 300ms
target, and every trial independently verified restored state. The measured
interval starts immediately before in-process `raft.NewNode` and ends when the
replacement follower applies the fixed reference index. It excludes process
launch, elections, first client commit, ongoing load, former-leader restart,
and full five-node convergence. The unqualified target sentence must therefore
not be used.

## Goal 3: failure injection and 50+ scenarios

**Status: injector, oracle, and catalog exist; the zero-data-loss scenario claim
is not supported yet.**

- [`internal/failure`](../internal/failure) implements directed partitions, message selection, delay,
  jitter, seeded loss, crash, stop, restart, and schedules.
- [`internal/oracle`](../internal/oracle) records operation outcomes, checks committed-prefix
  agreement and convergence, and emits deterministic traces.
- [`internal/scenario`](../internal/scenario) and the [scenario catalog](../test/failure/catalog.md) define 52 validated, distinct
  cases across all required categories.

The selector currently validates and serializes declarations. It does not run
the workload against a coordinated cluster, so there are no 52 outcome files
or suite summary. “Zero observed data loss” cannot be claimed from this catalog.

## Measurements

| Artifact | Workload | Observation | Scope |
| --- | --- | --- | --- |
| [Recovery report](../benchmarks/recovery-local.json) | 50 follower recoveries; 1,000 keys; 128 retained entries | p99 1.86ms | In-process follower initialization only |
| [Cluster report](../benchmarks/cluster-local.json) | Three nodes; 200 durable PUTs; concurrency 4 | 94.1 ops/s; p99 55.1ms | Single host, known leader |
| [Cluster report](../benchmarks/cluster-local.json) | Five fresh three-node graceful leader stops | p50 199.7ms; max 449.6ms | Five descriptive samples |

Reproduce code checks with `make check`, recovery measurements with `make
recovery-bench`, and cluster measurements with `make cluster-bench`.
