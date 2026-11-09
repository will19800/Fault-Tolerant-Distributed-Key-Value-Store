# Failure scenarios

The `internal/failure` package supplies the controls used by adversarial
scenarios. Faults remain outside Raft: each node receives a source-aware
transport wrapper, and process lifecycle changes go through a controller.

A scenario declares a random seed, directed network rules, and scheduled
process actions:

```go
partition, err := failure.PartitionRules(
    [][]raft.NodeID{{"node-1"}, {"node-2", "node-3"}},
    500*time.Millisecond,
    2*time.Second,
)
if err != nil {
    return err
}

scenario := failure.Scenario{
    Seed: 42,
    Network: append(partition, failure.NetworkRule{
        From: "node-2", To: "node-3",
        Message: failure.AppendEntries,
        LossRate: 0.25,
        Delay: 10*time.Millisecond,
        Jitter: 5*time.Millisecond,
    }),
    Actions: []failure.ProcessAction{
        {At: time.Second, Node: "node-1", Kind: failure.Crash},
        {At: 3*time.Second, Node: "node-1", Kind: failure.Restart},
    },
}
```

`NetworkRule` selects one direction and either votes, append/heartbeat traffic,
snapshot installation, or every Raft message. It can drop every selected
message, apply seeded probabilistic loss, or delay delivery by a fixed duration
plus seeded jitter. `StartAfter` and `Duration` bound its active window. Rules
are evaluated in declaration order, and the first active match wins. Replaying
the same seed is deterministic when the scenario produces the same RPC order.

`PartitionRules` creates bidirectional cuts between groups. Asymmetric
partitions use a single directed `NetworkRule`. `RunSchedule` orders actions by
their offsets and distinguishes abrupt `Crash`, graceful `GracefulStop`, and
`Restart` hooks supplied by the scenario harness. Both delayed messages and
scheduled actions honor context cancellation.

The `internal/oracle` package records client operations independently from the
cluster. Record each request with `Issue` before sending it, then call
`Acknowledge` after a successful response or `Timeout` when the client context
expires. Timed-out writes remain ambiguous and are retained in the trace.

After faults heal, give the oracle a consistent observation from every replica:

```go
err = correctness.ObserveReplica(oracle.ReplicaObservation{
    Node:        nodeID,
    BaseIndex:   snapshotIndex,
    BaseTerm:    snapshotTerm,
    CommitIndex: status.CommitIndex,
    Entries:     retainedLog,
    State:       appliedState,
})
```

`CheckCommittedPrefix` compares every committed index visible on two replicas.
It also compares snapshot boundary terms when one replica has compacted the
corresponding command. `CheckConvergence` compares the complete applied
key-value state after the workload stops. `Check` runs both properties.

On failure, `WriteTrace` atomically writes deterministic JSON containing the
scenario name, seed, logical event order, client outcomes, committed evidence,
replica state, and violation. The same scenario and seed can then reproduce the
run without depending on wall-clock timestamps in the artifact.

The machine-readable catalog in `internal/scenario` defines 52 distinct cases:
8 election, 8 replication, 7 quorum-loss, 7 healing, 8 restart, 7 snapshot and
compaction, and 7 combined-failure scenarios. Every definition includes its
purpose, topology, seed, concrete fault controls, workload, expected behavior,
oracle assertions, and selection command. [`catalog.md`](catalog.md) is the
readable index.

Run the complete declaration suite with:

```sh
go test ./test/failure -run TestAdversarialScenarios -count=1
```

Each row in the catalog provides a command that selects its exact ID and seed.
This suite validates and serializes the declarations and initializes their
failure injectors. A declaration pass is not a successful distributed failure
trial; coordinated cluster outcomes and oracle traces must be reported
separately as evidence.
