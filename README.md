# Fault-Tolerant Distributed Key-Value Store

A learning-focused distributed key-value store in Go. The project will grow from an in-memory single-server store into a five-node Raft cluster with durable logs, snapshots, and reproducible failure testing.

The target capabilities in this README and the design document are project goals until supported by implementation and test evidence.

## Repository layout

```text
cmd/
  kvserver/          Server process
  kvctl/             Command-line client
internal/
  api/               Client-facing request handling
  config/            Node and cluster configuration
  database/          Durable single-node database composition
  raft/              Consensus state and algorithms
  snapshot/          State snapshots and restoration
  storage/           Deterministic key-value state machine
  transport/         Node-to-node Raft communication
  wal/               Durable metadata and Raft log
configs/             Example configurations
test/integration/    Multi-package and multi-node tests
test/failure/        Reproducible adversarial scenarios
benchmarks/          Performance and recovery measurements
docs/                Architecture and design documentation
```

Dependencies point inward: commands assemble components, the API submits commands through Raft, and Raft applies committed commands to storage. Storage remains independent of networking and consensus.

## Development commands

```sh
make build
make test
make test-race
make vet
make check
```

## Run the single-node server

Start the server with its default address, `127.0.0.1:8080`:

```sh
go run ./cmd/kvserver
```

Acknowledged mutations are flushed to `data/store.wal` before success is
returned. Restarting the server with the same `-data-dir` restores its state.

Use the command-line client from another terminal:

```sh
go run ./cmd/kvctl put user:123:preferences '{"theme":"dark"}'
go run ./cmd/kvctl get user:123:preferences
go run ./cmd/kvctl delete user:123:preferences
go run ./cmd/kvctl health
```

Pass `-server http://host:port` to use another server. Run either command with `-help` to see its available flags.
