# Demonstration guide

Run `make check` first. Start a three-node cluster with `make cluster-3`, then
inspect `/metrics` on ports 8001-8003 to find the node whose role is `leader`.

Write and read through that node:

```sh
go run ./cmd/kvctl -server http://127.0.0.1:8001 put demo/value first
go run ./cmd/kvctl -server http://127.0.0.1:8001 get demo/value
```

If port 8001 is a follower, its response contains `leader_id`; map that ID to
the printed client URL. Show the leader's `commit_index`, `last_applied`, and
per-peer lag in `/metrics`, then inspect the JSON `commit_advanced` and `applied`
events in its log.

For a failover demonstration, identify the leader process in the terminal
running the cluster and stop that process. Poll the two surviving `/metrics`
endpoints until one reports `leader`, then issue another PUT and read it back.
Stopping the entire `make cluster-3` command stops all nodes, so use separate
node terminals when demonstrating individual process failure manually.

Reproduce measurements with `make recovery-bench` and `make cluster-bench`.
The commands overwrite the local JSON reports; preserve reports used for a
claim together with the commit and machine description.

Inspect the 52 declared fault cases in `test/failure/catalog.md`. A catalog
selection command validates and prints one deterministic declaration. It does
not yet execute the coordinated fault workload. Do not present catalog passes
as 52 successful distributed trials.
