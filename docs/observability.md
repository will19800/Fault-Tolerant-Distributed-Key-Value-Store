# Observability

`kvserver` writes JSON logs. Lifecycle and recovery records include message,
phase, elapsed duration, and Raft index. Consensus records use message `Raft
event` and include `event`, `node_id`, `peer_id`, `role`, `term`, `leader_id`,
and `index`. Event names cover election start, leader/follower transitions,
replication progress, commit advancement, application, and snapshot creation or
installation.

Every client listener exposes `GET /metrics` as JSON. Request measurements are
bounded by operation name (`put`, `get`, `delete`, and `health`) rather than key,
so arbitrary keys cannot create unbounded metric cardinality. Each operation
reports request count, HTTP status counts, average latency, and maximum latency.
Cluster nodes additionally report:

- node ID, role, current term, and known leader;
- commit, last-applied, last-log, and snapshot indexes;
- local apply lag;
- each follower's match index and replication lag when queried on a leader.

Example:

```sh
curl -s http://127.0.0.1:8001/metrics | python3 -m json.tool
tail -f .local/cluster-3/node-1/server.log
```

Metrics are process-local cumulative counters and reset on restart. The endpoint
has no authentication and is intended for local learning and testing. It is not
a Prometheus exposition endpoint or a production monitoring system.
