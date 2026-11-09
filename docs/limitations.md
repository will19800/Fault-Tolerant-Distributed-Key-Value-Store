# Limitations and future work

- Membership is fixed at three or five nodes. There is no joint-consensus
  reconfiguration, sharding, multi-region placement, or learner role.
- There is no authentication, authorization, TLS, encryption at rest, resource
  quota, admission control, or production hardening.
- PUT and DELETE are idempotent at the state level, but requests have no IDs or
  deduplication table. Retrying an ambiguous timeout can append another entry.
- Values are full opaque blobs. There are no transactions, range scans,
  compare-and-swap operations, secondary indexes, or watch streams.
- File replacement favors simple correctness over write throughput. Each Raft
  log update rewrites a checksummed file; segmented append-only storage and
  group commit are likely performance improvements.
- The JSON metrics endpoint is local-process instrumentation without retention,
  authentication, histograms, tracing, or alerting integration.
- The recovery benchmark covers in-process follower initialization through a
  fixed reference index. It excludes OS launch, election, first majority commit,
  former-leader recovery, and full-cluster convergence.
- The cluster benchmark uses one machine, known-leader PUTs, and graceful leader
  stop. Its five failover samples do not establish a tail-latency guarantee.
- Fifty-two scenarios are fully declared and validated, but the catalog runner
  does not yet orchestrate their workloads and assertions. No “zero data loss
  across 50+ scenarios” claim is earned until per-scenario outcomes and oracle
  traces are produced.
- The failure model is crash-stop/crash-recovery with non-Byzantine nodes,
  eventually delivered messages in a connected majority, and functioning
  durable storage. Correlated disk loss and malicious behavior are outside it.

High-value next steps are a coordinated catalog executor, request deduplication,
segmented logs with group commit, abrupt-process benchmark trials, dynamic
membership, Prometheus metrics, distributed tracing, and security controls.
