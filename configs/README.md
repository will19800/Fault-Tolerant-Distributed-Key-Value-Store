# Configuration

Cluster membership is supplied to every node as the same comma-separated
`node-id=peer-address` list. Client and peer traffic use separate ports, and
each process must have a distinct data directory.

Example three-node membership:

```text
node-1=127.0.0.1:9001,node-2=127.0.0.1:9002,node-3=127.0.0.1:9003
```

Start an entire local cluster with `make cluster-3` or `make cluster-5`.
Runtime files are written beneath `.local/` and are not committed.
