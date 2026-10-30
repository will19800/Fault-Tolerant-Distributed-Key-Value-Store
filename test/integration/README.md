# Integration tests

`cmd/kvserver/cluster_test.go` launches a real three-node cluster with separate
client and Raft HTTP listeners and isolated data directories. The Raft suite
also checks all ten pairs of unavailable nodes in a five-node cluster; every
surviving majority must commit a write and complete a linearizable read.
