package raft

import "time"

// RecoveryPhase identifies one observable point in node recovery.
type RecoveryPhase string

const (
	RecoveryProcessStarted  RecoveryPhase = "process_started"
	RecoveryHardStateLoaded RecoveryPhase = "hard_state_loaded"
	RecoveryLogLoaded       RecoveryPhase = "log_loaded"
	RecoverySnapshotLoaded  RecoveryPhase = "snapshot_loaded"
	RecoveryStateRestored   RecoveryPhase = "state_restored"
	RecoveryNodeStarted     RecoveryPhase = "node_started"
	RecoveryStateApplied    RecoveryPhase = "state_applied"
	RecoveryLogReplayed     RecoveryPhase = "retained_log_replayed"
	RecoveryPeerCaughtUp    RecoveryPhase = "peer_caught_up"
	RecoveryFirstCommit     RecoveryPhase = "first_successful_commit"
)

// RecoveryEvent reports elapsed monotonic time and the relevant Raft index.
type RecoveryEvent struct {
	Phase   RecoveryPhase
	Elapsed time.Duration
	Index   LogIndex
}

// RecoveryObserver receives recovery events synchronously. Implementations
// should return quickly and must be safe when called from the Raft goroutine.
type RecoveryObserver func(RecoveryEvent)
