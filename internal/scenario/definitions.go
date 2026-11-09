package scenario

import (
	"time"

	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/failure"
	"github.com/will19800/Fault-Tolerant-Distributed-Key-Value-Store/internal/raft"
)

func definitions() []Definition {
	return []Definition{
		define("election-isolate-leader-three", Election, 3, 1001,
			"Verify a three-node majority replaces an isolated leader.", "Isolate node-1 in both directions for 700ms during writes.",
			workload("mixed", 101, 2, 12, 64), "Nodes 2 and 3 elect a leader; node-1 rejoins as a follower.",
			fault(partition([][]raft.NodeID{{"node-1"}, {"node-2", "node-3"}}, 200*time.Millisecond, 700*time.Millisecond), nil), AutomaticReelection),
		define("election-isolate-leader-five", Election, 5, 1002,
			"Verify a five-node majority replaces an isolated leader.", "Isolate node-1 from four peers for 650ms.",
			workload("steady-writes", 102, 3, 16, 96), "The four-node side elects a leader and remains available.",
			fault(partition([][]raft.NodeID{{"node-1"}, {"node-2", "node-3", "node-4", "node-5"}}, 150*time.Millisecond, 650*time.Millisecond), nil), AutomaticReelection),
		define("election-drop-candidate-votes", Election, 3, 1003,
			"Exercise a failed candidacy followed by a later successful term.", "Drop node-2 RequestVote traffic to both peers for 450ms.",
			workload("read-heavy", 103, 2, 8, 48), "The split election makes no unsafe progress and a later election succeeds.",
			fault([]failure.NetworkRule{drop("node-2", "node-1", failure.RequestVote, 0, 450*time.Millisecond), drop("node-2", "node-3", failure.RequestVote, 0, 450*time.Millisecond)}, nil), AutomaticReelection),
		define("election-asymmetric-vote-loss", Election, 3, 1004,
			"Verify one-way vote loss cannot create two leaders.", "Drop RequestVote from node-3 to node-1 for 600ms while reverse traffic remains.",
			workload("mixed", 104, 3, 10, 80), "At most one leader gains a quorum and the cluster stabilizes after healing.",
			fault([]failure.NetworkRule{drop("node-3", "node-1", failure.RequestVote, 100*time.Millisecond, 600*time.Millisecond)}, nil), AutomaticReelection),
		define("election-delayed-votes", Election, 5, 1005,
			"Exercise election timeout races under delayed vote RPCs.", "Delay votes from node-2 to nodes 3, 4, and 5 by 180ms with jitter.",
			workload("mixed", 105, 4, 20, 72), "Terms may advance, but one majority leader eventually serves traffic.",
			fault([]failure.NetworkRule{delay("node-2", "node-3", failure.RequestVote, 180*time.Millisecond, 40*time.Millisecond, 0, 800*time.Millisecond), delay("node-2", "node-4", failure.RequestVote, 180*time.Millisecond, 40*time.Millisecond, 0, 800*time.Millisecond), delay("node-2", "node-5", failure.RequestVote, 180*time.Millisecond, 40*time.Millisecond, 0, 800*time.Millisecond)}, nil), AutomaticReelection),
		define("election-lossy-vote-rounds", Election, 5, 1006,
			"Verify repeated lossy elections eventually recover with a majority.", "Apply 45% seeded loss to node-1 vote requests toward three peers for 900ms.",
			workload("steady-writes", 106, 3, 24, 128), "Failed rounds are retried and a unique leader eventually commits writes.",
			fault([]failure.NetworkRule{loss("node-1", "node-2", failure.RequestVote, .45, 0, 900*time.Millisecond), loss("node-1", "node-3", failure.RequestVote, .45, 0, 900*time.Millisecond), loss("node-1", "node-4", failure.RequestVote, .45, 0, 900*time.Millisecond)}, nil), AutomaticReelection),
		define("election-crash-current-leader", Election, 3, 1007,
			"Verify automatic election after abrupt leader termination.", "Crash node-1 at 250ms and restart it at 950ms.",
			workload("mixed", 107, 2, 14, 64), "Nodes 2 and 3 elect a replacement before node-1 rejoins.",
			fault(nil, actions(action(250*time.Millisecond, "node-1", failure.Crash), action(950*time.Millisecond, "node-1", failure.Restart))), AutomaticReelection),
		define("election-graceful-leader-stop", Election, 5, 1008,
			"Compare leader replacement after a graceful stop.", "Gracefully stop node-1 at 200ms and restart it at 800ms.",
			workload("read-heavy", 108, 4, 18, 56), "A replacement is elected and the stopped node catches up after restart.",
			fault(nil, actions(action(200*time.Millisecond, "node-1", failure.GracefulStop), action(800*time.Millisecond, "node-1", failure.Restart))), AutomaticReelection),

		define("replication-drop-one-follower", Replication, 3, 1101,
			"Verify writes commit while one follower misses append traffic.", "Drop AppendEntries from node-1 to node-3 for 700ms.",
			workload("steady-writes", 111, 2, 16, 128), "The leader commits with node-2 and later repairs node-3.",
			fault([]failure.NetworkRule{drop("node-1", "node-3", failure.AppendEntries, 100*time.Millisecond, 700*time.Millisecond)}, nil), SuffixRepair),
		define("replication-asymmetric-append-loss", Replication, 3, 1102,
			"Exercise one-way loss of append requests without losing responses elsewhere.", "Drop node-1 to node-2 AppendEntries for 650ms only.",
			workload("overwrite", 112, 3, 6, 96), "Node-3 forms the quorum and node-2 converges after healing.",
			fault([]failure.NetworkRule{drop("node-1", "node-2", failure.AppendEntries, 150*time.Millisecond, 650*time.Millisecond)}, nil), SuffixRepair),
		define("replication-lossy-follower", Replication, 5, 1103,
			"Verify retry and match-index tracking under packet loss.", "Apply 35% AppendEntries loss from node-1 to node-5 for 900ms.",
			workload("mixed", 113, 4, 22, 80), "The majority commits continuously and node-5 catches up.",
			fault([]failure.NetworkRule{loss("node-1", "node-5", failure.AppendEntries, .35, 0, 900*time.Millisecond)}, nil), SuffixRepair),
		define("replication-delayed-follower", Replication, 5, 1104,
			"Exercise slow follower progress and batching.", "Delay node-1 to node-4 AppendEntries by 140ms plus jitter for one second.",
			workload("steady-writes", 114, 5, 25, 160), "Slow replication does not block the quorum and the follower converges.",
			fault([]failure.NetworkRule{delay("node-1", "node-4", failure.AppendEntries, 140*time.Millisecond, 35*time.Millisecond, 0, time.Second)}, nil), SuffixRepair),
		define("replication-two-slow-followers", Replication, 5, 1105,
			"Verify a five-node leader commits with two delayed followers.", "Delay AppendEntries to nodes 4 and 5 by different fixed durations.",
			workload("overwrite", 115, 4, 10, 192), "Three prompt replicas form a quorum; both slow replicas later match.",
			fault([]failure.NetworkRule{delay("node-1", "node-4", failure.AppendEntries, 120*time.Millisecond, 10*time.Millisecond, 100*time.Millisecond, 800*time.Millisecond), delay("node-1", "node-5", failure.AppendEntries, 190*time.Millisecond, 20*time.Millisecond, 100*time.Millisecond, 800*time.Millisecond)}, nil), SuffixRepair),
		define("replication-heartbeat-gap", Replication, 3, 1106,
			"Exercise follower elections caused by a temporary heartbeat gap.", "Drop leader append traffic to node-3 for 500ms during active writes.",
			workload("mixed", 116, 2, 12, 72), "The quorum remains safe and any term change preserves the committed prefix.",
			fault([]failure.NetworkRule{drop("node-1", "node-3", failure.AppendEntries, 250*time.Millisecond, 500*time.Millisecond)}, nil), SuffixRepair),
		define("replication-bidirectional-follower-cut", Replication, 5, 1107,
			"Verify an isolated follower cannot disturb an available quorum.", "Partition node-5 from the other four nodes for 850ms.",
			workload("delete-heavy", 117, 3, 18, 64), "Four nodes commit deletes and node-5 repairs its retained suffix.",
			fault(partition([][]raft.NodeID{{"node-5"}, {"node-1", "node-2", "node-3", "node-4"}}, 100*time.Millisecond, 850*time.Millisecond), nil), SuffixRepair),
		define("replication-jitter-all-followers", Replication, 5, 1108,
			"Verify ordering under varied delivery latency to every follower.", "Add distinct AppendEntries jitter on all four leader links for 900ms.",
			workload("steady-writes", 118, 6, 30, 100), "Entries apply in log order despite out-of-phase RPC completion.",
			fault([]failure.NetworkRule{delay("node-1", "node-2", failure.AppendEntries, 20*time.Millisecond, 30*time.Millisecond, 0, 900*time.Millisecond), delay("node-1", "node-3", failure.AppendEntries, 30*time.Millisecond, 40*time.Millisecond, 0, 900*time.Millisecond), delay("node-1", "node-4", failure.AppendEntries, 40*time.Millisecond, 50*time.Millisecond, 0, 900*time.Millisecond), delay("node-1", "node-5", failure.AppendEntries, 50*time.Millisecond, 60*time.Millisecond, 0, 900*time.Millisecond)}, nil), SuffixRepair),

		define("quorum-three-node-minority", QuorumLoss, 3, 1201,
			"Verify a lone leader cannot acknowledge writes.", "Partition node-1 from nodes 2 and 3 for 750ms.",
			workload("steady-writes", 121, 2, 10, 80), "Requests to the minority time out while the majority can elect and commit.",
			fault(partition([][]raft.NodeID{{"node-1"}, {"node-2", "node-3"}}, 100*time.Millisecond, 750*time.Millisecond), nil), MinorityNoProgress, AutomaticReelection),
		define("quorum-five-node-two-two-one", QuorumLoss, 5, 1202,
			"Verify only a three-node connected component can make progress.", "Split nodes into groups of two, two, and one for 800ms.",
			workload("mixed", 122, 5, 25, 96), "No component reaches three nodes, so writes wait until healing.",
			fault(partition([][]raft.NodeID{{"node-1", "node-2"}, {"node-3", "node-4"}, {"node-5"}}, 150*time.Millisecond, 800*time.Millisecond), nil), MinorityNoProgress),
		define("quorum-five-node-majority-minority", QuorumLoss, 5, 1203,
			"Verify a three-node side remains available against a two-node minority.", "Partition nodes 1-2 from nodes 3-5 for 900ms.",
			workload("overwrite", 123, 4, 12, 128), "Only the three-node side acknowledges writes; all nodes later converge.",
			fault(partition([][]raft.NodeID{{"node-1", "node-2"}, {"node-3", "node-4", "node-5"}}, 100*time.Millisecond, 900*time.Millisecond), nil), MinorityNoProgress, AutomaticReelection),
		define("quorum-crash-two-of-three", QuorumLoss, 3, 1204,
			"Verify loss of two processes stops all progress.", "Crash nodes 2 and 3, then restart both at staggered times.",
			workload("mixed", 124, 2, 14, 64), "Node-1 cannot commit alone and resumes only after a peer restarts.",
			fault(nil, actions(action(200*time.Millisecond, "node-2", failure.Crash), action(250*time.Millisecond, "node-3", failure.Crash), action(850*time.Millisecond, "node-2", failure.Restart), action(950*time.Millisecond, "node-3", failure.Restart))), MinorityNoProgress),
		define("quorum-crash-three-of-five", QuorumLoss, 5, 1205,
			"Verify two survivors cannot commit in a five-node cluster.", "Crash nodes 3, 4, and 5 and restart them in sequence.",
			workload("steady-writes", 125, 3, 20, 112), "Writes remain unacknowledged until the third live node returns.",
			fault(nil, actions(action(180*time.Millisecond, "node-3", failure.Crash), action(220*time.Millisecond, "node-4", failure.Crash), action(260*time.Millisecond, "node-5", failure.Crash), action(800*time.Millisecond, "node-3", failure.Restart), action(900*time.Millisecond, "node-4", failure.Restart), action(time.Second, "node-5", failure.Restart))), MinorityNoProgress),
		define("quorum-lossy-majority-links", QuorumLoss, 5, 1206,
			"Exercise intermittent quorum loss without a hard partition.", "Apply 80% seeded append loss from leader to nodes 3, 4, and 5.",
			workload("mixed", 126, 5, 24, 88), "Acknowledgements occur only on successful quorum rounds; timeouts remain ambiguous.",
			fault([]failure.NetworkRule{loss("node-1", "node-3", failure.AppendEntries, .8, 100*time.Millisecond, 800*time.Millisecond), loss("node-1", "node-4", failure.AppendEntries, .8, 100*time.Millisecond, 800*time.Millisecond), loss("node-1", "node-5", failure.AppendEntries, .8, 100*time.Millisecond, 800*time.Millisecond)}, nil), MinorityNoProgress),
		define("quorum-asymmetric-majority-blind", QuorumLoss, 3, 1207,
			"Verify asymmetric blindness cannot bypass quorum rules.", "Drop AppendEntries from node-1 to both followers while their outbound traffic remains.",
			workload("read-heavy", 127, 3, 9, 48), "The old leader cannot confirm reads or writes and a reachable majority recovers.",
			fault([]failure.NetworkRule{drop("node-1", "node-2", failure.AppendEntries, 150*time.Millisecond, 700*time.Millisecond), drop("node-1", "node-3", failure.AppendEntries, 150*time.Millisecond, 700*time.Millisecond)}, nil), MinorityNoProgress, AutomaticReelection),

		define("healing-majority-partition", Healing, 3, 1301,
			"Verify full convergence after a leader-isolating partition heals.", "Isolate node-1 for 600ms, then restore every link.",
			workload("mixed", 131, 3, 15, 96), "The former leader discards any uncommitted suffix and catches up.",
			fault(partition([][]raft.NodeID{{"node-1"}, {"node-2", "node-3"}}, 100*time.Millisecond, 600*time.Millisecond), nil), SuffixRepair, AutomaticReelection),
		define("healing-three-way-partition", Healing, 3, 1302,
			"Verify recovery after every node is isolated.", "Create three singleton groups for 550ms.",
			workload("steady-writes", 132, 3, 12, 72), "No writes commit during isolation; one leader emerges after healing.",
			fault(partition([][]raft.NodeID{{"node-1"}, {"node-2"}, {"node-3"}}, 150*time.Millisecond, 550*time.Millisecond), nil), MinorityNoProgress, AutomaticReelection),
		define("healing-asymmetric-links", Healing, 5, 1303,
			"Verify recovery after independent one-way link failures.", "Drop three directed append links for different overlapping windows.",
			workload("overwrite", 133, 4, 14, 104), "Quorum writes remain ordered and every lagging node converges.",
			fault([]failure.NetworkRule{drop("node-1", "node-3", failure.AppendEntries, 100*time.Millisecond, 500*time.Millisecond), drop("node-1", "node-4", failure.AppendEntries, 200*time.Millisecond, 600*time.Millisecond), drop("node-5", "node-1", failure.RequestVote, 300*time.Millisecond, 450*time.Millisecond)}, nil), SuffixRepair),
		define("healing-loss-burst", Healing, 3, 1304,
			"Verify repair after a high-loss burst ends.", "Apply 70% append loss to both followers for 500ms.",
			workload("mixed", 134, 2, 18, 120), "Timed-out operations are reconciled and acknowledged state converges.",
			fault([]failure.NetworkRule{loss("node-1", "node-2", failure.AppendEntries, .7, 200*time.Millisecond, 500*time.Millisecond), loss("node-1", "node-3", failure.AppendEntries, .7, 200*time.Millisecond, 500*time.Millisecond)}, nil), SuffixRepair),
		define("healing-long-delay", Healing, 5, 1305,
			"Verify delayed in-flight RPCs do not corrupt post-heal state.", "Delay two follower links by 350ms for 700ms.",
			workload("delete-heavy", 135, 5, 20, 84), "Late RPCs are term-checked and all replicas converge.",
			fault([]failure.NetworkRule{delay("node-1", "node-4", failure.AppendEntries, 350*time.Millisecond, 30*time.Millisecond, 100*time.Millisecond, 700*time.Millisecond), delay("node-1", "node-5", failure.AppendEntries, 350*time.Millisecond, 30*time.Millisecond, 100*time.Millisecond, 700*time.Millisecond)}, nil), SuffixRepair),
		define("healing-minority-with-uncommitted-writes", Healing, 5, 1306,
			"Verify speculative minority suffixes are replaced safely.", "Partition nodes 1-2 from nodes 3-5 while writes target both sides.",
			workload("partition-targeted", 136, 6, 26, 128), "Only majority acknowledgements survive; minority suffixes are repaired.",
			fault(partition([][]raft.NodeID{{"node-1", "node-2"}, {"node-3", "node-4", "node-5"}}, 120*time.Millisecond, 780*time.Millisecond), nil), MinorityNoProgress, SuffixRepair, AutomaticReelection),
		define("healing-repeated-partitions", Healing, 3, 1307,
			"Verify two separated partition windows both heal cleanly.", "Cut node-3 twice using two non-overlapping append-drop windows.",
			workload("steady-writes", 137, 3, 17, 90), "Node-3 catches up after each window without duplicate application.",
			fault([]failure.NetworkRule{drop("node-1", "node-3", failure.AppendEntries, 100*time.Millisecond, 250*time.Millisecond), drop("node-1", "node-3", failure.AppendEntries, 550*time.Millisecond, 300*time.Millisecond)}, nil), SuffixRepair),

		define("restart-follower-once", Restart, 3, 1401,
			"Verify a follower recovers durable state during active writes.", "Crash node-3 at 250ms and restart it at 700ms.",
			workload("steady-writes", 141, 2, 16, 128), "The leader remains available and node-3 replays then catches up.",
			fault(nil, actions(action(250*time.Millisecond, "node-3", failure.Crash), action(700*time.Millisecond, "node-3", failure.Restart))), SuffixRepair),
		define("restart-former-leader", Restart, 3, 1402,
			"Verify a crashed former leader rejoins under a newer term.", "Crash node-1 and restart it after nodes 2-3 elect a leader.",
			workload("mixed", 142, 3, 14, 96), "Node-1 accepts the newer term and converges without serving stale reads.",
			fault(nil, actions(action(200*time.Millisecond, "node-1", failure.Crash), action(850*time.Millisecond, "node-1", failure.Restart))), AutomaticReelection, SuffixRepair),
		define("restart-two-followers-staggered", Restart, 5, 1403,
			"Verify a leader remains available during staggered follower restarts.", "Restart nodes 4 and 5 in overlapping windows.",
			workload("overwrite", 143, 4, 18, 144), "Three live nodes commit and both restarted followers catch up.",
			fault(nil, actions(action(180*time.Millisecond, "node-4", failure.Crash), action(300*time.Millisecond, "node-5", failure.Crash), action(650*time.Millisecond, "node-4", failure.Restart), action(800*time.Millisecond, "node-5", failure.Restart))), SuffixRepair),
		define("restart-full-cluster-sequential", Restart, 3, 1404,
			"Verify durable recovery when every process is stopped and restarted.", "Crash all nodes, then restart them one at a time.",
			workload("mixed", 144, 2, 11, 80), "Previously acknowledged state survives and the cluster elects a leader.",
			fault(nil, actions(action(250*time.Millisecond, "node-1", failure.Crash), action(270*time.Millisecond, "node-2", failure.Crash), action(290*time.Millisecond, "node-3", failure.Crash), action(700*time.Millisecond, "node-1", failure.Restart), action(800*time.Millisecond, "node-2", failure.Restart), action(900*time.Millisecond, "node-3", failure.Restart))), AutomaticReelection),
		define("restart-graceful-follower", Restart, 5, 1405,
			"Verify graceful follower shutdown and recovery.", "Gracefully stop node-5 at 220ms and restart it at 620ms.",
			workload("read-heavy", 145, 5, 24, 64), "The remaining cluster stays available and node-5 rejoins cleanly.",
			fault(nil, actions(action(220*time.Millisecond, "node-5", failure.GracefulStop), action(620*time.Millisecond, "node-5", failure.Restart))), SuffixRepair),
		define("restart-leader-twice", Restart, 5, 1406,
			"Verify repeated leader process loss does not lose acknowledged writes.", "Crash node-1, restart it, then crash node-2 after leadership changes.",
			workload("steady-writes", 146, 5, 28, 112), "Each available majority elects a leader and preserves the committed prefix.",
			fault(nil, actions(action(180*time.Millisecond, "node-1", failure.Crash), action(600*time.Millisecond, "node-1", failure.Restart), action(800*time.Millisecond, "node-2", failure.Crash), action(1200*time.Millisecond, "node-2", failure.Restart))), AutomaticReelection),
		define("restart-minority-pair", Restart, 5, 1407,
			"Verify simultaneous loss and return of two followers.", "Crash nodes 4 and 5 together, then restart in reverse order.",
			workload("delete-heavy", 147, 4, 21, 88), "The majority commits throughout and both followers restore final deletes.",
			fault(nil, actions(action(200*time.Millisecond, "node-4", failure.Crash), action(200*time.Millisecond, "node-5", failure.Crash), action(700*time.Millisecond, "node-5", failure.Restart), action(760*time.Millisecond, "node-4", failure.Restart))), SuffixRepair),
		define("restart-during-loss", Restart, 3, 1408,
			"Combine follower restart with a lossy catch-up link.", "Restart node-3 while its append link has 40% seeded loss.",
			workload("mixed", 148, 3, 15, 104), "Retries eventually catch node-3 up without changing committed history.",
			fault([]failure.NetworkRule{loss("node-1", "node-3", failure.AppendEntries, .4, 450*time.Millisecond, 700*time.Millisecond)}, actions(action(200*time.Millisecond, "node-3", failure.Crash), action(500*time.Millisecond, "node-3", failure.Restart))), SuffixRepair),

		define("snapshot-transfer-lagging-follower", Snapshot, 3, 1501,
			"Verify a follower behind compaction installs a snapshot.", "Block node-3 append traffic long enough for leader compaction, then heal.",
			workload("snapshot-building", 151, 2, 40, 256), "Node-3 installs the snapshot and then applies the retained suffix.",
			fault([]failure.NetworkRule{drop("node-1", "node-3", failure.AppendEntries, 100*time.Millisecond, 900*time.Millisecond)}, nil), SnapshotState, SuffixRepair),
		define("snapshot-lossy-install", Snapshot, 5, 1502,
			"Verify snapshot installation retries after packet loss.", "Apply 55% loss to InstallSnapshot from node-1 to node-5.",
			workload("snapshot-building", 152, 4, 48, 192), "A successful retry restores node-5 to the snapshot boundary.",
			fault([]failure.NetworkRule{loss("node-1", "node-5", failure.InstallSnapshot, .55, 500*time.Millisecond, 900*time.Millisecond)}, nil), SnapshotState, SuffixRepair),
		define("snapshot-delayed-install", Snapshot, 3, 1503,
			"Verify delayed snapshot transfer does not block quorum commits.", "Delay InstallSnapshot to node-3 by 300ms plus jitter.",
			workload("snapshot-building", 153, 3, 44, 224), "Nodes 1-2 continue committing and node-3 catches up afterward.",
			fault([]failure.NetworkRule{delay("node-1", "node-3", failure.InstallSnapshot, 300*time.Millisecond, 60*time.Millisecond, 450*time.Millisecond, time.Second)}, nil), SnapshotState, SuffixRepair),
		define("snapshot-restart-lagging-follower", Snapshot, 5, 1504,
			"Verify a restarted follower uses a compacted leader snapshot.", "Keep node-5 down across compaction, then restart it.",
			workload("snapshot-building", 154, 5, 52, 160), "Node-5 installs state at the compacted boundary and converges.",
			fault(nil, actions(action(150*time.Millisecond, "node-5", failure.Crash), action(1000*time.Millisecond, "node-5", failure.Restart))), SnapshotState, SuffixRepair),
		define("snapshot-former-leader-rejoin", Snapshot, 3, 1505,
			"Verify a former leader behind compaction can rejoin.", "Crash node-1 while the new leader compacts, then restart node-1.",
			workload("snapshot-building", 155, 3, 46, 208), "The old leader installs the newer snapshot and follows the current term.",
			fault(nil, actions(action(180*time.Millisecond, "node-1", failure.Crash), action(1100*time.Millisecond, "node-1", failure.Restart))), SnapshotState, AutomaticReelection, SuffixRepair),
		define("snapshot-compaction-active-writes", Snapshot, 5, 1506,
			"Verify retained suffix ordering while snapshots are transferred.", "Delay snapshot transfer and append traffic to node-4 in separate windows.",
			workload("snapshot-building", 156, 6, 60, 256), "Snapshot state plus the retained suffix equals the leader's final state.",
			fault([]failure.NetworkRule{delay("node-1", "node-4", failure.InstallSnapshot, 220*time.Millisecond, 40*time.Millisecond, 400*time.Millisecond, 900*time.Millisecond), loss("node-1", "node-4", failure.AppendEntries, .3, 100*time.Millisecond, 500*time.Millisecond)}, nil), SnapshotState, SuffixRepair),
		define("snapshot-two-lagging-followers", Snapshot, 5, 1507,
			"Verify two followers independently recover beyond compaction.", "Block append traffic to nodes 4 and 5 for different durations.",
			workload("snapshot-building", 157, 5, 58, 176), "Both followers install or replay the required state and converge.",
			fault([]failure.NetworkRule{drop("node-1", "node-4", failure.AppendEntries, 120*time.Millisecond, 850*time.Millisecond), drop("node-1", "node-5", failure.AppendEntries, 120*time.Millisecond, 1050*time.Millisecond)}, nil), SnapshotState, SuffixRepair),

		define("combined-partition-leader-crash", Combined, 5, 1601,
			"Exercise leader loss inside a majority/minority partition.", "Partition 2+3, crash node-1 on the majority side, then heal and restart.",
			workload("partition-targeted", 161, 6, 28, 128), "The remaining majority elects safely and all replicas converge.",
			fault(partition([][]raft.NodeID{{"node-1", "node-2", "node-3"}, {"node-4", "node-5"}}, 100*time.Millisecond, 900*time.Millisecond), actions(action(300*time.Millisecond, "node-1", failure.Crash), action(1050*time.Millisecond, "node-1", failure.Restart))), AutomaticReelection, MinorityNoProgress, SuffixRepair),
		define("combined-loss-delay-restart", Combined, 3, 1602,
			"Exercise three fault types during concurrent client traffic.", "Lose append RPCs to node-2, delay node-3, and restart node-2.",
			workload("mixed", 162, 4, 20, 112), "Only quorum-confirmed operations are acknowledged and final state converges.",
			fault([]failure.NetworkRule{loss("node-1", "node-2", failure.AppendEntries, .35, 100*time.Millisecond, 900*time.Millisecond), delay("node-1", "node-3", failure.AppendEntries, 90*time.Millisecond, 25*time.Millisecond, 100*time.Millisecond, 900*time.Millisecond)}, actions(action(400*time.Millisecond, "node-2", failure.Crash), action(700*time.Millisecond, "node-2", failure.Restart))), SuffixRepair),
		define("combined-snapshot-partition-restart", Combined, 5, 1603,
			"Combine snapshot catch-up with partition and process restart.", "Partition node-5 through compaction, restart it, and add snapshot loss.",
			workload("snapshot-building", 163, 6, 64, 240), "Node-5 retries installation and reaches the same applied state.",
			fault(append(partition([][]raft.NodeID{{"node-5"}, {"node-1", "node-2", "node-3", "node-4"}}, 100*time.Millisecond, 700*time.Millisecond), loss("node-1", "node-5", failure.InstallSnapshot, .4, 800*time.Millisecond, 700*time.Millisecond)), actions(action(350*time.Millisecond, "node-5", failure.Crash), action(780*time.Millisecond, "node-5", failure.Restart))), SnapshotState, SuffixRepair),
		define("combined-repeated-leader-failures", Combined, 5, 1604,
			"Verify safety across process loss and a later directed partition.", "Crash node-1, then isolate node-2 after the replacement election.",
			workload("steady-writes", 164, 5, 34, 136), "Successive majorities retain one committed prefix and re-elect automatically.",
			fault(partition([][]raft.NodeID{{"node-2"}, {"node-1", "node-3", "node-4", "node-5"}}, 650*time.Millisecond, 600*time.Millisecond), actions(action(180*time.Millisecond, "node-1", failure.Crash), action(1000*time.Millisecond, "node-1", failure.Restart))), AutomaticReelection, SuffixRepair),
		define("combined-quorum-loss-full-restart", Combined, 3, 1605,
			"Verify durable acknowledged state across partition and full outage.", "Lose quorum, crash all nodes, restart all nodes, then heal.",
			workload("mixed", 165, 3, 18, 104), "Pre-fault acknowledgements survive and ambiguous operations are reconciled.",
			fault(partition([][]raft.NodeID{{"node-1"}, {"node-2", "node-3"}}, 150*time.Millisecond, 850*time.Millisecond), actions(action(400*time.Millisecond, "node-1", failure.Crash), action(420*time.Millisecond, "node-2", failure.Crash), action(440*time.Millisecond, "node-3", failure.Crash), action(750*time.Millisecond, "node-1", failure.Restart), action(800*time.Millisecond, "node-2", failure.Restart), action(850*time.Millisecond, "node-3", failure.Restart))), MinorityNoProgress, AutomaticReelection),
		define("combined-asymmetric-partition-loss", Combined, 5, 1606,
			"Exercise asymmetric isolation plus loss on the surviving majority.", "Block node-1 outbound append traffic and add 25% loss between majority peers.",
			workload("partition-targeted", 166, 7, 30, 96), "A reachable majority elects one leader and repairs every suffix after healing.",
			fault([]failure.NetworkRule{drop("node-1", "node-2", failure.AppendEntries, 100*time.Millisecond, 850*time.Millisecond), drop("node-1", "node-3", failure.AppendEntries, 100*time.Millisecond, 850*time.Millisecond), drop("node-1", "node-4", failure.AppendEntries, 100*time.Millisecond, 850*time.Millisecond), drop("node-1", "node-5", failure.AppendEntries, 100*time.Millisecond, 850*time.Millisecond), loss("node-3", "node-4", failure.AppendEntries, .25, 200*time.Millisecond, 600*time.Millisecond)}, nil), AutomaticReelection, SuffixRepair),
		define("combined-chaos-seeded", Combined, 5, 1607,
			"Run a reproducible mix of loss, delay, crash, and restart during heavy traffic.", "Apply seeded loss and jitter on two links while restarting two followers.",
			workload("mixed", 167, 8, 32, 160), "The available majority preserves acknowledgements and every node converges.",
			fault([]failure.NetworkRule{loss("node-1", "node-4", failure.AppendEntries, .3, 100*time.Millisecond, 1100*time.Millisecond), delay("node-1", "node-5", failure.AppendEntries, 80*time.Millisecond, 70*time.Millisecond, 100*time.Millisecond, 1100*time.Millisecond)}, actions(action(350*time.Millisecond, "node-3", failure.Crash), action(620*time.Millisecond, "node-3", failure.Restart), action(800*time.Millisecond, "node-4", failure.Crash), action(1050*time.Millisecond, "node-4", failure.Restart))), SuffixRepair),
	}
}

func define(id string, category Category, nodes int, seed int64, purpose, schedule string, workload Workload, expected string, faults failure.Scenario, extra ...Assertion) Definition {
	faults.Seed = seed
	assertions := []Assertion{CommittedPrefix, OrderedApplication, AcknowledgedWrites, EventualConvergence}
	assertions = append(assertions, extra...)
	return Definition{
		ID: id, Category: category, Purpose: purpose, Nodes: nodes, FaultSchedule: schedule,
		Workload: workload, Expected: expected, Assertions: assertions, Failure: faults,
		Command: ReproductionCommand(id, seed),
	}
}

func workload(pattern string, operations, concurrency, keySpace, valueBytes int) Workload {
	return Workload{Pattern: pattern, Operations: operations, Concurrency: concurrency, KeySpace: keySpace, ValueBytes: valueBytes}
}

func fault(network []failure.NetworkRule, process []failure.ProcessAction) failure.Scenario {
	return failure.Scenario{Network: network, Actions: process}
}

func drop(from, to raft.NodeID, message failure.MessageType, start, duration time.Duration) failure.NetworkRule {
	return failure.NetworkRule{From: from, To: to, Message: message, Drop: true, StartAfter: start, Duration: duration}
}

func loss(from, to raft.NodeID, message failure.MessageType, rate float64, start, duration time.Duration) failure.NetworkRule {
	return failure.NetworkRule{From: from, To: to, Message: message, LossRate: rate, StartAfter: start, Duration: duration}
}

func delay(from, to raft.NodeID, message failure.MessageType, fixed, jitter, start, duration time.Duration) failure.NetworkRule {
	return failure.NetworkRule{From: from, To: to, Message: message, Delay: fixed, Jitter: jitter, StartAfter: start, Duration: duration}
}

func action(at time.Duration, node raft.NodeID, kind failure.ProcessActionKind) failure.ProcessAction {
	return failure.ProcessAction{At: at, Node: node, Kind: kind}
}

func actions(items ...failure.ProcessAction) []failure.ProcessAction { return items }

func partition(groups [][]raft.NodeID, start, duration time.Duration) []failure.NetworkRule {
	rules, err := failure.PartitionRules(groups, start, duration)
	if err != nil {
		panic(err)
	}
	return rules
}
