package raft

// conflictNextIndex uses a follower's rejection hint to skip indexes or an
// entire conflicting term instead of retrying one entry at a time.
func conflictNextIndex(entries []Entry, response AppendEntriesResponse) LogIndex {
	if response.ConflictTerm != 0 {
		for index := len(entries) - 1; index >= 0; index-- {
			if entries[index].Term == response.ConflictTerm {
				return entries[index].Index + 1
			}
		}
	}
	if response.ConflictIndex < 1 {
		return 1
	}
	lastIndex, _ := lastLogPosition(entries)
	return min(response.ConflictIndex, lastIndex+1)
}

// committedIndex returns the highest current-term entry stored on a quorum.
// Raft's current-term restriction indirectly commits all preceding entries.
func committedIndex(entries []Entry, followerMatches map[NodeID]LogIndex, quorum int, term Term, current LogIndex) LogIndex {
	for position := len(entries) - 1; position >= 0; position-- {
		entry := entries[position]
		if entry.Index <= current {
			break
		}
		if entry.Term != term {
			continue
		}
		replicas := 1 // The leader has every entry in its own log.
		for _, match := range followerMatches {
			if match >= entry.Index {
				replicas++
			}
		}
		if replicas >= quorum {
			return entry.Index
		}
	}
	return current
}
