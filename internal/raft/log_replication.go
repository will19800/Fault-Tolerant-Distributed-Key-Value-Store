package raft

import "slices"

func lastLogPosition(entries []Entry) (LogIndex, Term) {
	if len(entries) == 0 {
		return 0, 0
	}
	last := entries[len(entries)-1]
	return last.Index, last.Term
}

func candidateLogIsFresh(request RequestVoteRequest, entries []Entry) bool {
	localIndex, localTerm := lastLogPosition(entries)
	if request.LastLogTerm != localTerm {
		return request.LastLogTerm > localTerm
	}
	return request.LastLogIndex >= localIndex
}

func reconcileLog(local []Entry, request AppendEntriesRequest) ([]Entry, AppendEntriesResponse, bool) {
	if request.PrevLogIndex > LogIndex(len(local)) {
		return local, AppendEntriesResponse{ConflictIndex: LogIndex(len(local)) + 1}, false
	}
	if request.PrevLogIndex > 0 {
		previous := local[request.PrevLogIndex-1]
		if previous.Term != request.PrevLogTerm {
			firstIndex := request.PrevLogIndex
			for firstIndex > 1 && local[firstIndex-2].Term == previous.Term {
				firstIndex--
			}
			return local, AppendEntriesResponse{
				ConflictIndex: firstIndex,
				ConflictTerm:  previous.Term,
			}, false
		}
	}

	for position, incoming := range request.Entries {
		localPosition := int(incoming.Index - 1)
		if localPosition < len(local) {
			if local[localPosition].Term == incoming.Term {
				continue
			}
			next := cloneEntries(local[:localPosition])
			next = append(next, cloneEntries(request.Entries[position:])...)
			return next, AppendEntriesResponse{
				Success: true, MatchIndex: request.PrevLogIndex + LogIndex(len(request.Entries)),
			}, true
		}
		next := cloneEntries(local)
		next = append(next, cloneEntries(request.Entries[position:])...)
		return next, AppendEntriesResponse{
			Success: true, MatchIndex: request.PrevLogIndex + LogIndex(len(request.Entries)),
		}, true
	}
	return local, AppendEntriesResponse{
		Success: true, MatchIndex: request.PrevLogIndex + LogIndex(len(request.Entries)),
	}, false
}

func cloneEntries(entries []Entry) []Entry {
	cloned := slices.Clone(entries)
	for index := range cloned {
		cloned[index].Command = slices.Clone(cloned[index].Command)
	}
	return cloned
}
