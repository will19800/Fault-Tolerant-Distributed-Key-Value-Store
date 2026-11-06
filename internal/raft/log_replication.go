package raft

import "slices"

func lastLogPosition(entries []Entry) (LogIndex, Term) {
	return lastLogPositionFrom(0, 0, entries)
}

func lastLogPositionFrom(baseIndex LogIndex, baseTerm Term, entries []Entry) (LogIndex, Term) {
	if len(entries) == 0 {
		return baseIndex, baseTerm
	}
	last := entries[len(entries)-1]
	return last.Index, last.Term
}

func candidateLogIsFresh(request RequestVoteRequest, entries []Entry) bool {
	return candidateLogIsFreshFrom(request, 0, 0, entries)
}

func candidateLogIsFreshFrom(request RequestVoteRequest, baseIndex LogIndex, baseTerm Term, entries []Entry) bool {
	localIndex, localTerm := lastLogPositionFrom(baseIndex, baseTerm, entries)
	if request.LastLogTerm != localTerm {
		return request.LastLogTerm > localTerm
	}
	return request.LastLogIndex >= localIndex
}

func reconcileLog(local []Entry, request AppendEntriesRequest) ([]Entry, AppendEntriesResponse, bool) {
	return reconcileLogFrom(0, 0, local, request)
}

func reconcileLogFrom(baseIndex LogIndex, baseTerm Term, local []Entry, request AppendEntriesRequest) ([]Entry, AppendEntriesResponse, bool) {
	lastIndex, _ := lastLogPositionFrom(baseIndex, baseTerm, local)
	if request.PrevLogIndex < baseIndex {
		return local, AppendEntriesResponse{ConflictIndex: baseIndex + 1}, false
	}
	if request.PrevLogIndex > lastIndex {
		return local, AppendEntriesResponse{ConflictIndex: lastIndex + 1}, false
	}
	if request.PrevLogIndex > 0 {
		previousTerm, ok := logTermAt(baseIndex, baseTerm, local, request.PrevLogIndex)
		if !ok || previousTerm != request.PrevLogTerm {
			firstIndex := request.PrevLogIndex
			for firstIndex > baseIndex+1 {
				term, _ := logTermAt(baseIndex, baseTerm, local, firstIndex-1)
				if term != previousTerm {
					break
				}
				firstIndex--
			}
			return local, AppendEntriesResponse{
				ConflictIndex: firstIndex,
				ConflictTerm:  previousTerm,
			}, false
		}
	}

	for position, incoming := range request.Entries {
		localPosition := int(incoming.Index - baseIndex - 1)
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

func logTermAt(baseIndex LogIndex, baseTerm Term, entries []Entry, index LogIndex) (Term, bool) {
	if index == baseIndex {
		return baseTerm, true
	}
	if index <= baseIndex || index > baseIndex+LogIndex(len(entries)) {
		return 0, false
	}
	return entries[index-baseIndex-1].Term, true
}

func logEntryAt(baseIndex LogIndex, entries []Entry, index LogIndex) (Entry, bool) {
	if index <= baseIndex || index > baseIndex+LogIndex(len(entries)) {
		return Entry{}, false
	}
	return entries[index-baseIndex-1], true
}

func logEntriesFrom(baseIndex LogIndex, entries []Entry, index LogIndex) []Entry {
	if index <= baseIndex {
		return cloneEntries(entries)
	}
	position := index - baseIndex - 1
	if position >= LogIndex(len(entries)) {
		return nil
	}
	return cloneEntries(entries[position:])
}

func retainedSuffixForSnapshot(baseIndex LogIndex, baseTerm Term, entries []Entry, snapshotIndex LogIndex, snapshotTerm Term) []Entry {
	term, ok := logTermAt(baseIndex, baseTerm, entries, snapshotIndex)
	if !ok || term != snapshotTerm {
		return nil
	}
	return logEntriesFrom(baseIndex, entries, snapshotIndex+1)
}

func cloneEntries(entries []Entry) []Entry {
	cloned := slices.Clone(entries)
	for index := range cloned {
		cloned[index].Command = slices.Clone(cloned[index].Command)
	}
	return cloned
}
