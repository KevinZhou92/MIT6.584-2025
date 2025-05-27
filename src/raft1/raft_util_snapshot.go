package raft

import (
	"errors"
)

// volatile state on all servers
func (rf *Raft) getLogEntryWithSnapshotInfo(virtualIndex int) (LogEntry, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	realIndex := rf.getRealIndex(virtualIndex)

	if realIndex < -1 || realIndex >= len(rf.logs) {
		return EMPTY_LOG_ENTRY, errors.New("log index out of range")
	}

	if realIndex == -1 {
		return LogEntry{rf.snapshotState.LastIncludedTerm, -1}, nil
	}

	return rf.logs[realIndex], nil
}

func (rf *Raft) getLogEntriesFromIndexWithSnapshotInfo(virtualIndex int) ([]LogEntry, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	realIndex := rf.getRealIndex(virtualIndex)

	if realIndex < 0 || realIndex >= len(rf.logs) {
		return []LogEntry{EMPTY_LOG_ENTRY}, errors.New("log index out of range")
	}

	logEntries := append([]LogEntry(nil), rf.logs[realIndex:]...)

	return logEntries, nil
}

func (rf *Raft) getSnapshotState() SnapshotState {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return *rf.snapshotState
}

// Virtual index is the index of the log assume the log was never truncated
// Real index is the actual index in current truncated raft logs
func (rf *Raft) getRealIndex(virtualIndex int) int {
	return virtualIndex - rf.snapshotState.LastIncludedIndex - 1
}
