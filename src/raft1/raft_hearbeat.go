package raft

import (
	"time"
)

func (rf *Raft) runHeartbeatProcess() {
	Debug(dInfo, "Server %d start heartbeat process", rf.me)
	for !rf.killed() {
		time.Sleep(time.Duration(50) * time.Millisecond)

		if _, isLeader := rf.getLeaderInfo(); !isLeader {
			return
		}

		for server := range rf.peers {
			if server == rf.me {
				continue
			}
			go rf.sendHeartBeat(server)
		}
		// Debug(dInfo, "Server %d sent heartbeat to followers", rf.me)
	}
}

func (rf *Raft) sendHeartBeat(server int) {
	appendEntriesArgs := rf.buildHeartBeatArgs(server)
	appendEntriesReply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, appendEntriesArgs, appendEntriesReply)

	if ok && !appendEntriesReply.Success {
		rf.handleUnsuccessfulAppend(appendEntriesArgs.LeaderCommit, appendEntriesArgs.PrevLogIndex+1, server, appendEntriesArgs, appendEntriesReply)
	}
}

// ---------------------
// RPC Heartbeat Utils
// ---------------------
func (rf *Raft) buildHeartBeatArgs(server int) *AppendEntriesArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	currentTerm := rf.electionState.CurrentTerm
	leaderCommitIndex := rf.logState.commitIndex
	prevLogIndex := rf.peerIndexState.nextIndex[server] - 1

	prevLog, err := rf.getLogEntryWithSnapshotInfoWithoutLock(prevLogIndex)
	// Debug(dInfo, "Server %d get prevlog %v for server %d", rf.me, prevLog, server)
	appendEntriesArgs := AppendEntriesArgs{
		currentTerm, rf.me, []LogEntry{}, -1, -1, leaderCommitIndex}

	if err == nil {
		appendEntriesArgs.PrevLogIndex = prevLogIndex
		appendEntriesArgs.PrevLogTerm = prevLog.Term
	} else {
		appendEntriesArgs.PrevLogIndex = rf.snapshotState.LastIncludedIndex
		appendEntriesArgs.PrevLogTerm = rf.snapshotState.LastIncludedTerm
	}

	return &appendEntriesArgs
}
