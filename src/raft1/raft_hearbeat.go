package raft

import "time"

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

			go rf.sendAppendEntries(server, rf.buildHeartBeatArgs(server), &AppendEntriesReply{})
		}
		// Debug(dInfo, "Server %d sent heartbeat to followers", rf.me)
	}
}

// ---------------------
// RPC Heartbeat Utils
// ---------------------
func (rf *Raft) buildHeartBeatArgs(server int) *AppendEntriesArgs {
	currentTerm := rf.getCurrentTerm()
	leaderCommitIndex := rf.getServerCommitIndex()
	prevLogIndex := rf.getNextIndexForPeer(server) - 1
	prevLog, err := rf.getLogEntryWithSnapshotInfo(prevLogIndex)
	Debug(dInfo, "Server %d get prevlog %v for server %d", rf.me, prevLog, server)
	appendEntriesArgs := AppendEntriesArgs{
		currentTerm, rf.me, []LogEntry{}, -1, -1, leaderCommitIndex}

	if err == nil {
		appendEntriesArgs.PrevLogIndex = prevLogIndex
		appendEntriesArgs.PrevLogTerm = prevLog.Term
	}

	return &appendEntriesArgs
}
