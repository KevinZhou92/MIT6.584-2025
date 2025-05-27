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

			go rf.sendAppendEntries(server, rf.buildHeartBeatArgs(), &AppendEntriesReply{})
		}
		// Debug(dInfo, "Server %d sent heartbeat to followers", rf.me)
	}
}

// ---------------------
// RPC Heartbeat Utils
// ---------------------
func (rf *Raft) buildHeartBeatArgs() *AppendEntriesArgs {
	currentTerm := rf.getCurrentTerm()
	leaderCommitIndex := rf.getServerCommitIndex()
	appendEntriesArgs := AppendEntriesArgs{
		currentTerm, rf.me, []LogEntry{}, -1, -1, leaderCommitIndex}

	return &appendEntriesArgs
}
