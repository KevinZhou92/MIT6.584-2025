package raft

import (
	"bytes"
	"fmt"

	"6.5840/labgob"
	"6.5840/raftapi"
)

type InstallSnapShotArgs struct {
	Term                  int
	LeaderId              int
	LastIncludedIndex     int
	LastIncludedIndexTerm int
	Offset                int
	Data                  []byte
	Done                  bool
}

func (args InstallSnapShotArgs) String() string {
	return fmt.Sprintf(
		"InstallSnapShotArgs{Term: %d, LeaderId: %d, LastIncludedIndex: %d, LastIncludedIndexTerm: %d, Offset: %d, Done: %t}",
		args.Term, args.LeaderId, args.LastIncludedIndex, args.LastIncludedIndexTerm, args.Offset, args.Done)
}

type InstallSnapShotReply struct {
	Term int
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(virtualIndex int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Debug(dSnap, "Server %d receive snapshot request for absoluteIndex %d", rf.me, virtualIndex)

	// Index will be the absolute index assume the log is not truncated
	// realIndex represent the actual log index in the current truncated rf.logs
	realIndex := rf.getRealIndex(virtualIndex)
	// Your code here (3D).
	// Don't truncate log if there are not enough logs yet
	if len(rf.logs) < 1 || realIndex >= len(rf.logs) || realIndex < 0 {
		Debug(dSnap, "Server %d doesn't have enough logs, current log size %d, realIndex %d", rf.me, len(rf.logs), realIndex)
		return
	}

	newLastIncludedTerm := rf.logs[realIndex].Term

	// truncate log and write snapshot to persister
	Debug(dSnap, "Server %d truncate logs with log size %d, truncate with virtualIndex %d, and realIndex %d", rf.me, len(rf.logs), virtualIndex, realIndex)

	newLogs := append([]LogEntry{}, rf.logs[realIndex+1:]...)
	rf.snapshotState = &SnapshotState{virtualIndex, newLastIncludedTerm}
	rf.logs = newLogs

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.electionState)
	e.Encode(newLogs)
	e.Encode(rf.snapshotState)
	raftstate := w.Bytes()

	rf.persister.Save(raftstate, snapshot)
	Debug(dSnap, "Server %d truncate through virtualIndex %d, realIndex %d, snapshotstate: %v, server now has %d logs", rf.me, virtualIndex, realIndex, rf.snapshotState, len(newLogs))
}

func (rf *Raft) installSnapshot(server int) {
	installSnapShotArgs := rf.buildInstallSnapshotArgs()
	installSnapShotReply := InstallSnapShotReply{}
	Debug(dSnap, "Server %d send snapshot %v to peer %d", rf.me, installSnapShotArgs, server)

	ok := rf.sendInstallSnapshot(server, &installSnapShotArgs, &installSnapShotReply)
	if !ok {
		Debug(dSnap, "Server %d failed to install snapshot %v on peer %d", rf.me, installSnapShotArgs, server)
		return
	}

	replyTerm := installSnapShotReply.Term
	// Leader is not with the highest term, step down to follower
	if replyTerm > rf.getCurrentTerm() {
		Debug(dSnap, "Server %d is not leader anymore cuz there is a peer has a higher term", rf.me)
		rf.setElectionState(FOLLOWER, replyTerm, -1)
	}
	// Update next index and match index after installing the snapshot
	rf.setNextIndexForPeer(server, installSnapShotArgs.LastIncludedIndex+1)
	rf.setMatchIndexForPeer(server, installSnapShotArgs.LastIncludedIndex)

	Debug(dSnap, "Server %d installed snapshot %v on peer %d, set nextIndex as %d, matchIndex as %d",
		rf.me, installSnapShotArgs, server, rf.getNextIndexForPeer(server), rf.getMatchIndexForPeer(server))
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapShotArgs, reply *InstallSnapShotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)

	return ok
}

func (rf *Raft) InstallSnapShot(args *InstallSnapShotArgs, reply *InstallSnapShotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	requestTerm, leaderId, lastIncludedIndex, lastIncludedIndexTerm, _, data, _ :=
		args.Term, args.LeaderId, args.LastIncludedIndex, args.LastIncludedIndexTerm, args.Offset, args.Data, args.Done
	reply.Term = rf.electionState.CurrentTerm
	Debug(dSnap, "Server %d received install snapshot request: %v from leader %d", rf.me, args, leaderId)

	// The leader that send the install snapshot request is not leader anymore
	if requestTerm < rf.electionState.CurrentTerm {
		Debug(dSnap, "Server %d received install snapshot request: %v from an old leader", rf.me, args)
		return
	}

	if lastIncludedIndex < rf.snapshotState.LastIncludedIndex {
		Debug(dSnap, "Server %d received install snapshot request: %v but has more recent snapshot %d", rf.me, args, rf.snapshotState.LastIncludedIndex)
		return
	}

	// truncate the log if snapshot contains a prefix of the existing logs
	if lastIncludedIndex < len(rf.logs)+rf.snapshotState.LastIncludedIndex {
		rf.logs = append([]LogEntry{}, rf.logs[lastIncludedIndex-rf.snapshotState.LastIncludedIndex:]...)
	} else {
		rf.logs = []LogEntry{}
	}

	if lastIncludedIndex > rf.logState.lastAppliedIndex {
		Debug(dSnap, "Server %d will advance lastAppliedIndex according to snapshot request: %v. Logstate: %v", rf.me, args, rf.logState)
		rf.pendingSnapshotApplyMsg = &raftapi.ApplyMsg{
			CommandValid:  false,
			Command:       -1,
			CommandIndex:  -1,
			SnapshotValid: true,
			Snapshot:      data,
			SnapshotTerm:  lastIncludedIndexTerm,
			SnapshotIndex: lastIncludedIndex}

		rf.logState.lastAppliedIndex = lastIncludedIndex
		rf.logState.commitIndex = max(rf.logState.commitIndex, lastIncludedIndex)
		Debug(dSnap, "Server %d update logState %s with last included index %d", rf.me, rf.logState, lastIncludedIndex)
	}

	rf.snapshotState = &SnapshotState{lastIncludedIndex, lastIncludedIndexTerm}

	Debug(dSnap, "Server %d installed snapshot request: %v from leader %d, current state: %v, current log size: %d", rf.me, args, leaderId, rf.logState, len(rf.logs))

	Debug(dSnap, "Server %d has a pending applyMsg %v", rf.me, rf.pendingSnapshotApplyMsg)
	rf.applyCond.Signal()

	rf.persistStateAndSnapshot(data)
}

func (rf *Raft) buildInstallSnapshotArgs() InstallSnapShotArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return InstallSnapShotArgs{
		rf.electionState.CurrentTerm,
		rf.me,
		rf.snapshotState.LastIncludedIndex,
		rf.snapshotState.LastIncludedTerm,
		0,
		rf.persister.ReadSnapshot(),
		true,
	}
}
