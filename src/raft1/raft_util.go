package raft

func (rf *Raft) getLogState() LogState {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return *rf.logState
}

func (rf *Raft) getServerCommitIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.logState.commitIndex
}

// volatile state on all servers
func (rf *Raft) setServerCommitIndex(commitIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logState.commitIndex = commitIndex
}

func (rf *Raft) getLogSize() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return len(rf.logs)
}

func (rf *Raft) getLogSizeWithSnapshotInfo() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return len(rf.logs) + rf.snapshotState.LastIncludedIndex + 1
}

func (rf *Raft) getLastLogTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if len(rf.logs) == 0 {
		return rf.snapshotState.LastIncludedTerm
	}

	return rf.logs[len(rf.logs)-1].Term
}

// volatile state on all servers
func (rf *Raft) initializePeerIndexState() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.peerIndexState = &PeerIndexState{
		nextIndex:  make([]int, len(rf.peers)),
		matchIndex: make([]int, len(rf.peers)),
	}

	for idx := range len(rf.peers) {
		rf.peerIndexState.nextIndex[idx] = len(rf.logs) + rf.snapshotState.LastIncludedIndex + 1
		rf.peerIndexState.matchIndex[idx] = 0
	}
}

func (rf *Raft) getPeerIndexState() PeerIndexState {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return *rf.peerIndexState
}

func (rf *Raft) getMatchIndexForPeer(server int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.peerIndexState.matchIndex[server]
}

func (rf *Raft) setMatchIndexForPeer(server int, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.peerIndexState.matchIndex[server] = index
}

func (rf *Raft) getNextIndexForPeer(server int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.peerIndexState.nextIndex[server]
}

func (rf *Raft) setNextIndexForPeer(server int, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.peerIndexState.nextIndex[server] = index
}
