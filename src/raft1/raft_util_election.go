package raft

var roleMap = map[Role]string{
	LEADER:    "Leader",
	CANDIDATE: "Candidate",
	FOLLOWER:  "Follower",
}

var EMPTY_LOG_ENTRY LogEntry = LogEntry{-1, -1}

func (rf *Raft) getCurrentTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.electionState.CurrentTerm
}

func (rf *Raft) getLeaderInfo() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader := rf.electionState.Role == LEADER
	var leaderId int
	if isLeader {
		leaderId = rf.me
	} else {
		leaderId = -1
	}

	return leaderId, isLeader
}

func (rf *Raft) isCandidate() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.electionState.Role == CANDIDATE
}

func (rf *Raft) isLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.electionState.Role == LEADER
}

func (rf *Raft) GetRole() Role {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.electionState.Role
}

func (rf *Raft) getElectionState() ElectionState {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return *rf.electionState
}

func (rf *Raft) setState(role Role, term int, votedFor int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.electionState.Role = role
	rf.electionState.CurrentTerm = term
	rf.electionState.VotedFor = votedFor
	rf.persist()
}
