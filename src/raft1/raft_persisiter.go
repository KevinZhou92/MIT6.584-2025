package raft

import (
	"bytes"

	"6.5840/labgob"
)

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.electionState)
	e.Encode(rf.logs)
	e.Encode(rf.snapshotState)
	raftstate := w.Bytes()

	rf.persister.Save(raftstate, rf.persister.ReadSnapshot())
}

func (rf *Raft) persistStateAndSnapshot(data []byte) {
	// Your code here (3C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.electionState)
	e.Encode(rf.logs)
	e.Encode(rf.snapshotState)
	raftstate := w.Bytes()

	rf.persister.Save(raftstate, data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var electionState ElectionState
	var logs []LogEntry
	var snapshotState SnapshotState
	if d.Decode(&electionState) != nil || d.Decode(&logs) != nil || d.Decode(&snapshotState) != nil {
		Debug(dError, "Server %d is unable to restore persist state", rf.me)
		return
	}

	rf.electionState = &electionState
	rf.logs = logs
	rf.snapshotState = &snapshotState
}
