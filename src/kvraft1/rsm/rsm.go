package rsm

import (
	"bytes"
	"log"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
	"github.com/google/uuid"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int
	Id  string
	Req any
}

type Result struct {
	Id  string
	Req any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	resChans map[int]chan Result

	lastAppliedIndex int // index of last applied log entry
	persister        *tester.Persister
	// snapshotCond     *sync.Cond
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		resChans:     make(map[int]chan Result), // initialize the map, index -> chan
		persister:    persister,
	}
	// rsm.snapshotCond = sync.NewCond(&rsm.mu)

	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

	// restore last applied index from raft and restore snapshot
	if persister.SnapshotSize() > 0 {
		rsm.lastAppliedIndex = GetLastAppliedIndex(persister, me)

		snapshot := persister.ReadSnapshot()
		rsm.sm.Restore(snapshot)
	}

	go rsm.RunReaderProcess()
	// go rsm.RunSnapShotProcess()

	// log.Printf("RSM server %d started with %d servers\n", me, len(servers))

	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	id := uuid.New().String()
	op := Op{Me: rsm.me, Id: id, Req: req}

	// index, term, isLeader
	index, _, isLeader := rsm.Raft().Start(op)
	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}
	// log.Printf("RSM server %d: Submitting op %v\n", rsm.me, op)

	rsm.mu.Lock()
	resChan := make(chan Result, 1)
	if rsm.resChans == nil {
		rsm.resChans = make(map[int]chan Result)
	}
	rsm.resChans[index] = resChan
	rsm.mu.Unlock()

	defer func() {
		rsm.mu.Lock()
		delete(rsm.resChans, index)
		rsm.mu.Unlock()
	}()

	// log.Println("RSM server ", rsm.me, " submitted op at index ", index)

	select {
	case res := <-resChan:
		// log.Println("RSM server ", rsm.me, " finished op at index ", index)
		if res.Id != op.Id {
			// Clean up recev chans as current server is not leader anymore
			return rpc.ErrWrongLeader, nil
		} else {
			return rpc.OK, res.Req
		}
	case <-time.After(2000 * time.Millisecond):
		// log.Printf("Timeout waiting for op %v", op)
		return rpc.ErrWrongLeader, nil
	}

}

func (rsm *RSM) RunReaderProcess() {
	// RSM should exit if the raft instance is killed
	for msg := range rsm.applyCh {
		rsm.mu.Lock()

		// log.Println(rsm.me)
		// log.Printf("### server %v applying msg %v\n", rsm.me, msg)
		if !msg.CommandValid && msg.SnapshotValid {
			// log.Printf("### server %v restoring snapshot\n", rsm.me)
			rsm.sm.Restore(msg.Snapshot)
			rsm.lastAppliedIndex = msg.SnapshotIndex
			rsm.mu.Unlock()
			// log.Printf("RSM server %d restored snapshot at index %d\n", rsm.me, msg.SnapshotIndex)
			continue
		}

		res := rsm.sm.DoOp(msg.Command.(Op).Req)
		index := msg.CommandIndex
		rsm.lastAppliedIndex = index

		if rsm.maxraftstate != -1 && rsm.Raft().PersistBytes() > 4*rsm.maxraftstate {
			// log.Printf("^^^ will signal snapshot process from RSM server %d with lastAppliedIndex %d", rsm.me, rsm.lastAppliedIndex)
			// rsm.snapshotCond.Signal() // WAKE UP SNAPSHOT PROCESS
			snapshot := rsm.sm.Snapshot()
			// 2. Call Raft.Snapshot()
			// log.Printf("RSM %d snapshotting at index %d with size %d with persist bytes %d\n", rsm.me, rsm.lastAppliedIndex, len(snapshot), rsm.Raft().PersistBytes())

			rsm.Raft().Snapshot(rsm.lastAppliedIndex, snapshot)
		}

		if ch, ok := rsm.resChans[index]; ok {
			ch <- Result{Id: msg.Command.(Op).Id, Req: res}
		}
		rsm.mu.Unlock()
	}
}

// We should not snapshot too often, or Raft's performance will suffer.Here is an example,
// If the lastAppliedIndex hasnt' changed since last snapshot, but our persist bytes(which include raft state and raft logs) is
// over the threshold, in this case, if we snapshot again, the persist bytes won't be reduced, and we will be stuck in snapshotting forever loop.
// So we only snapshot when lastAppliedIndex has advanced since last snapshot.
// func (rsm *RSM) RunSnapShotProcess() {
// 	// rsm.mu.Lock()
// 	// defer rsm.mu.Unlock()

// 	// for {
// 	// 	rsm.snapshotCond.Wait()

// 	// 	snapshot := rsm.sm.Snapshot()
// 	// 	// 2. Call Raft.Snapshot()
// 	// 	// log.Printf("RSM %d snapshotting at index %d with size %d with persist bytes %d\n", rsm.me, rsm.lastAppliedIndex, len(snapshot), rsm.Raft().PersistBytes())

// 	// 	rsm.Raft().Snapshot(rsm.lastAppliedIndex, snapshot)
// 	// 	// Sleep 50ms to allow KV server to apply some messages
// 	// 	time.Sleep(10 * time.Millisecond)

// 	// 	// log.Printf("RSM %d finished snapshotting, and new persist bytes is %d\n", rsm.me, newPersistBytes)
// 	// }
// }

func GetLastAppliedIndex(persister *tester.Persister, serverId int) int {
	data := persister.ReadRaftState()

	d := labgob.NewDecoder(bytes.NewBuffer(data))
	var electionState *raft.ElectionState
	var logs []*raft.LogEntry
	var snapshotState *raft.SnapshotState
	d.Decode(&electionState)
	d.Decode(&logs)
	if d.Decode(&snapshotState) != nil {
		log.Fatalf("%v couldn't decode snapshotState", serverId)
	}

	return snapshotState.LastIncludedIndex
}
