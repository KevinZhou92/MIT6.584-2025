package rsm

import (
	"sync"

	"6.5840/kvsrv1/rpc"
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
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

	go rsm.StartReaderProcess()

	// fmt.Printf("RSM server %d started with %d servers\n", me, len(servers))

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

	// fmt.Printf("RSM server %d: Submitting op %v\n", rsm.me, op)
	// index, term, isLeader
	index, _, isLeader := rsm.Raft().Start(op)
	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}

	rsm.mu.Lock()
	resChan := make(chan Result, 1)
	if rsm.resChans == nil {
		rsm.resChans = make(map[int]chan Result)
	}
	rsm.resChans[index] = resChan
	rsm.mu.Unlock()

	// fmt.Println("RSM server ", rsm.me, " submitted op at index ", index)
	res := <-resChan
	// fmt.Println("RSM server ", rsm.me, " finished op at index ", index)
	if res.Id != op.Id {
		// fmt.Printf("Deleting res chan for op %v", op)
		// Clean up recev chans as current server is not leader anymore
		rsm.CleanupRecvChans()
		return rpc.ErrWrongLeader, nil
	}

	rsm.mu.Lock()
	delete(rsm.resChans, index)
	rsm.mu.Unlock()

	return rpc.OK, res.Req
}

func (rsm *RSM) StartReaderProcess() {
	// RSM should exit if the raft instance is killed
	defer func() {
		rsm.CleanupRecvChans()
	}()

	for msg := range rsm.applyCh {
		index := msg.CommandIndex

		// fmt.Println(rsm.me)
		// fmt.Println(msg)
		res := rsm.sm.DoOp(msg.Command.(Op).Req)

		rsm.mu.Lock()
		if ch, ok := rsm.resChans[index]; ok {
			ch <- Result{Id: msg.Command.(Op).Id, Req: res}
		}
		rsm.mu.Unlock()
	}
}

func (rsm *RSM) CleanupRecvChans() {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	for index, ch := range rsm.resChans {
		close(ch)
		delete(rsm.resChans, index)
	}
}
