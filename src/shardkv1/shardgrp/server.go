package shardgrp

import (
	"bytes"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/shardkv1/util"
	tester "6.5840/tester1"
)

type VersionedValue struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	gid  tester.Tgid

	// Your code here
	mu sync.Mutex

	store          map[string]VersionedValue
	clientsLastSeq map[string]int64

	shardLocked bool
}

func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch req := req.(type) {
	case rpc.GetArgs:
		var reply rpc.GetReply

		// log.Printf("get %v\n", req)
		val, ok := kv.store[req.Key]

		if !ok {
			reply.Err = rpc.ErrNoKey
			return reply
		}

		reply.Value = val.Value
		reply.Version = val.Version
		reply.Err = rpc.OK

		return reply
	case rpc.PutArgs:
		var reply rpc.PutReply

		// clientLastSeq, ok := kv.clientsLastSeq[req.ClientId]
		// if ok && req.SeqNum <= clientLastSeq {
		// 	log.Printf("$server %d received a duplicate request with seqNum %d, server sequence %d, will not execute it again\n", kv.me, req.SeqNum, clientLastSeq)
		// 	// This is a duplicate request, return OK without applying again
		// 	reply.Err = rpc.OK
		// 	return reply
		// }

		val, ok := kv.store[req.Key]
		util.Debug(util.Info, "---server %d put %v while current val is %v with putArgs: %v\n", kv.me, req, val, req)
		if !ok && req.Version != 0 {
			reply.Err = rpc.ErrVersion
			return reply
		}

		if !ok {
			val = VersionedValue{Value: "", Version: 0}
		}
		if req.Version != val.Version {
			reply.Err = rpc.ErrVersion

			return reply
		}
		val.Value = req.Value
		val.Version += 1
		kv.store[req.Key] = val
		reply.Err = rpc.OK
		kv.clientsLastSeq[req.ClientId] = req.SeqNum

		return reply
	default:
		util.Debug(util.Fatal, "KVServer %d received unknown request type %T", kv.me, req)
		panic("unknown request type")
	}
}

func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Your code here
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.store)
	e.Encode(kv.clientsLastSeq)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Your code here
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.store) != nil {
		util.Debug(util.Fatal, "%v couldn't decode store", kv.me)
	}
	if d.Decode(&kv.clientsLastSeq) != nil {
		util.Debug(util.Fatal, "%v couldn't decode clientsLastSeq", kv.me)
	}
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	getReply := res.(rpc.GetReply)
	reply.Value = getReply.Value
	reply.Version = getReply.Version
	reply.Err = getReply.Err
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	putReply := res.(rpc.PutReply)
	reply.Err = putReply.Err
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.shardLocked = true

}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	store := make(map[string]VersionedValue)
	clientsLastSeq := make(map[string]int64)
	kv := &KVServer{gid: gid, me: me, store: store, clientsLastSeq: clientsLastSeq}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// Your code here

	return []tester.IService{kv, kv.rsm.Raft()}
}
