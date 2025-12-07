package shardgrp

import (
	"bytes"
	"log"
	"strconv"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type VersionedValue struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me         int
	dead       int32 // set by Kill()
	rsm        *rsm.RSM
	gid        tester.Tgid
	serverName string

	// Your code here
	mu sync.Mutex

	shardStore      map[shardcfg.Tshid]map[string]VersionedValue // shardId -> key : value
	shardLockStatus map[shardcfg.Tshid]bool
	shardConfigNum  map[shardcfg.Tshid]shardcfg.Tnum
}

func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch req := req.(type) {
	case rpc.GetArgs:
		var reply rpc.GetReply

		// log.Printf("get %v\n", req)
		shardId := shardcfg.Key2Shard(req.Key)
		keyStore, ok := kv.shardStore[shardId]

		if !ok {
			// log.Printf("[Shard Group] can't find shard group for key %v on server %v\n", req.Key, kv.serverName)
			reply.Err = rpc.ErrWrongGroup
			return reply
		}

		if kv.shardLockStatus[shardId] {
			// log.Printf("[Shard Group]shard id %d, for key %v is locked on server %v\n", shardId, req.Key, kv.serverName)
			reply.Err = rpc.ErrWrongGroup
			return reply
		}

		val, ok := keyStore[req.Key]
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
		// log.Printf("put %v\n", req)
		shardId := shardcfg.Key2Shard(req.Key)
		keyStore, ok := kv.shardStore[shardId]

		if !ok {
			reply.Err = rpc.ErrWrongGroup
			return reply
		}

		if kv.shardLockStatus[shardId] {
			reply.Err = rpc.ErrWrongGroup
			return reply
		}

		val, ok := keyStore[req.Key]
		// log.Printf("[Shard Group]---server %d put %v while current val is %v with putArgs: %v\n", kv.me, req, val, req)
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
		keyStore[req.Key] = val
		reply.Err = rpc.OK

		return reply
	case shardrpc.FreezeShardArgs:
		var reply shardrpc.FreezeShardReply

		shardId, configNum := req.Shard, req.Num
		if configNum < shardcfg.Tnum(kv.shardConfigNum[shardId]) {
			// log.Printf("[Shard Group - Freeze]server %v request config num %d, server config num %d for shardId %d\n", kv.serverName, configNum, kv.shardConfigNum[shardId], shardId)
			reply.Err = rpc.ErrWrongGroup
			return reply
		}
		// log.Printf("[Shard Group - Freeze]freeze shards on server %v for shardId %v and new configNum %v\n", kv.serverName, shardId, configNum)
		_, ok := kv.shardStore[shardId]

		if !ok {
			reply.Err = rpc.ErrWrongGroup
			return reply
		}

		reply.State = kv.EncodeShardData(shardId)
		kv.shardLockStatus[shardId] = true
		reply.Err = rpc.OK
		// log.Printf("[Shard Group - Freeze]encode shards %d on server %v\n", shardId, kv.serverName)

		return reply
	case shardrpc.InstallShardArgs:
		var reply shardrpc.InstallShardReply

		shardId, state, configNum := req.Shard, req.State, req.Num
		// log.Printf("[Shard Group - Install] Server %v request config num %d, server config num %d for shardId %d\n", kv.serverName, configNum, kv.shardConfigNum[shardId], shardId)
		if configNum < shardcfg.Tnum(kv.shardConfigNum[shardId]) {
			// log.Printf("[Shard Group - Install] Can't install shardId %d on server %v request config num %d, server config num %d \n", shardId, kv.serverName, configNum, kv.shardConfigNum[shardId])
			reply.Err = rpc.ErrWrongGroup
			return reply
		}

		if configNum == shardcfg.Tnum(kv.shardConfigNum[shardId]) {
			// log.Printf("[Shard Group - Install] Already installed shardId %d on server %v request config num %d, server config num %d \n", shardId, kv.serverName, configNum, kv.shardConfigNum[shardId])
			reply.Err = rpc.OK
			return reply
		}
		_, ok := kv.shardStore[shardId]

		if ok {
			// log.Printf("[Shard Group - Install] Can't install on server %v request config num %d, server config num %d for shardId %d since shard exists\n", kv.serverName, configNum, kv.shardConfigNum[shardId], shardId)
			reply.Err = rpc.ErrWrongGroup
			return reply
		}
		shardId, shardData := kv.DecodeShardData(state)
		kv.shardStore[shardId] = shardData
		kv.shardLockStatus[shardId] = false
		kv.shardConfigNum[shardId] = configNum
		reply.Err = rpc.OK

		return reply
	case shardrpc.DeleteShardArgs:
		var reply shardrpc.DeleteShardReply

		shardId, configNum := req.Shard, req.Num
		// log.Printf("[Shard Group] Delete shard %d on server %v\n", shardId, kv.serverName)
		if configNum <= shardcfg.Tnum(kv.shardConfigNum[shardId]) {
			// log.Printf("[Shard Group] Cab't Delete shard %d on server %v due to config issue\n", shardId, kv.serverName)
			reply.Err = rpc.ErrWrongGroup
			return reply
		}
		_, ok := kv.shardStore[shardId]

		if !ok {
			// log.Printf("[Shard Group] Can't delete shard %d on server %v\n", shardId, kv.serverName)
			reply.Err = rpc.ErrWrongGroup
			return reply
		}
		delete(kv.shardStore, shardId)
		delete(kv.shardLockStatus, shardId)
		delete(kv.shardConfigNum, shardId)
		reply.Err = rpc.OK

		return reply
	default:
		log.Printf("KVServer %d received unknown request type %T", kv.me, req)
		panic("unknown request type")
	}
}

func (kv *KVServer) EncodeShardData(shardId shardcfg.Tshid) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(shardId)
	e.Encode(kv.shardStore[shardId])

	return w.Bytes()
}

func (kv *KVServer) DecodeShardData(data []byte) (shardcfg.Tshid, map[string]VersionedValue) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var shardId shardcfg.Tshid
	var shardData map[string]VersionedValue
	if d.Decode(&shardId) != nil {
		log.Printf("%v couldn't decode shardId", kv.me)
	}
	if d.Decode(&shardData) != nil {
		log.Printf("%v couldn't decode shardData", kv.me)
	}

	return shardId, shardData
}

func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Your code here
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.shardStore)
	e.Encode(kv.shardConfigNum)
	e.Encode(kv.shardLockStatus)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Your code here
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.shardStore) != nil {
		log.Printf("%v couldn't decode store", kv.me)
	}
	if d.Decode(&kv.shardConfigNum) != nil {
		log.Printf("%v couldn't decode shardConfigNum", kv.me)
	}
	if d.Decode(&kv.shardLockStatus) != nil {
		log.Printf("%v couldn't decode shardLockStatus", kv.me)
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
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	// log.Printf("[Freeze]freeze shards on server %d with res %v\n", kv.me, res)
	freezeShardReply := res.(shardrpc.FreezeShardReply)
	reply.State = freezeShardReply.State
	reply.Num = freezeShardReply.Num
	reply.Err = freezeShardReply.Err
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	installShardReply := res.(shardrpc.InstallShardReply)
	reply.Err = installShardReply.Err
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	deleteShardReply := res.(shardrpc.DeleteShardReply)
	reply.Err = deleteShardReply.Err
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

	serverName := "server-" + strconv.Itoa(int(gid)) + "-" + strconv.Itoa(me)
	store := make(map[shardcfg.Tshid]map[string]VersionedValue)
	shardLockStatus := make(map[shardcfg.Tshid]bool)
	shardConfigNum := make(map[shardcfg.Tshid]shardcfg.Tnum)

	if gid == tester.Tgid(1) {
		for shardId := range shardcfg.NShards {
			store[shardcfg.Tshid(shardId)] = make(map[string]VersionedValue)
			shardLockStatus[shardcfg.Tshid(shardId)] = false
			shardConfigNum[shardcfg.Tshid(shardId)] = 1
		}
	}
	// log.Printf("[ShardGroup Init]Initialize shard group %v with shardConfigNum %v and shardLockStatus %v on server %v\n", gid, shardConfigNum, shardLockStatus, serverName)
	kv := &KVServer{gid: gid, me: me, shardStore: store, shardConfigNum: shardConfigNum, shardLockStatus: shardLockStatus, serverName: serverName}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// Your code here

	return []tester.IService{kv, kv.rsm.Raft()}
}
