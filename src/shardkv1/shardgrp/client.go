package shardgrp

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	clientId  string
	leaderIdx int

	sequenceNum int64
	mu          sync.Mutex
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers, leaderIdx: -1}
	return ck
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// Your code here
	var args = rpc.GetArgs{Key: key}
	var reply rpc.GetReply

	var serverIdx = 0
	restransmit := 0
	for {
		if ck.leaderIdx != -1 {
			serverIdx = ck.leaderIdx
		} else {
			serverIdx = (serverIdx + 1) % len(ck.servers)
		}

		reply = rpc.GetReply{}
		// log.Printf("[Shard Group Client] Get key %v from serverName %v\n", args.Key, ck.servers[serverIdx])
		ok := ck.clnt.Call(ck.servers[serverIdx], "KVServer.Get", &args, &reply)
		if !ok {
			if restransmit > 3 {
				// log.Printf("==clerk Get RPC to server %d failed\n", serverIdx)
				ck.leaderIdx = -1
				reply.Err = rpc.ErrWrongGroup
				break
			}
			restransmit += 1
			time.Sleep(2 * time.Millisecond)
			continue
		}

		if reply.Err == rpc.ErrWrongLeader {
			// log.Printf("[Shard Group Client] Get key %v encountered error %v", args.Key, reply.Err)
			ck.leaderIdx = -1
			time.Sleep(2 * time.Millisecond)
			continue
			// log.Printf("==clerk Get RPC to a non leader server %d failed\n", server)
		}

		if reply.Err == rpc.OK || reply.Err == rpc.ErrNoKey || reply.Err == rpc.ErrWrongGroup {
			// log.Printf("==clerk Get RPC to a leader server %d succeeded\n", server)
			// log.Printf("[Shard Group Client] Get key %v encountered error %v", args.Key, reply.Err)
			ck.leaderIdx = serverIdx
			break
		}
	}

	return reply.Value, reply.Version, reply.Err
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	seqNum := ck.getAndIncrementSeqNum()
	args := rpc.PutArgs{Key: key, Value: value, Version: version, ClientId: ck.clientId, SeqNum: seqNum}
	var reply rpc.PutReply

	// log.Printf("[Shard Group Client] Put RPC for key %v\n", args.Key)
	restransmit := 0
	var serverIdx = 0
	for {
		if ck.leaderIdx != -1 {
			serverIdx = ck.leaderIdx
		} else {
			serverIdx = (serverIdx + 1) % len(ck.servers)
		}
		reply = rpc.PutReply{}
		// log.Printf("[Shard Group Client] Put RPC %v to a server %d with retransmit val %v\n", args, serverIdx, restransmit)
		ok := ck.clnt.Call(ck.servers[serverIdx], "KVServer.Put", &args, &reply)
		if !ok {
			if restransmit > 3 {
				// log.Printf("==clerk Put RPC to server %v failed\n", ck.servers[serverIdx])
				reply.Err = rpc.ErrWrongGroup
				ck.leaderIdx = -1
				break
			}
			restransmit += 1
			time.Sleep(2 * time.Millisecond)
			continue
		}

		if reply.Err == rpc.ErrWrongLeader {
			ck.leaderIdx = -1
			time.Sleep(2 * time.Millisecond)
			// log.Printf("==clerk Put RPC %v to a non leader server %v failed\n", args, ck.servers[serverIdx])
			continue
		}

		if reply.Err == rpc.OK || reply.Err == rpc.ErrWrongGroup {
			// log.Printf("==clerk Put RPC %v to a leader server %v succeeded\n", args, ck.servers[serverIdx])
			ck.leaderIdx = serverIdx
			break
		}

		if reply.Err == rpc.ErrVersion {
			if restransmit > 0 {
				reply.Err = rpc.ErrMaybe
			}
			// log.Printf("==clerk Put RPC %v to a leader server %d failed due to error %v since there is a different version and retransmit is %v\n", args, ck.servers[serverIdx], reply.Err, restransmit)
			ck.leaderIdx = serverIdx
			break
		}
	}

	return reply.Err
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// Your code here
	args := shardrpc.FreezeShardArgs{Shard: s, Num: num}
	var reply shardrpc.FreezeShardReply

	var serverIdx = 0
	for {
		if ck.leaderIdx != -1 {
			serverIdx = ck.leaderIdx
		} else {
			serverIdx = (serverIdx + 1) % len(ck.servers)
		}
		// log.Printf("[Shard Group Client]FreezeShard shardId %v with configNum %v to a server id %d server %v\n", args.Shard, args.Num, serverIdx, ck.servers[serverIdx])

		reply = shardrpc.FreezeShardReply{}

		ok := ck.clnt.Call(ck.servers[serverIdx], "KVServer.FreezeShard", &args, &reply)
		if !ok {
			// log.Printf("==clerk FreezeShard RPC to server %d failed\n", serverIdx)
			time.Sleep(2 * time.Millisecond)
			continue
		}

		if reply.Err == rpc.ErrWrongLeader {
			ck.leaderIdx = -1
			time.Sleep(2 * time.Millisecond)
			// log.Printf("==clerk FreezeShard RPC %v to a non leader server %d failed\n", args, serverIdx)
			continue
		}

		if reply.Err == rpc.OK || reply.Err == rpc.ErrWrongGroup {
			// log.Printf("==clerk FreezeShard RPC %v to a leader server %d succeeded\n", args, serverIdx)
			ck.leaderIdx = serverIdx
			break
		}
	}

	return reply.State, reply.Err
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.InstallShardArgs{Shard: s, Num: num, State: state}
	var reply shardrpc.InstallShardReply

	restransmit := false
	var serverIdx = 0
	for {
		if ck.leaderIdx != -1 {
			serverIdx = ck.leaderIdx
		} else {
			serverIdx = (serverIdx + 1) % len(ck.servers)
		}
		// log.Printf("[Shard Group Client]InstallShard shardId %v with configNum %v to a server id %d server %v\n", args.Shard, args.Num, serverIdx, ck.servers[serverIdx])

		reply = shardrpc.InstallShardReply{}

		ok := ck.clnt.Call(ck.servers[serverIdx], "KVServer.InstallShard", &args, &reply)
		if !ok {
			// log.Printf("==clerk Put RPC to server %d failed\n", serverIdx)
			restransmit = true
			time.Sleep(2 * time.Millisecond)
			continue
		}

		if reply.Err == rpc.ErrWrongLeader {
			ck.leaderIdx = -1
			time.Sleep(2 * time.Millisecond)
			// log.Printf("==clerk Put RPC %v to a non leader server %d failed\n", args, serverIdx)
			continue
		}

		if reply.Err == rpc.OK || reply.Err == rpc.ErrWrongGroup {
			// log.Printf("[Shard Group Client]+++clerk InstallShard RPC shard id %v with configNum %v to a leader server %d encounter errWrongGroup\n", args.Shard, args.Num, serverIdx)
			ck.leaderIdx = serverIdx
			break
		}

		if reply.Err == rpc.ErrVersion {
			if restransmit {
				reply.Err = rpc.ErrMaybe
			}
			// log.Printf("==clerk Put RPC %v to a leader server %d failed since there is a different version and retransmit is %v\n", args, serverIdx, restransmit)
			ck.leaderIdx = serverIdx
			break
		}
		// log.Printf("[Shard Group Client]+++clerk InstallShard shard id %v with configNum %v to a server id %d server %v failed %v\n", args.Shard, args.Num, serverIdx, ck.servers[serverIdx], reply.Err)
	}

	return reply.Err
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.DeleteShardArgs{Shard: s, Num: num}
	var reply shardrpc.DeleteShardReply

	var serverIdx = 0
	for {
		if ck.leaderIdx != -1 {
			serverIdx = ck.leaderIdx
		} else {
			serverIdx = (serverIdx + 1) % len(ck.servers)
		}
		// log.Printf("[Shard Group Client]DeleteShard shardId %v with configNum %v to a server id %d server %v\n", args.Shard, args.Num, serverIdx, ck.servers[serverIdx])

		reply = shardrpc.DeleteShardReply{}

		ok := ck.clnt.Call(ck.servers[serverIdx], "KVServer.DeleteShard", &args, &reply)
		if !ok {
			time.Sleep(2 * time.Millisecond)
			// log.Printf("==clerk DeleteShard RPC to server %d failed\n", serverIdx)
			continue
		}

		if reply.Err == rpc.ErrWrongLeader {
			ck.leaderIdx = -1
			time.Sleep(2 * time.Millisecond)
			// log.Printf("==clerk DeleteShard RPC %v to a non leader server %d failed\n", args, serverIdx)
			continue
		}

		if reply.Err == rpc.OK {
			// log.Printf("==clerk DeleteShard RPC %v to a leader server %d succeeded\n", args, serverIdx)
			ck.leaderIdx = serverIdx
			break
		}

		if reply.Err == rpc.ErrWrongGroup {
			// log.Printf("==clerk DeleteShard RPC %v to a leader server %d failed since there is a different version\n", args, serverIdx)
			ck.leaderIdx = serverIdx
			break
		}
	}

	return reply.Err
}

func (ck *Clerk) getAndIncrementSeqNum() int64 {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	seq := ck.sequenceNum
	ck.sequenceNum++
	return seq
}
