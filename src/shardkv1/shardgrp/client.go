package shardgrp

import (
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
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
	var args rpc.GetArgs
	var reply rpc.GetReply

	var serverIdx = 0
	for {
		if ck.leaderIdx != -1 {
			serverIdx = ck.leaderIdx
		} else {
			serverIdx = (serverIdx + 1) % len(ck.servers)
		}
		args = rpc.GetArgs{Key: key}
		reply = rpc.GetReply{}
		ok := ck.clnt.Call(ck.servers[serverIdx], "KVServer.Get", &args, &reply)
		if !ok {
			// log.Printf("==clerk Get RPC to server %d failed\n", server)
			continue
		}

		if reply.Err == rpc.ErrWrongLeader {
			ck.leaderIdx = -1
			// log.Printf("==clerk Get RPC to a non leader server %d failed\n", server)
		}

		if reply.Err == rpc.OK || reply.Err == rpc.ErrNoKey {
			// log.Printf("==clerk Get RPC to a leader server %d succeeded\n", server)
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

	restransmit := false
	var serverIdx = 0
	for {
		if ck.leaderIdx != -1 {
			serverIdx = ck.leaderIdx
		} else {
			serverIdx = (serverIdx + 1) % len(ck.servers)
		}
		reply = rpc.PutReply{}
		// log.Printf("+++clerk Put RPC %v to a server %d with retransmit val %v\n", args, serverIdx, restransmit)
		ok := ck.clnt.Call(ck.servers[serverIdx], "KVServer.Put", &args, &reply)
		if !ok {
			// log.Printf("==clerk Put RPC to server %d failed\n", serverIdx)
			restransmit = true
			continue
		}

		if reply.Err == rpc.ErrWrongLeader {
			ck.leaderIdx = -1
			// log.Printf("==clerk Put RPC %v to a non leader server %d failed\n", args, serverIdx)
			continue
		}

		if reply.Err == rpc.OK {
			// log.Printf("==clerk Put RPC %v to a leader server %d succeeded\n", args, serverIdx)
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
	}
	// log.Printf("Put %v took %v", args, dur)
	return reply.Err
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// Your code here
	return nil, ""
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here
	return ""
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here
	return ""
}

func (ck *Clerk) getAndIncrementSeqNum() int64 {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	seq := ck.sequenceNum
	ck.sequenceNum++
	return seq
}
